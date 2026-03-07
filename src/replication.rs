use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{future, SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::warn;

use crate::metadata::Metadata;
use crate::metrics::inc_request;
use crate::protocol::{self, ReplicaRecord, ReplicateRequest, Request, Response, PROTOCOL_VERSION};
use crate::security::TlsEndpoints;

#[derive(Clone)]
pub struct Replicator {
    metadata: Arc<Metadata>,
    timeout: Duration,
    retries: usize,
    backoff: Duration,
    tls: TlsEndpoints,
    auth_token: Option<String>,
}

impl Replicator {
    pub fn new(
        metadata: Arc<Metadata>,
        timeout_ms: u64,
        retries: usize,
        backoff_ms: u64,
        tls: TlsEndpoints,
        auth_token: Option<String>,
    ) -> Self {
        Self {
            metadata,
            timeout: Duration::from_millis(timeout_ms),
            retries,
            backoff: Duration::from_millis(backoff_ms),
            tls,
            auth_token,
        }
    }

    pub fn disabled() -> Self {
        Self {
            metadata: Arc::new(Metadata::single_node("node-1".to_string())),
            timeout: Duration::from_millis(0),
            retries: 0,
            backoff: Duration::from_millis(0),
            tls: TlsEndpoints {
                acceptor: None,
                connector: None,
                server_name: None,
            },
            auth_token: None,
        }
    }

    pub fn internal_auth_token(&self) -> Option<String> {
        self.auth_token.clone()
    }

    pub async fn send_request_to_node(
        &self,
        node_id: &str,
        request: Request,
    ) -> anyhow::Result<Response> {
        let addr = self
            .metadata
            .address(node_id)
            .ok_or_else(|| anyhow::anyhow!("unknown peer '{node_id}'"))?;
        send_request_once(addr, request, self.timeout, self.tls.clone()).await
    }

    /// Replicate batch to followers and wait for acknowledgements.
    /// Returns the number of follower acknowledgements (caller adds local write).
    pub async fn replicate(
        &self,
        topic: &str,
        partition: u32,
        leader_epoch: u64,
        entries: Vec<ReplicaRecord>,
        followers: Vec<String>,
    ) -> usize {
        if followers.is_empty() {
            return 0;
        }

        let req = ReplicateRequest {
            leader_id: self.metadata.self_id().to_string(),
            leader_epoch,
            topic: topic.to_string(),
            partition,
            entries,
            auth: self.auth_token.clone(),
        };

        let tasks = followers
            .into_iter()
            .filter_map(|id| self.metadata.address(&id).map(|addr| (id, addr)))
            .map(|(id, addr)| {
                replicate_with_retry(
                    addr,
                    req.clone(),
                    self.timeout,
                    self.retries,
                    self.backoff,
                    id,
                    self.tls.clone(),
                )
            });

        let results = future::join_all(tasks).await;
        results.into_iter().filter(|r| *r).count()
    }
}

async fn replicate_once(
    addr: String,
    req: ReplicateRequest,
    timeout: Duration,
    peer_id: String,
    tls: TlsEndpoints,
) -> bool {
    let peer = peer_id.clone();

    match send_request_once(addr, Request::Replicate(req), timeout, tls).await {
        Ok(Response::Produced { .. }) => true,
        Ok(Response::NotLeader { leader }) => {
            inc_request("replicate", "not_leader");
            warn!(?leader, peer=%peer, "peer rejected replicate: not leader");
            false
        }
        Ok(Response::Error(err)) => {
            inc_request("replicate", "error");
            warn!(error=%err, peer=%peer, "peer replication error");
            false
        }
        Ok(other) => {
            warn!(peer=%peer, response=?other, "unexpected replication response");
            false
        }
        Err(err) => {
            warn!(%err, peer=%peer_id, "replication failed");
            false
        }
    }
}

async fn send_request_once(
    addr: String,
    request: Request,
    timeout: Duration,
    tls: TlsEndpoints,
) -> anyhow::Result<Response> {
    let action = async move {
        let tcp = TcpStream::connect(&addr).await?;
        let codec = LengthDelimitedCodec::builder()
            .length_field_length(4)
            .new_codec();

        if let Some(connector) = tls.connector {
            let name = tls.server_name.clone().ok_or_else(|| {
                anyhow::anyhow!("tls connector configured but server_name is missing")
            })?;
            let tls_stream = connector.connect(name, tcp).await?;
            let mut framed = Framed::new(tls_stream, codec);
            send_request_over_framed(&mut framed, request).await
        } else {
            let mut framed = Framed::new(tcp, codec);
            send_request_over_framed(&mut framed, request).await
        }
    };

    if timeout.is_zero() {
        return action.await;
    }

    match time::timeout(timeout, action).await {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!("request to peer timed out")),
    }
}

async fn send_request_over_framed<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    request: Request,
) -> anyhow::Result<Response>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    perform_handshake(framed).await?;
    let payload = protocol::encode(&request)?;
    framed.send(Bytes::from(payload)).await?;
    let frame = framed
        .next()
        .await
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("peer closed connection before responding"))?;
    let response: Response = protocol::decode(&frame)?;
    Ok(response)
}

async fn perform_handshake<S>(framed: &mut Framed<S, LengthDelimitedCodec>) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let handshake = Request::Handshake {
        client_version: PROTOCOL_VERSION,
    };
    let payload = protocol::encode(&handshake)?;
    framed.send(Bytes::from(payload)).await?;

    let frame = framed
        .next()
        .await
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("no handshake response from peer"))?;
    let response: Response = protocol::decode(&frame)?;
    match response {
        Response::HandshakeOk { server_version } if server_version == PROTOCOL_VERSION => Ok(()),
        Response::HandshakeOk { server_version } => Err(anyhow::anyhow!(
            "protocol version mismatch: server {} client {}",
            server_version,
            PROTOCOL_VERSION
        )),
        Response::Error(err) => Err(anyhow::anyhow!("handshake rejected: {err}")),
        other => Err(anyhow::anyhow!(
            "unexpected handshake response: {:?}",
            other
        )),
    }
}

async fn replicate_with_retry(
    addr: String,
    req: ReplicateRequest,
    timeout: Duration,
    retries: usize,
    backoff: Duration,
    peer_id: String,
    tls: TlsEndpoints,
) -> bool {
    let mut attempt = 0usize;
    loop {
        let ok = replicate_once(
            addr.clone(),
            req.clone(),
            timeout,
            peer_id.clone(),
            tls.clone(),
        )
        .await;
        if ok {
            return true;
        }
        if attempt >= retries {
            return false;
        }
        attempt += 1;
        time::sleep(backoff).await;
    }
}
