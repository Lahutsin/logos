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
use crate::protocol::{self, ReplicaRecord, ReplicateRequest, Request, Response};
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
            tls: TlsEndpoints { acceptor: None, connector: None, server_name: None },
            auth_token: None,
        }
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

    let action = async move {
        let tcp = TcpStream::connect(&addr).await?;
        let codec = LengthDelimitedCodec::builder().length_field_length(4).new_codec();

        if let (Some(connector), Some(name)) = (tls.connector, tls.server_name.clone()) {
            let tls_stream = connector.connect(name, tcp).await?;
            let mut framed = Framed::new(tls_stream, codec);
            let payload = protocol::encode(&Request::Replicate(req.clone()))?;
            framed.send(Bytes::from(payload)).await?;
            if let Some(frame) = framed.next().await.transpose()? {
                let response: Response = protocol::decode(&frame)?;
                match response {
                    Response::Produced { .. } => return Ok::<bool, anyhow::Error>(true),
                    Response::NotLeader { leader } => {
                        inc_request("replicate", "not_leader");
                        warn!(?leader, peer=%peer, "peer rejected replicate: not leader");
                    }
                    Response::Error(err) => {
                        inc_request("replicate", "error");
                        warn!(error=%err, peer=%peer, "peer replication error");
                    }
                    _ => {
                        warn!(peer=%peer, "unexpected replication response");
                    }
                }
            }
        } else {
            let mut framed = Framed::new(tcp, codec);
            let payload = protocol::encode(&Request::Replicate(req))?;
            framed.send(Bytes::from(payload)).await?;
            if let Some(frame) = framed.next().await.transpose()? {
                let response: Response = protocol::decode(&frame)?;
                match response {
                    Response::Produced { .. } => return Ok::<bool, anyhow::Error>(true),
                    Response::NotLeader { leader } => {
                        warn!(?leader, peer=%peer, "peer rejected replicate: not leader");
                    }
                    Response::Error(err) => {
                        warn!(error=%err, peer=%peer, "peer replication error");
                    }
                    _ => {
                        warn!(peer=%peer, "unexpected replication response");
                    }
                }
            }
        }

        Ok::<bool, anyhow::Error>(false)
    };

    match time::timeout(timeout, action).await {
        Ok(Ok(ok)) => ok,
        Ok(Err(err)) => {
            warn!(%err, peer=%peer_id, "replication failed");
            false
        }
        Err(_) => {
            warn!(peer=%peer_id, "replication timed out");
            false
        }
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
        let ok = replicate_once(addr.clone(), req.clone(), timeout, peer_id.clone(), tls.clone()).await;
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
