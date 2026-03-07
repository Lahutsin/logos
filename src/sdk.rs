use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::protocol::{
    self, CommitOffsetRequest, FetchRequest, GroupFetchRequest, HeartbeatRequest,
    JoinGroupRequest, LeaveGroupRequest, ProduceRequest, Record, Request, Response,
    PROTOCOL_VERSION,
};

#[derive(Debug, Clone)]
pub struct GroupHeartbeatConfig {
    pub addr: String,
    pub group_id: String,
    pub topic: String,
    pub member_id: String,
    pub generation: u64,
    pub interval: Duration,
    pub auth: Option<String>,
}

pub struct GroupHeartbeatHandle {
    stop_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl GroupHeartbeatHandle {
    pub async fn stop(mut self) -> anyhow::Result<()> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        self.task
            .await
            .map_err(|err| anyhow::anyhow!("heartbeat task failed to join: {err}"))?
    }

    pub fn abort(mut self) {
        self.stop_tx.take();
        self.task.abort();
    }
}

pub fn spawn_group_heartbeats(config: GroupHeartbeatConfig) -> GroupHeartbeatHandle {
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let interval = if config.interval.is_zero() {
            Duration::from_millis(1)
        } else {
            config.interval
        };

        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut client: Option<Client> = None;

        loop {
            tokio::select! {
                _ = &mut stop_rx => return Ok(()),
                _ = ticker.tick() => {
                    if client.is_none() {
                        match Client::connect(&config.addr).await {
                            Ok(connected) => client = Some(connected),
                            Err(_) => continue,
                        }
                    }

                    let response = match client
                        .as_mut()
                        .expect("client is set before heartbeats")
                        .heartbeat(
                            &config.group_id,
                            &config.topic,
                            &config.member_id,
                            config.generation,
                            config.auth.clone(),
                        )
                        .await
                    {
                        Ok(response) => response,
                        Err(_) => {
                            client = None;
                            continue;
                        }
                    };

                    match response {
                        Response::HeartbeatOk { generation, .. } if generation == config.generation => {}
                        Response::RebalanceRequired { generation, .. } => {
                            anyhow::bail!(
                                "consumer group rebalance required; current generation is {generation}"
                            );
                        }
                        Response::Error(err) => {
                            anyhow::bail!("consumer group heartbeat failed: {err}");
                        }
                        other => {
                            anyhow::bail!("unexpected heartbeat response: {other:?}");
                        }
                    }
                }
            }
        }
    });

    GroupHeartbeatHandle {
        stop_tx: Some(stop_tx),
        task,
    }
}

pub struct Client {
    framed: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Client {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let codec = LengthDelimitedCodec::builder()
            .length_field_length(4)
            .new_codec();
        let mut framed = Framed::new(stream, codec);

        // handshake
        let handshake = Request::Handshake {
            client_version: PROTOCOL_VERSION,
        };
        framed
            .send(Bytes::from(protocol::encode(&handshake)?))
            .await?;
        if let Some(res) = framed.next().await.transpose()? {
            match protocol::decode::<Response>(&res)? {
                Response::HandshakeOk { server_version } if server_version == PROTOCOL_VERSION => {}
                Response::HandshakeOk { server_version } => anyhow::bail!(
                    "protocol version mismatch: server {server_version} client {PROTOCOL_VERSION}"
                ),
                other => anyhow::bail!("unexpected handshake response: {:?}", other),
            }
        } else {
            anyhow::bail!("no handshake response")
        }

        Ok(Self { framed })
    }

    pub async fn produce(
        &mut self,
        topic: &str,
        partition: u32,
        records: Vec<Record>,
        auth: Option<String>,
    ) -> anyhow::Result<Response> {
        let req = Request::Produce(ProduceRequest {
            topic: topic.to_string(),
            partition,
            records,
            auth,
        });
        self.send(req).await
    }

    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        auth: Option<String>,
    ) -> anyhow::Result<Response> {
        let req = Request::Fetch(FetchRequest {
            topic: topic.to_string(),
            partition,
            offset,
            max_bytes,
            auth,
        });
        self.send(req).await
    }

    pub async fn join_group(
        &mut self,
        group_id: &str,
        topic: &str,
        member_id: Option<String>,
        auth: Option<String>,
    ) -> anyhow::Result<Response> {
        let req = Request::JoinGroup(JoinGroupRequest {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            member_id,
            auth,
        });
        self.send(req).await
    }

    pub async fn heartbeat(
        &mut self,
        group_id: &str,
        topic: &str,
        member_id: &str,
        generation: u64,
        auth: Option<String>,
    ) -> anyhow::Result<Response> {
        let req = Request::Heartbeat(HeartbeatRequest {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            member_id: member_id.to_string(),
            generation,
            auth,
        });
        self.send(req).await
    }

    pub async fn commit_offset(
        &mut self,
        group_id: &str,
        topic: &str,
        member_id: &str,
        generation: u64,
        partition: u32,
        offset: u64,
        auth: Option<String>,
    ) -> anyhow::Result<Response> {
        let req = Request::CommitOffset(CommitOffsetRequest {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            member_id: member_id.to_string(),
            generation,
            partition,
            offset,
            auth,
        });
        self.send(req).await
    }

    pub async fn group_fetch(
        &mut self,
        group_id: &str,
        topic: &str,
        member_id: &str,
        generation: u64,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        auth: Option<String>,
    ) -> anyhow::Result<Response> {
        let req = Request::GroupFetch(GroupFetchRequest {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            member_id: member_id.to_string(),
            generation,
            partition,
            offset,
            max_bytes,
            auth,
        });
        self.send(req).await
    }

    pub async fn leave_group(
        &mut self,
        group_id: &str,
        topic: &str,
        member_id: &str,
        generation: u64,
        auth: Option<String>,
    ) -> anyhow::Result<Response> {
        let req = Request::LeaveGroup(LeaveGroupRequest {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            member_id: member_id.to_string(),
            generation,
            auth,
        });
        self.send(req).await
    }

    async fn send(&mut self, req: Request) -> anyhow::Result<Response> {
        let bytes = protocol::encode(&req)?;
        self.framed.send(Bytes::from(bytes)).await?;
        if let Some(frame) = self.framed.next().await.transpose()? {
            let resp: Response = protocol::decode(&frame)?;
            Ok(resp)
        } else {
            anyhow::bail!("connection closed")
        }
    }
}
