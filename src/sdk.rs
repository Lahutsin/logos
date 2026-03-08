use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::protocol::{
    self, CommitOffsetRequest, ConsumerGroupAssignment, FetchRequest, FetchedRecord,
    GroupFetchRequest, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, ProduceRequest,
    Record, Request, Response, PROTOCOL_VERSION,
};

#[derive(Debug)]
enum HeartbeatEvent {
    Rebalance { generation: u64 },
    Failed(String),
}

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
    spawn_group_heartbeats_inner(config, None)
}

fn spawn_group_heartbeats_inner(
    config: GroupHeartbeatConfig,
    events_tx: Option<mpsc::UnboundedSender<HeartbeatEvent>>,
) -> GroupHeartbeatHandle {
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
                            if let Some(events_tx) = events_tx.as_ref() {
                                let _ = events_tx.send(HeartbeatEvent::Rebalance { generation });
                            }
                            anyhow::bail!(
                                "consumer group rebalance required; current generation is {generation}"
                            );
                        }
                        Response::Error(err) => {
                            if let Some(events_tx) = events_tx.as_ref() {
                                let _ = events_tx.send(HeartbeatEvent::Failed(err.clone()));
                            }
                            anyhow::bail!("consumer group heartbeat failed: {err}");
                        }
                        other => {
                            if let Some(events_tx) = events_tx.as_ref() {
                                let _ = events_tx.send(HeartbeatEvent::Failed(format!(
                                    "unexpected heartbeat response: {other:?}"
                                )));
                            }
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

#[derive(Debug, Clone)]
pub struct GroupConsumerConfig {
    pub bootstrap_addr: String,
    pub group_id: String,
    pub topic: String,
    pub auth: Option<String>,
    pub fetch_max_bytes: u32,
    pub idle_backoff: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupConsumerRecord {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub record: Record,
    pub leader_hint: Option<String>,
    pub member_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupConsumerAction {
    Continue,
    Stop,
}

pub struct GroupConsumer {
    config: GroupConsumerConfig,
    bootstrap: Client,
    fetch_clients: HashMap<String, Client>,
    member_id: Option<String>,
    generation: u64,
    assignments: Vec<ConsumerGroupAssignment>,
    next_offsets: HashMap<u32, u64>,
    next_assignment_index: usize,
    heartbeat_handle: Option<GroupHeartbeatHandle>,
    heartbeat_events: Option<mpsc::UnboundedReceiver<HeartbeatEvent>>,
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
        request: CommitOffsetRequest,
    ) -> anyhow::Result<Response> {
        self.send(Request::CommitOffset(request)).await
    }

    pub async fn group_fetch(&mut self, request: GroupFetchRequest) -> anyhow::Result<Response> {
        self.send(Request::GroupFetch(request)).await
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

impl GroupConsumer {
    pub async fn connect(config: GroupConsumerConfig) -> anyhow::Result<Self> {
        let bootstrap = Client::connect(&config.bootstrap_addr).await?;
        let mut consumer = Self {
            config,
            bootstrap,
            fetch_clients: HashMap::new(),
            member_id: None,
            generation: 0,
            assignments: Vec::new(),
            next_offsets: HashMap::new(),
            next_assignment_index: 0,
            heartbeat_handle: None,
            heartbeat_events: None,
        };
        consumer.ensure_joined().await?;
        Ok(consumer)
    }

    pub fn member_id(&self) -> Option<&str> {
        self.member_id.as_deref()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn assignments(&self) -> &[ConsumerGroupAssignment] {
        &self.assignments
    }

    pub async fn run<F, Fut>(&mut self, mut handler: F) -> anyhow::Result<()>
    where
        F: FnMut(GroupConsumerRecord) -> Fut,
        Fut: Future<Output = anyhow::Result<GroupConsumerAction>>,
    {
        loop {
            self.ensure_joined().await?;

            if self.heartbeat_requires_rejoin()? {
                self.rejoin().await?;
                continue;
            }

            let Some((assignment, records)) = self.fetch_next_batch().await? else {
                time::sleep(self.config.idle_backoff.max(Duration::from_millis(1))).await;
                continue;
            };

            let mut rejoined = false;
            for fetched in records {
                if self.heartbeat_requires_rejoin()? {
                    self.rejoin().await?;
                    rejoined = true;
                    break;
                }

                let message = GroupConsumerRecord {
                    topic: assignment.topic.clone(),
                    partition: assignment.partition,
                    offset: fetched.offset,
                    record: fetched.record,
                    leader_hint: assignment.leader_hint.clone(),
                    member_id: self.member_id.clone().unwrap_or_default(),
                    generation: self.generation,
                };

                let action = match handler(message).await {
                    Ok(action) => action,
                    Err(err) => {
                        self.stop_heartbeats().await;
                        return Err(err);
                    }
                };

                if !self
                    .commit_processed_offset(assignment.partition, fetched.offset + 1)
                    .await?
                {
                    rejoined = true;
                    break;
                }

                if action == GroupConsumerAction::Stop {
                    self.shutdown().await?;
                    return Ok(());
                }
            }

            if rejoined {
                continue;
            }
        }
    }

    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.stop_heartbeats().await;

        let Some(member_id) = self.member_id.clone() else {
            return Ok(());
        };

        let response = self
            .send_leave_group(&member_id, self.generation)
            .await
            .map_err(|err| anyhow::anyhow!("failed to leave consumer group: {err}"))?;

        self.member_id = None;
        self.generation = 0;
        self.assignments.clear();
        self.next_offsets.clear();
        self.next_assignment_index = 0;
        self.fetch_clients.clear();

        match response {
            Response::GroupLeft { .. }
            | Response::RebalanceRequired { .. }
            | Response::Error(_) => Ok(()),
            other => Err(anyhow::anyhow!(
                "unexpected leave_group response while shutting down consumer: {other:?}"
            )),
        }
    }

    async fn ensure_joined(&mut self) -> anyhow::Result<()> {
        if self.member_id.is_some() && self.heartbeat_handle.is_some() {
            return Ok(());
        }

        let member_id = self.member_id.clone();
        let response = self.send_join_group(member_id).await?;
        match response {
            Response::GroupJoined {
                member_id,
                generation,
                heartbeat_interval_ms,
                assignments,
                ..
            } => {
                self.stop_heartbeats().await;
                self.fetch_clients.clear();
                self.member_id = Some(member_id.clone());
                self.generation = generation;
                self.assignments = assignments;
                self.assignments
                    .sort_by_key(|assignment| assignment.partition);
                self.next_offsets = self
                    .assignments
                    .iter()
                    .map(|assignment| (assignment.partition, assignment.offset))
                    .collect();
                self.next_assignment_index = 0;
                self.start_heartbeats(heartbeat_interval_ms, &member_id, generation);
                Ok(())
            }
            Response::Error(err) => Err(anyhow::anyhow!("consumer group join failed: {err}")),
            other => Err(anyhow::anyhow!(
                "unexpected join_group response while starting consumer: {other:?}"
            )),
        }
    }

    async fn rejoin(&mut self) -> anyhow::Result<()> {
        self.abort_heartbeats();
        self.assignments.clear();
        self.next_offsets.clear();
        self.next_assignment_index = 0;
        self.fetch_clients.clear();
        self.ensure_joined().await
    }

    fn start_heartbeats(&mut self, heartbeat_interval_ms: u64, member_id: &str, generation: u64) {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        self.heartbeat_events = Some(events_rx);
        self.heartbeat_handle = Some(spawn_group_heartbeats_inner(
            GroupHeartbeatConfig {
                addr: self.config.bootstrap_addr.clone(),
                group_id: self.config.group_id.clone(),
                topic: self.config.topic.clone(),
                member_id: member_id.to_string(),
                generation,
                interval: Duration::from_millis((heartbeat_interval_ms / 2).max(1)),
                auth: self.config.auth.clone(),
            },
            Some(events_tx),
        ));
    }

    async fn fetch_next_batch(
        &mut self,
    ) -> anyhow::Result<Option<(ConsumerGroupAssignment, Vec<FetchedRecord>)>> {
        if self.assignments.is_empty() {
            return Ok(None);
        }

        let mut last_error: Option<anyhow::Error> = None;
        let assignments_len = self.assignments.len();
        for step in 0..assignments_len {
            let index = (self.next_assignment_index + step) % assignments_len;
            let assignment = self.assignments[index].clone();
            let offset = self
                .next_offsets
                .get(&assignment.partition)
                .copied()
                .unwrap_or(assignment.offset);
            let addr = assignment
                .leader_hint
                .clone()
                .unwrap_or_else(|| self.config.bootstrap_addr.clone());

            let response = match self
                .send_group_fetch(&addr, assignment.partition, offset)
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    last_error = Some(err);
                    continue;
                }
            };

            match response {
                Response::Fetched { records } => {
                    if records.is_empty() {
                        continue;
                    }
                    self.next_assignment_index = (index + 1) % assignments_len;
                    return Ok(Some((assignment, records)));
                }
                Response::RebalanceRequired { .. } => {
                    self.rejoin().await?;
                    return Ok(None);
                }
                Response::Error(err)
                    if err.contains("partition not assigned") || err.contains("consumer group") =>
                {
                    self.rejoin().await?;
                    return Ok(None);
                }
                Response::NotLeader { leader } => {
                    self.update_leader_hint(assignment.partition, leader);
                }
                Response::Error(err) => {
                    return Err(anyhow::anyhow!(
                        "group fetch failed for partition {}: {err}",
                        assignment.partition
                    ));
                }
                other => {
                    return Err(anyhow::anyhow!(
                        "unexpected group_fetch response for partition {}: {other:?}",
                        assignment.partition
                    ));
                }
            }
        }

        if let Some(err) = last_error {
            return Err(err);
        }

        Ok(None)
    }

    async fn commit_processed_offset(
        &mut self,
        partition: u32,
        offset: u64,
    ) -> anyhow::Result<bool> {
        let Some(member_id) = self.member_id.clone() else {
            anyhow::bail!("consumer group member is not joined")
        };

        let response = self
            .send_commit_offset(&member_id, self.generation, partition, offset)
            .await?;
        match response {
            Response::OffsetCommitted {
                generation,
                partition: committed_partition,
                offset: committed_offset,
                ..
            } if generation == self.generation
                && committed_partition == partition
                && committed_offset == offset =>
            {
                self.next_offsets.insert(partition, offset);
                Ok(true)
            }
            Response::RebalanceRequired { .. } => {
                self.rejoin().await?;
                Ok(false)
            }
            Response::Error(err)
                if err.contains("partition not assigned") || err.contains("consumer group") =>
            {
                self.rejoin().await?;
                Ok(false)
            }
            Response::Error(err) => Err(anyhow::anyhow!(
                "commit_offset failed for partition {partition}: {err}"
            )),
            other => Err(anyhow::anyhow!(
                "unexpected commit_offset response for partition {partition}: {other:?}"
            )),
        }
    }

    fn heartbeat_requires_rejoin(&mut self) -> anyhow::Result<bool> {
        let Some(events) = self.heartbeat_events.as_mut() else {
            return Ok(false);
        };

        let mut requires_rejoin = false;
        loop {
            match events.try_recv() {
                Ok(HeartbeatEvent::Rebalance { generation }) => {
                    self.generation = generation;
                    requires_rejoin = true;
                }
                Ok(HeartbeatEvent::Failed(err)) => {
                    return Err(anyhow::anyhow!("consumer group heartbeat failed: {err}"));
                }
                Err(mpsc::error::TryRecvError::Empty) => return Ok(requires_rejoin),
                Err(mpsc::error::TryRecvError::Disconnected) => return Ok(requires_rejoin),
            }
        }
    }

    fn update_leader_hint(&mut self, partition: u32, leader_hint: Option<String>) {
        if let Some(assignment) = self
            .assignments
            .iter_mut()
            .find(|assignment| assignment.partition == partition)
        {
            assignment.leader_hint = leader_hint;
        }
    }

    async fn send_join_group(&mut self, member_id: Option<String>) -> anyhow::Result<Response> {
        match self
            .bootstrap
            .join_group(
                &self.config.group_id,
                &self.config.topic,
                member_id.clone(),
                self.config.auth.clone(),
            )
            .await
        {
            Ok(response) => Ok(response),
            Err(_) => {
                self.bootstrap = Client::connect(&self.config.bootstrap_addr).await?;
                self.bootstrap
                    .join_group(
                        &self.config.group_id,
                        &self.config.topic,
                        member_id,
                        self.config.auth.clone(),
                    )
                    .await
            }
        }
    }

    async fn send_commit_offset(
        &mut self,
        member_id: &str,
        generation: u64,
        partition: u32,
        offset: u64,
    ) -> anyhow::Result<Response> {
        match self
            .bootstrap
            .commit_offset(CommitOffsetRequest {
                group_id: self.config.group_id.clone(),
                topic: self.config.topic.clone(),
                member_id: member_id.to_string(),
                generation,
                partition,
                offset,
                auth: self.config.auth.clone(),
            })
            .await
        {
            Ok(response) => Ok(response),
            Err(_) => {
                self.bootstrap = Client::connect(&self.config.bootstrap_addr).await?;
                self.bootstrap
                    .commit_offset(CommitOffsetRequest {
                        group_id: self.config.group_id.clone(),
                        topic: self.config.topic.clone(),
                        member_id: member_id.to_string(),
                        generation,
                        partition,
                        offset,
                        auth: self.config.auth.clone(),
                    })
                    .await
            }
        }
    }

    async fn send_leave_group(
        &mut self,
        member_id: &str,
        generation: u64,
    ) -> anyhow::Result<Response> {
        match self
            .bootstrap
            .leave_group(
                &self.config.group_id,
                &self.config.topic,
                member_id,
                generation,
                self.config.auth.clone(),
            )
            .await
        {
            Ok(response) => Ok(response),
            Err(_) => {
                self.bootstrap = Client::connect(&self.config.bootstrap_addr).await?;
                self.bootstrap
                    .leave_group(
                        &self.config.group_id,
                        &self.config.topic,
                        member_id,
                        generation,
                        self.config.auth.clone(),
                    )
                    .await
            }
        }
    }

    async fn send_group_fetch(
        &mut self,
        addr: &str,
        partition: u32,
        offset: u64,
    ) -> anyhow::Result<Response> {
        if !self.fetch_clients.contains_key(addr) {
            self.fetch_clients
                .insert(addr.to_string(), Client::connect(addr).await?);
        }

        let result = match self.fetch_clients.get_mut(addr) {
            Some(client) => {
                client
                    .group_fetch(GroupFetchRequest {
                        group_id: self.config.group_id.clone(),
                        topic: self.config.topic.clone(),
                        member_id: self.member_id.clone().unwrap_or_default(),
                        generation: self.generation,
                        partition,
                        offset,
                        max_bytes: self.config.fetch_max_bytes,
                        auth: self.config.auth.clone(),
                    })
                    .await
            }
            None => anyhow::bail!("fetch client for '{addr}' is unavailable"),
        };

        match result {
            Ok(response) => Ok(response),
            Err(_) => {
                self.fetch_clients.remove(addr);
                self.fetch_clients
                    .insert(addr.to_string(), Client::connect(addr).await?);
                self.fetch_clients
                    .get_mut(addr)
                    .expect("fetch client is inserted before retry")
                    .group_fetch(GroupFetchRequest {
                        group_id: self.config.group_id.clone(),
                        topic: self.config.topic.clone(),
                        member_id: self.member_id.clone().unwrap_or_default(),
                        generation: self.generation,
                        partition,
                        offset,
                        max_bytes: self.config.fetch_max_bytes,
                        auth: self.config.auth.clone(),
                    })
                    .await
            }
        }
    }

    async fn stop_heartbeats(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            let _ = handle.stop().await;
        }
        self.heartbeat_events = None;
    }

    fn abort_heartbeats(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
        self.heartbeat_events = None;
    }
}

impl Drop for GroupConsumer {
    fn drop(&mut self) {
        self.abort_heartbeats();
    }
}
