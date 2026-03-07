use std::sync::Arc;

use crate::consumer_group::{ConsumerGroupCoordinator, GroupError};
use crate::metadata::Metadata;
use crate::metrics::{add_bytes, inc_request};
use crate::protocol::{
    CommitOffsetRequest, FetchRequest, GroupFetchRequest, HeartbeatRequest, JoinGroupRequest,
    LeaveGroupRequest, ProduceRequest, ReplicaRecord, Request, Response,
    ValidateGroupFetchRequest,
};
use crate::replication::Replicator;
use crate::security::{Action, Authz};
use crate::storage::Storage;
use tracing::error;

#[derive(Clone)]
pub struct Broker {
    storage: Storage,
    metadata: Arc<Metadata>,
    ack_quorum: usize,
    replicator: Replicator,
    authz: Authz,
    max_batch_bytes: u64,
    consumer_groups: ConsumerGroupCoordinator,
}

impl Broker {
    pub fn new(
        storage: Storage,
        replicator: Replicator,
        metadata: Arc<Metadata>,
        ack_quorum: usize,
        authz: Authz,
        max_batch_bytes: u64,
        consumer_group_heartbeat_ms: u64,
        consumer_group_session_timeout_ms: u64,
    ) -> Self {
        Self {
            storage,
            metadata,
            ack_quorum: ack_quorum.max(1),
            replicator,
            authz,
            max_batch_bytes,
            consumer_groups: ConsumerGroupCoordinator::new(
                consumer_group_heartbeat_ms,
                consumer_group_session_timeout_ms,
            ),
        }
    }

    pub async fn handle(&self, request: Request) -> Response {
        match request {
            Request::Produce(req) => self.handle_produce(req).await,
            Request::Replicate(req) => self.handle_replicate(req).await,
            Request::Fetch(req) => self.handle_fetch(req).await,
            Request::JoinGroup(req) => self.handle_join_group(req).await,
            Request::Heartbeat(req) => self.handle_heartbeat(req).await,
            Request::CommitOffset(req) => self.handle_commit_offset(req).await,
            Request::GroupFetch(req) => self.handle_group_fetch(req).await,
            Request::LeaveGroup(req) => self.handle_leave_group(req).await,
            Request::ValidateGroupFetch(req) => self.handle_validate_group_fetch(req).await,
            Request::Health => Response::HealthOk,
            Request::Handshake { client_version } => {
                if client_version == crate::protocol::PROTOCOL_VERSION {
                    Response::HandshakeOk {
                        server_version: crate::protocol::PROTOCOL_VERSION,
                    }
                } else {
                    Response::Error(format!(
                        "unsupported protocol version: client {} server {}",
                        client_version,
                        crate::protocol::PROTOCOL_VERSION
                    ))
                }
            }
        }
    }

    async fn handle_join_group(&self, req: JoinGroupRequest) -> Response {
        if let Err(err) = self
            .authz
            .authorize(req.auth.as_deref(), &req.topic, Action::Fetch, 0)
            .await
        {
            inc_request("join_group", "auth_err");
            return Response::Error(format!("auth failed: {err}"));
        }

        if let Some(response) = self
            .forward_group_request(
                &req.group_id,
                Request::JoinGroup(req.clone()),
                "join_group_forward",
            )
            .await
        {
            return response;
        }

        let partitions = match self.resolve_group_partitions(&req.topic) {
            Ok(partitions) => partitions,
            Err(response) => {
                inc_request("join_group", "routing_err");
                return response;
            }
        };

        match self
            .consumer_groups
            .join(&req.group_id, &req.topic, req.member_id.as_deref(), &partitions)
        {
            Ok(joined) => {
                inc_request("join_group", "ok");
                Response::GroupJoined {
                    group_id: joined.group_id,
                    member_id: joined.member_id,
                    generation: joined.generation,
                    heartbeat_interval_ms: joined.heartbeat_interval_ms,
                    session_timeout_ms: joined.session_timeout_ms,
                    assignments: joined.assignments,
                }
            }
            Err(err) => {
                inc_request("join_group", "error");
                self.group_error_response(&req.group_id, err)
            }
        }
    }

    async fn handle_heartbeat(&self, req: HeartbeatRequest) -> Response {
        if let Err(err) = self
            .authz
            .authorize(req.auth.as_deref(), &req.topic, Action::Fetch, 0)
            .await
        {
            inc_request("heartbeat", "auth_err");
            return Response::Error(format!("auth failed: {err}"));
        }

        if let Some(response) = self
            .forward_group_request(
                &req.group_id,
                Request::Heartbeat(req.clone()),
                "heartbeat_forward",
            )
            .await
        {
            return response;
        }

        let partitions = match self.resolve_group_partitions(&req.topic) {
            Ok(partitions) => partitions,
            Err(response) => {
                inc_request("heartbeat", "routing_err");
                return response;
            }
        };

        match self.consumer_groups.heartbeat(
            &req.group_id,
            &req.topic,
            &req.member_id,
            req.generation,
            &partitions,
        ) {
            Ok(outcome) => {
                inc_request("heartbeat", "ok");
                Response::HeartbeatOk {
                    group_id: outcome.group_id,
                    member_id: outcome.member_id,
                    generation: outcome.generation,
                }
            }
            Err(err) => {
                inc_request("heartbeat", "error");
                self.group_error_response(&req.group_id, err)
            }
        }
    }

    async fn handle_commit_offset(&self, req: CommitOffsetRequest) -> Response {
        if let Err(err) = self
            .authz
            .authorize(req.auth.as_deref(), &req.topic, Action::Fetch, 0)
            .await
        {
            inc_request("commit_offset", "auth_err");
            return Response::Error(format!("auth failed: {err}"));
        }

        if let Some(response) = self
            .forward_group_request(
                &req.group_id,
                Request::CommitOffset(req.clone()),
                "commit_offset_forward",
            )
            .await
        {
            return response;
        }

        let partitions = match self.resolve_group_partitions(&req.topic) {
            Ok(partitions) => partitions,
            Err(response) => {
                inc_request("commit_offset", "routing_err");
                return response;
            }
        };

        match self.consumer_groups.commit_offset(
            &req.group_id,
            &req.topic,
            &req.member_id,
            req.generation,
            req.partition,
            req.offset,
            &partitions,
        ) {
            Ok(outcome) => {
                inc_request("commit_offset", "ok");
                Response::OffsetCommitted {
                    group_id: outcome.group_id,
                    member_id: outcome.member_id,
                    generation: outcome.generation,
                    topic: outcome.topic,
                    partition: outcome.partition,
                    offset: outcome.offset,
                }
            }
            Err(err) => {
                inc_request("commit_offset", "error");
                self.group_error_response(&req.group_id, err)
            }
        }
    }

    async fn handle_leave_group(&self, req: LeaveGroupRequest) -> Response {
        if let Err(err) = self
            .authz
            .authorize(req.auth.as_deref(), &req.topic, Action::Fetch, 0)
            .await
        {
            inc_request("leave_group", "auth_err");
            return Response::Error(format!("auth failed: {err}"));
        }

        if let Some(response) = self
            .forward_group_request(
                &req.group_id,
                Request::LeaveGroup(req.clone()),
                "leave_group_forward",
            )
            .await
        {
            return response;
        }

        let partitions = match self.resolve_group_partitions(&req.topic) {
            Ok(partitions) => partitions,
            Err(response) => {
                inc_request("leave_group", "routing_err");
                return response;
            }
        };

        match self.consumer_groups.leave(
            &req.group_id,
            &req.topic,
            &req.member_id,
            req.generation,
            &partitions,
        ) {
            Ok(outcome) => {
                inc_request("leave_group", "ok");
                Response::GroupLeft {
                    group_id: outcome.group_id,
                    member_id: outcome.member_id,
                    generation: outcome.generation,
                }
            }
            Err(err) => {
                inc_request("leave_group", "error");
                self.group_error_response(&req.group_id, err)
            }
        }
    }

    async fn handle_validate_group_fetch(&self, req: ValidateGroupFetchRequest) -> Response {
        if let Err(err) = self
            .authz
            .authorize(req.auth.as_deref(), &req.topic, Action::Replicate, 0)
            .await
        {
            inc_request("validate_group_fetch", "auth_err");
            return Response::Error(format!("auth failed: {err}"));
        }

        if let Some(response) = self
            .forward_group_request(
                &req.group_id,
                Request::ValidateGroupFetch(req.clone()),
                "validate_group_fetch_forward",
            )
            .await
        {
            return response;
        }

        let partitions = match self.resolve_group_partitions(&req.topic) {
            Ok(partitions) => partitions,
            Err(response) => {
                inc_request("validate_group_fetch", "routing_err");
                return response;
            }
        };

        match self.consumer_groups.authorize_fetch(
            &req.group_id,
            &req.topic,
            &req.member_id,
            req.generation,
            req.partition,
            &partitions,
        ) {
            Ok(()) => {
                inc_request("validate_group_fetch", "ok");
                Response::GroupFetchAuthorized {
                    group_id: req.group_id,
                    member_id: req.member_id,
                    generation: req.generation,
                    topic: req.topic,
                    partition: req.partition,
                }
            }
            Err(err) => {
                inc_request("validate_group_fetch", "error");
                self.group_error_response(&req.group_id, err)
            }
        }
    }

    async fn handle_group_fetch(&self, req: GroupFetchRequest) -> Response {
        let bytes = req.max_bytes as u64;
        if let Err(err) = self
            .authz
            .authorize(req.auth.as_deref(), &req.topic, Action::Fetch, bytes)
            .await
        {
            inc_request("group_fetch", "auth_err");
            return Response::Error(format!("auth failed: {err}"));
        }

        if let Some(response) = self.validate_group_fetch_access(&req).await {
            return response;
        }

        match self
            .storage
            .fetch_async(&req.topic, req.partition, req.offset, req.max_bytes)
            .await
        {
            Ok(records) => {
                inc_request("group_fetch", "ok");
                let fetched_bytes: u64 = records
                    .iter()
                    .map(|r| {
                        r.record.key.len() as u64
                            + r.record.value.len() as u64
                            + std::mem::size_of::<i64>() as u64
                    })
                    .sum();
                add_bytes("fetch", fetched_bytes);
                Response::Fetched { records }
            }
            Err(err) => {
                error!(error = %err, "group fetch failed");
                inc_request("group_fetch", "error");
                Response::Error(err.to_string())
            }
        }
    }

    async fn handle_produce(&self, req: ProduceRequest) -> Response {
        if req.records.is_empty() {
            inc_request("produce", "empty");
            return Response::Error("empty produce batch".to_string());
        }

        let produce_bytes: u64 = req
            .records
            .iter()
            .map(|r| r.key.len() as u64 + r.value.len() as u64 + std::mem::size_of::<i64>() as u64)
            .sum();

        if produce_bytes > self.max_batch_bytes {
            inc_request("produce", "too_large");
            return Response::Error(format!(
                "batch too large: {} bytes > limit {}",
                produce_bytes, self.max_batch_bytes
            ));
        }

        if let Err(err) = self
            .authz
            .authorize(
                req.auth.as_deref(),
                &req.topic,
                Action::Produce,
                produce_bytes,
            )
            .await
        {
            inc_request("produce", "auth_err");
            return Response::Error(format!("auth failed: {err}"));
        }

        if !self.metadata.is_local_leader(&req.topic, req.partition) {
            let leader = self
                .metadata
                .leader(&req.topic, req.partition)
                .map(|l| l.to_string());
            inc_request("produce", "not_leader");
            return Response::NotLeader { leader };
        }

        let need_acks = self.ack_quorum.max(1);
        let followers = self.metadata.followers(&req.topic, req.partition);
        let leader_epoch = self
            .metadata
            .leader_epoch(&req.topic, req.partition)
            .unwrap_or(0);
        let records_for_replication = if need_acks > 1 {
            Some(req.records.clone())
        } else {
            None
        };
        let records_for_storage = req.records.clone();

        match self
            .storage
            .append_async(&req.topic, req.partition, records_for_storage)
            .await
        {
            Ok((base, last)) => {
                inc_request("produce", "ok");
                add_bytes("produce", produce_bytes);
                let mut acks = 1usize;
                if let Some(records) = records_for_replication {
                    let entries: Vec<ReplicaRecord> = records
                        .into_iter()
                        .enumerate()
                        .map(|(i, record)| ReplicaRecord {
                            offset: base + i as u64,
                            record,
                        })
                        .collect();
                    let replicated = self
                        .replicator
                        .replicate(&req.topic, req.partition, leader_epoch, entries, followers)
                        .await;
                    acks += replicated;
                }

                if acks < need_acks {
                    return Response::Error(format!("acks {acks}/{need_acks} not satisfied"));
                }

                Response::Produced {
                    base_offset: base,
                    last_offset: last,
                    acks: acks as u32,
                }
            }
            Err(err) => {
                error!(error = %err, "produce failed");
                inc_request("produce", "error");
                Response::Error(err.to_string())
            }
        }
    }

    async fn handle_replicate(&self, req: crate::protocol::ReplicateRequest) -> Response {
        if req.entries.is_empty() {
            inc_request("replicate", "empty");
            return Response::Error("empty replicate batch".to_string());
        }

        let replicate_bytes: u64 = req
            .entries
            .iter()
            .map(|r| {
                r.record.key.len() as u64
                    + r.record.value.len() as u64
                    + std::mem::size_of::<i64>() as u64
            })
            .sum();

        if replicate_bytes > self.max_batch_bytes {
            inc_request("replicate", "too_large");
            return Response::Error(format!(
                "replication batch too large: {} bytes > limit {}",
                replicate_bytes, self.max_batch_bytes
            ));
        }

        if let Err(err) = self
            .authz
            .authorize(
                req.auth.as_deref(),
                &req.topic,
                Action::Replicate,
                replicate_bytes,
            )
            .await
        {
            inc_request("replicate", "auth_err");
            return Response::Error(format!("auth failed: {err}"));
        }

        let current_epoch = self
            .metadata
            .leader_epoch(&req.topic, req.partition)
            .unwrap_or(0);

        if req.leader_epoch < current_epoch {
            inc_request("replicate", "fenced");
            return Response::Error("fenced: stale leader epoch".to_string());
        }

        if !self
            .metadata
            .is_local_follower(&req.topic, req.partition, &req.leader_id)
        {
            let leader = self.metadata.leader(&req.topic, req.partition);
            inc_request("replicate", "not_follower");
            return Response::NotLeader { leader };
        }

        let entries: Vec<(u64, crate::protocol::Record)> = req
            .entries
            .into_iter()
            .map(|r| (r.offset, r.record))
            .collect();

        match self
            .storage
            .append_with_offsets_async(&req.topic, req.partition, entries)
            .await
        {
            Ok((base, last)) => {
                inc_request("replicate", "ok");
                add_bytes("replicate", replicate_bytes);
                Response::Produced {
                    base_offset: base,
                    last_offset: last,
                    acks: 1,
                }
            }
            Err(err) => {
                error!(error = %err, "replica append failed");
                inc_request("replicate", "error");
                Response::Error(err.to_string())
            }
        }
    }

    async fn handle_fetch(&self, req: FetchRequest) -> Response {
        let bytes = req.max_bytes as u64;
        if let Err(err) = self
            .authz
            .authorize(req.auth.as_deref(), &req.topic, Action::Fetch, bytes)
            .await
        {
            inc_request("fetch", "auth_err");
            return Response::Error(format!("auth failed: {err}"));
        }
        match self
            .storage
            .fetch_async(&req.topic, req.partition, req.offset, req.max_bytes)
            .await
        {
            Ok(records) => {
                inc_request("fetch", "ok");
                let fetched_bytes: u64 = records
                    .iter()
                    .map(|r| {
                        r.record.key.len() as u64
                            + r.record.value.len() as u64
                            + std::mem::size_of::<i64>() as u64
                    })
                    .sum();
                add_bytes("fetch", fetched_bytes);
                Response::Fetched { records }
            }
            Err(err) => {
                error!(error = %err, "fetch failed");
                inc_request("fetch", "error");
                Response::Error(err.to_string())
            }
        }
    }

    fn resolve_group_partitions(&self, topic: &str) -> Result<Vec<u32>, Response> {
        if self.metadata.has_source() {
            let partitions = self.metadata.partitions_for_topic(topic);
            if partitions.is_empty() {
                return Err(Response::Error(format!(
                    "topic '{topic}' is not configured for consumer groups"
                )));
            }
            return Ok(partitions);
        }

        let partitions = self.storage.partitions_for_topic(topic);
        if partitions.is_empty() {
            Ok(vec![0])
        } else {
            Ok(partitions)
        }
    }

    fn group_error_response(&self, group_id: &str, err: GroupError) -> Response {
        match err {
            GroupError::UnknownGroup | GroupError::UnknownMember => Response::RebalanceRequired {
                group_id: group_id.to_string(),
                generation: 0,
            },
            GroupError::StaleGeneration { current_generation } => Response::RebalanceRequired {
                group_id: group_id.to_string(),
                generation: current_generation,
            },
            GroupError::PartitionNotAssigned => {
                Response::Error("partition not assigned to consumer group member".to_string())
            }
            GroupError::TopicMismatch => {
                Response::Error("consumer group topic mismatch".to_string())
            }
            GroupError::OffsetRegression { current_offset } => Response::Error(format!(
                "committed offset regression: current committed offset is {current_offset}"
            )),
            GroupError::InvalidRequest(field) => {
                Response::Error(format!("invalid consumer group request: {field}"))
            }
        }
    }

    async fn forward_group_request(
        &self,
        group_id: &str,
        request: Request,
        metric: &str,
    ) -> Option<Response> {
        let coordinator = self.group_coordinator(group_id);
        if coordinator == self.metadata.self_id() {
            return None;
        }

        match self.replicator.send_request_to_node(&coordinator, request).await {
            Ok(response) => {
                inc_request(metric, "ok");
                Some(response)
            }
            Err(err) => {
                inc_request(metric, "error");
                Some(Response::Error(format!(
                    "consumer group coordinator request failed: {err}"
                )))
            }
        }
    }

    async fn validate_group_fetch_access(&self, req: &GroupFetchRequest) -> Option<Response> {
        let coordinator = self.group_coordinator(&req.group_id);
        if coordinator == self.metadata.self_id() {
            let partitions = match self.resolve_group_partitions(&req.topic) {
                Ok(partitions) => partitions,
                Err(response) => {
                    inc_request("group_fetch", "routing_err");
                    return Some(response);
                }
            };

            if let Err(err) = self.consumer_groups.authorize_fetch(
                &req.group_id,
                &req.topic,
                &req.member_id,
                req.generation,
                req.partition,
                &partitions,
            ) {
                inc_request("group_fetch", "ownership_err");
                return Some(self.group_error_response(&req.group_id, err));
            }
            return None;
        }

        let validation = Request::ValidateGroupFetch(ValidateGroupFetchRequest {
            group_id: req.group_id.clone(),
            topic: req.topic.clone(),
            member_id: req.member_id.clone(),
            generation: req.generation,
            partition: req.partition,
            auth: self.replicator.internal_auth_token(),
        });

        match self.replicator.send_request_to_node(&coordinator, validation).await {
            Ok(Response::GroupFetchAuthorized { .. }) => None,
            Ok(response) => Some(response),
            Err(err) => Some(Response::Error(format!(
                "consumer group ownership validation failed: {err}"
            ))),
        }
    }

    fn group_coordinator(&self, group_id: &str) -> String {
        self.metadata
            .consumer_group_coordinator(group_id)
            .unwrap_or_else(|| self.metadata.self_id().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::Metadata;
    use crate::protocol::{
        CommitOffsetRequest, GroupFetchRequest, JoinGroupRequest, ProduceRequest,
        ReplicateRequest, Request,
    };
    use crate::replication::Replicator;
    use tempfile::tempdir;

    #[tokio::test]
    async fn produce_rejects_empty_batch() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), 1024 * 1024, None, None, 16, false).unwrap();
        let metadata = Arc::new(Metadata::single_node("node-1".to_string()));
        let authz = Authz::load(None).unwrap();
        let broker = Broker::new(
            storage,
            Replicator::disabled(),
            metadata,
            1,
            authz,
            4 * 1024 * 1024,
            3_000,
            15_000,
        );

        let response = broker
            .handle(Request::Produce(ProduceRequest {
                topic: "topic".to_string(),
                partition: 0,
                records: Vec::new(),
                auth: None,
            }))
            .await;

        match response {
            Response::Error(msg) => assert!(msg.contains("empty produce batch")),
            other => panic!("expected error response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replicate_rejects_empty_batch() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), 1024 * 1024, None, None, 16, false).unwrap();
        let metadata = Arc::new(Metadata::single_node("node-1".to_string()));
        let authz = Authz::load(None).unwrap();
        let broker = Broker::new(
            storage,
            Replicator::disabled(),
            metadata,
            1,
            authz,
            4 * 1024 * 1024,
            3_000,
            15_000,
        );

        let response = broker
            .handle(Request::Replicate(ReplicateRequest {
                leader_id: "node-2".to_string(),
                leader_epoch: 1,
                topic: "topic".to_string(),
                partition: 0,
                entries: Vec::new(),
                auth: None,
            }))
            .await;

        match response {
            Response::Error(msg) => assert!(msg.contains("empty replicate batch")),
            other => panic!("expected error response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn consumer_group_assigns_partition_to_single_member_and_commits_offsets() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), 1024 * 1024, None, None, 16, false).unwrap();
        let metadata = Arc::new(Metadata::single_node("node-1".to_string()));
        let authz = Authz::load(None).unwrap();
        let broker = Broker::new(
            storage,
            Replicator::disabled(),
            metadata,
            1,
            authz,
            4 * 1024 * 1024,
            3_000,
            15_000,
        );

        let produced = broker
            .handle(Request::Produce(ProduceRequest {
                topic: "jobs".to_string(),
                partition: 0,
                records: vec![crate::protocol::Record {
                    key: b"job-1".to_vec(),
                    value: b"run".to_vec(),
                    timestamp: 1,
                }],
                auth: None,
            }))
            .await;
        assert!(matches!(produced, Response::Produced { .. }));

        let first = broker
            .handle(Request::JoinGroup(JoinGroupRequest {
                group_id: "workers".to_string(),
                topic: "jobs".to_string(),
                member_id: None,
                auth: None,
            }))
            .await;
        let (first_member_id, first_generation) = match first {
            Response::GroupJoined {
                member_id,
                generation,
                assignments,
                ..
            } => {
                assert_eq!(1, assignments.len());
                assert_eq!(0, assignments[0].partition);
                assert_eq!(0, assignments[0].offset);
                (member_id, generation)
            }
            other => panic!("unexpected join response: {other:?}"),
        };

        let second = broker
            .handle(Request::JoinGroup(JoinGroupRequest {
                group_id: "workers".to_string(),
                topic: "jobs".to_string(),
                member_id: None,
                auth: None,
            }))
            .await;
        let (second_member_id, second_generation) = match second {
            Response::GroupJoined {
                member_id,
                generation,
                assignments,
                ..
            } => {
                assert!(assignments.is_empty());
                (member_id, generation)
            }
            other => panic!("unexpected join response: {other:?}"),
        };
        assert!(second_generation > first_generation);

        let rejoined_first = broker
            .handle(Request::JoinGroup(JoinGroupRequest {
                group_id: "workers".to_string(),
                topic: "jobs".to_string(),
                member_id: Some(first_member_id.clone()),
                auth: None,
            }))
            .await;
        let generation = match rejoined_first {
            Response::GroupJoined {
                generation,
                assignments,
                ..
            } => {
                assert_eq!(1, assignments.len());
                generation
            }
            other => panic!("unexpected rejoin response: {other:?}"),
        };

        let stale_fetch = broker
            .handle(Request::GroupFetch(GroupFetchRequest {
                group_id: "workers".to_string(),
                topic: "jobs".to_string(),
                member_id: first_member_id.clone(),
                generation: first_generation,
                partition: 0,
                offset: 0,
                max_bytes: 1024,
                auth: None,
            }))
            .await;
        assert!(matches!(
            stale_fetch,
            Response::RebalanceRequired { generation: g, .. } if g == generation
        ));

        let wrong_owner = broker
            .handle(Request::GroupFetch(GroupFetchRequest {
                group_id: "workers".to_string(),
                topic: "jobs".to_string(),
                member_id: second_member_id,
                generation,
                partition: 0,
                offset: 0,
                max_bytes: 1024,
                auth: None,
            }))
            .await;
        assert!(matches!(
            wrong_owner,
            Response::Error(msg) if msg.contains("partition not assigned")
        ));

        let fetched = broker
            .handle(Request::GroupFetch(GroupFetchRequest {
                group_id: "workers".to_string(),
                topic: "jobs".to_string(),
                member_id: first_member_id.clone(),
                generation,
                partition: 0,
                offset: 0,
                max_bytes: 1024,
                auth: None,
            }))
            .await;
        match fetched {
            Response::Fetched { records } => {
                assert_eq!(1, records.len());
                assert_eq!(b"run".to_vec(), records[0].record.value);
            }
            other => panic!("unexpected group fetch response: {other:?}"),
        }

        let committed = broker
            .handle(Request::CommitOffset(CommitOffsetRequest {
                group_id: "workers".to_string(),
                topic: "jobs".to_string(),
                member_id: first_member_id,
                generation,
                partition: 0,
                offset: 1,
                auth: None,
            }))
            .await;
        assert!(matches!(
            committed,
            Response::OffsetCommitted { offset: 1, .. }
        ));
    }
}
