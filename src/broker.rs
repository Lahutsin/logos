use std::sync::Arc;

use crate::metadata::Metadata;
use crate::metrics::{add_bytes, inc_request};
use crate::protocol::{FetchRequest, ProduceRequest, ReplicaRecord, Request, Response};
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
}

impl Broker {
    pub fn new(
        storage: Storage,
        replicator: Replicator,
        metadata: Arc<Metadata>,
        ack_quorum: usize,
        authz: Authz,
        max_batch_bytes: u64,
    ) -> Self {
        Self {
            storage,
            metadata,
            ack_quorum: ack_quorum.max(1),
            replicator,
            authz,
            max_batch_bytes,
        }
    }

    pub async fn handle(&self, request: Request) -> Response {
        match request {
            Request::Produce(req) => self.handle_produce(req).await,
            Request::Replicate(req) => self.handle_replicate(req).await,
            Request::Fetch(req) => self.handle_fetch(req).await,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::Metadata;
    use crate::protocol::{ProduceRequest, ReplicateRequest, Request};
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
}
