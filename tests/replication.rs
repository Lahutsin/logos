use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use logos::broker::Broker;
use logos::metadata::Metadata;
use logos::protocol::{self, Record, Request, Response};
use logos::replication::Replicator;
use logos::sdk::{spawn_group_heartbeats, Client, GroupHeartbeatConfig};
use logos::security::{Authz, TlsEndpoints};
use logos::storage::Storage;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

struct ServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl ServerHandle {
    async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.task.await;
    }
}

fn plaintext_tls() -> TlsEndpoints {
    TlsEndpoints {
        acceptor: None,
        connector: None,
        server_name: None,
    }
}

fn open_storage(path: &Path) -> Result<Storage> {
    Storage::open(path, 1_048_576, None, None, 1, false).map_err(Into::into)
}

fn write_metadata(
    path: &Path,
    topic: &str,
    leader: &str,
    followers: &[&str],
    epoch: u64,
    nodes: &[(&str, String)],
) -> Result<()> {
    write_partitioned_metadata(
        path,
        topic,
        &[(0, leader, followers, epoch)],
        nodes,
    )
}

fn write_partitioned_metadata(
    path: &Path,
    topic: &str,
    partitions_def: &[(u32, &str, &[&str], u64)],
    nodes: &[(&str, String)],
) -> Result<()> {
    let mut nodes_obj = serde_json::Map::new();
    for (id, addr) in nodes {
        nodes_obj.insert((*id).to_string(), Value::String(addr.clone()));
    }

    let mut partitions = serde_json::Map::new();
    for (partition, leader, followers, epoch) in partitions_def {
        partitions.insert(
            partition.to_string(),
            serde_json::json!({
                "leader": leader,
                "followers": followers,
                "epoch": epoch,
            }),
        );
    }

    let mut topics = serde_json::Map::new();
    topics.insert(topic.to_string(), Value::Object(partitions));

    let mut root = serde_json::Map::new();
    root.insert("self_id".to_string(), Value::Null);
    root.insert("nodes".to_string(), Value::Object(nodes_obj));
    root.insert("topics".to_string(), Value::Object(topics));

    let bytes = serde_json::to_vec_pretty(&Value::Object(root))?;
    std::fs::write(path, bytes)?;
    Ok(())
}

fn make_record(key: &str, value: &str, ts: i64) -> Record {
    Record {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        timestamp: ts,
    }
}

async fn start_node(
    node_id: &str,
    listener: TcpListener,
    metadata_path: &Path,
    storage: Storage,
    ack_quorum: usize,
) -> Result<ServerHandle> {
    let metadata = Arc::new(Metadata::load(Some(metadata_path), node_id.to_string())?);
    let replicator = Replicator::new(metadata.clone(), 1_000, 3, 50, plaintext_tls(), None);
    let authz = Authz::load(None)?;
    let broker = Broker::new(
        storage,
        replicator,
        metadata,
        ack_quorum,
        authz,
        4 * 1024 * 1024,
        3_000,
        15_000,
    );

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        serve(listener, broker, shutdown_rx).await;
    });

    Ok(ServerHandle {
        shutdown: Some(shutdown_tx),
        task,
    })
}

async fn serve(listener: TcpListener, broker: Broker, mut shutdown: oneshot::Receiver<()>) {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_res = listener.accept() => {
                let (stream, _) = match accept_res {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let b = broker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, b).await;
                });
            }
        }
    }
}

async fn handle_connection(stream: tokio::net::TcpStream, broker: Broker) -> Result<()> {
    let codec = LengthDelimitedCodec::builder()
        .length_field_length(4)
        .new_codec();
    let mut framed = Framed::new(stream, codec);

    while let Some(frame) = framed.next().await {
        let bytes = frame?;
        let request: Request = protocol::decode(&bytes)?;
        let response = broker.handle(request).await;
        let encoded = protocol::encode(&response)?;
        framed.send(Bytes::from(encoded)).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_satisfies_quorum_and_copies_data() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let topic = "replica-quorum";

    let leader_listener = TcpListener::bind("127.0.0.1:0").await?;
    let follower_listener = TcpListener::bind("127.0.0.1:0").await?;
    let leader_addr = leader_listener.local_addr()?;
    let follower_addr = follower_listener.local_addr()?;

    let metadata_path = tmp.path().join("metadata.json");
    write_metadata(
        &metadata_path,
        topic,
        "node-1",
        &["node-2"],
        1,
        &[
            ("node-1", leader_addr.to_string()),
            ("node-2", follower_addr.to_string()),
        ],
    )?;

    let leader_storage = open_storage(&tmp.path().join("leader"))?;
    let follower_storage = open_storage(&tmp.path().join("follower"))?;

    let leader = start_node(
        "node-1",
        leader_listener,
        &metadata_path,
        leader_storage.clone(),
        2,
    )
    .await?;
    let follower = start_node(
        "node-2",
        follower_listener,
        &metadata_path,
        follower_storage.clone(),
        1,
    )
    .await?;

    let mut producer = Client::connect(&leader_addr.to_string()).await?;
    let resp = producer
        .produce(topic, 0, vec![make_record("k1", "v1", 0)], None)
        .await?;

    match resp {
        Response::Produced {
            acks,
            base_offset,
            last_offset,
        } => {
            assert_eq!(2, acks);
            assert_eq!(0, base_offset);
            assert_eq!(0, last_offset);
        }
        other => panic!("unexpected produce response: {other:?}"),
    }

    let mut follower_client = Client::connect(&follower_addr.to_string()).await?;
    let fetched = follower_client
        .fetch(topic, 0, 0, 1024 * 1024, None)
        .await?;

    match fetched {
        Response::Fetched { records } => {
            assert_eq!(1, records.len());
            assert_eq!(0, records[0].offset);
            assert_eq!(b"v1".to_vec(), records[0].record.value);
        }
        other => panic!("unexpected fetch response: {other:?}"),
    }

    leader.stop().await;
    follower.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_rejects_non_contiguous_offsets_and_blocks_quorum() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let topic = "replica-truncate";

    let leader_listener = TcpListener::bind("127.0.0.1:0").await?;
    let follower_listener = TcpListener::bind("127.0.0.1:0").await?;
    let leader_addr = leader_listener.local_addr()?;
    let follower_addr = follower_listener.local_addr()?;

    let metadata_path = tmp.path().join("metadata.json");
    write_metadata(
        &metadata_path,
        topic,
        "node-1",
        &["node-2"],
        1,
        &[
            ("node-1", leader_addr.to_string()),
            ("node-2", follower_addr.to_string()),
        ],
    )?;

    let leader_storage = open_storage(&tmp.path().join("leader"))?;
    let follower_storage = open_storage(&tmp.path().join("follower"))?;

    let leader = start_node(
        "node-1",
        leader_listener,
        &metadata_path,
        leader_storage.clone(),
        2,
    )
    .await?;
    let follower = start_node(
        "node-2",
        follower_listener,
        &metadata_path,
        follower_storage.clone(),
        1,
    )
    .await?;

    follower_storage
        .append(topic, 0, vec![make_record("rogue", "stale", 1)])
        .context("manual append to follower should succeed")?;

    let mut producer = Client::connect(&leader_addr.to_string()).await?;
    let resp = producer
        .produce(topic, 0, vec![make_record("k1", "v1", 2)], None)
        .await?;

    match resp {
        Response::Error(msg) => {
            assert!(
                msg.contains("acks 1/2 not satisfied"),
                "unexpected error: {msg}"
            );
        }
        other => panic!("expected quorum failure, got {other:?}"),
    }

    let mut follower_client = Client::connect(&follower_addr.to_string()).await?;
    let fetched = follower_client
        .fetch(topic, 0, 0, 1024 * 1024, None)
        .await?;

    match fetched {
        Response::Fetched { records } => {
            assert_eq!(1, records.len());
            assert_eq!(b"stale".to_vec(), records[0].record.value);
        }
        other => panic!("unexpected fetch response: {other:?}"),
    }

    leader.stop().await;
    follower.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_recovers_after_leader_change_and_redelivery() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let topic = "replica-failover";

    // First epoch: node-1 is leader, node-2 follower.
    let leader_listener = TcpListener::bind("127.0.0.1:0").await?;
    let follower_listener = TcpListener::bind("127.0.0.1:0").await?;
    let leader_addr = leader_listener.local_addr()?;
    let follower_addr = follower_listener.local_addr()?;

    let metadata_path = tmp.path().join("meta-epoch1.json");
    write_metadata(
        &metadata_path,
        topic,
        "node-1",
        &["node-2"],
        1,
        &[
            ("node-1", leader_addr.to_string()),
            ("node-2", follower_addr.to_string()),
        ],
    )?;

    let leader_storage = open_storage(&tmp.path().join("leader"))?;
    let follower_storage = open_storage(&tmp.path().join("follower"))?;

    let leader = start_node(
        "node-1",
        leader_listener,
        &metadata_path,
        leader_storage.clone(),
        2,
    )
    .await?;
    let follower = start_node(
        "node-2",
        follower_listener,
        &metadata_path,
        follower_storage.clone(),
        1,
    )
    .await?;

    let mut client = Client::connect(&leader_addr.to_string()).await?;
    let resp = client
        .produce(topic, 0, vec![make_record("k1", "v1", 0)], None)
        .await?;
    assert!(matches!(resp, Response::Produced { acks: 2, .. }));

    leader.stop().await;
    follower.stop().await;

    // Second epoch: leader flips to node-2, redelivering to node-1 as follower.
    let new_leader_listener = TcpListener::bind("127.0.0.1:0").await?;
    let new_follower_listener = TcpListener::bind("127.0.0.1:0").await?;
    let new_leader_addr = new_leader_listener.local_addr()?;
    let new_follower_addr = new_follower_listener.local_addr()?;

    let metadata_path_epoch2 = tmp.path().join("meta-epoch2.json");
    write_metadata(
        &metadata_path_epoch2,
        topic,
        "node-2",
        &["node-1"],
        2,
        &[
            ("node-1", new_follower_addr.to_string()),
            ("node-2", new_leader_addr.to_string()),
        ],
    )?;

    let new_leader = start_node(
        "node-2",
        new_leader_listener,
        &metadata_path_epoch2,
        follower_storage.clone(),
        2,
    )
    .await?;
    let new_follower = start_node(
        "node-1",
        new_follower_listener,
        &metadata_path_epoch2,
        leader_storage.clone(),
        1,
    )
    .await?;

    let mut new_client = Client::connect(&new_leader_addr.to_string()).await?;
    let second = new_client
        .produce(topic, 0, vec![make_record("k2", "v2", 1)], None)
        .await?;
    assert!(matches!(
        second,
        Response::Produced {
            acks: 2,
            base_offset: 1,
            last_offset: 1
        }
    ));

    let mut leader_fetch = Client::connect(&new_leader_addr.to_string()).await?;
    let leader_records = leader_fetch.fetch(topic, 0, 0, 1024 * 1024, None).await?;

    let mut follower_fetch = Client::connect(&new_follower_addr.to_string()).await?;
    let follower_records = follower_fetch.fetch(topic, 0, 0, 1024 * 1024, None).await?;

    for resp in [leader_records, follower_records] {
        match resp {
            Response::Fetched { records } => {
                assert_eq!(2, records.len());
                assert_eq!(b"v1".to_vec(), records[0].record.value);
                assert_eq!(b"v2".to_vec(), records[1].record.value);
            }
            other => panic!("unexpected fetch response: {other:?}"),
        }
    }

    new_leader.stop().await;
    new_follower.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_groups_coordinate_across_partition_leaders() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let topic = "jobs";

    let node1_listener = TcpListener::bind("127.0.0.1:0").await?;
    let node2_listener = TcpListener::bind("127.0.0.1:0").await?;
    let node1_addr = node1_listener.local_addr()?;
    let node2_addr = node2_listener.local_addr()?;

    let metadata_path = tmp.path().join("metadata-groups.json");
    write_partitioned_metadata(
        &metadata_path,
        topic,
        &[
            (0, "node-1", &["node-2"], 1),
            (1, "node-2", &["node-1"], 1),
        ],
        &[
            ("node-1", node1_addr.to_string()),
            ("node-2", node2_addr.to_string()),
        ],
    )?;

    let node1_storage = open_storage(&tmp.path().join("node-1"))?;
    let node2_storage = open_storage(&tmp.path().join("node-2"))?;

    let node1 = start_node(
        "node-1",
        node1_listener,
        &metadata_path,
        node1_storage.clone(),
        2,
    )
    .await?;
    let node2 = start_node(
        "node-2",
        node2_listener,
        &metadata_path,
        node2_storage.clone(),
        2,
    )
    .await?;

    let mut partition0_producer = Client::connect(&node1_addr.to_string()).await?;
    assert!(matches!(
        partition0_producer
            .produce(topic, 0, vec![make_record("job-0", "run-0", 0)], None)
            .await?,
        Response::Produced { acks: 2, .. }
    ));

    let mut partition1_producer = Client::connect(&node2_addr.to_string()).await?;
    assert!(matches!(
        partition1_producer
            .produce(topic, 1, vec![make_record("job-1", "run-1", 1)], None)
            .await?,
        Response::Produced { acks: 2, .. }
    ));

    let metadata = Metadata::load(Some(&metadata_path), "node-1".to_string())?;
    let coordinator = metadata
        .consumer_group_coordinator("workers")
        .context("coordinator should be present")?;

    let (bootstrap_addr, fetch_addr) = if coordinator == "node-1" {
        (node2_addr, node2_addr)
    } else {
        (node1_addr, node1_addr)
    };

    let mut consumer = Client::connect(&bootstrap_addr.to_string()).await?;
    let joined = consumer.join_group("workers", topic, None, None).await?;
    let (member_id, generation, heartbeat_interval_ms) = match joined {
        Response::GroupJoined {
            member_id,
            generation,
            heartbeat_interval_ms,
            assignments,
            ..
        } => {
            assert_eq!(2, assignments.len());
            assert_eq!(0, assignments[0].partition);
            assert_eq!(1, assignments[1].partition);
            (member_id, generation, heartbeat_interval_ms)
        }
        other => panic!("unexpected join response: {other:?}"),
    };

    let heartbeat_handle = spawn_group_heartbeats(GroupHeartbeatConfig {
        addr: bootstrap_addr.to_string(),
        group_id: "workers".to_string(),
        topic: topic.to_string(),
        member_id: member_id.clone(),
        generation,
        interval: Duration::from_millis((heartbeat_interval_ms / 4).max(50)),
        auth: None,
    });

    sleep(Duration::from_millis(150)).await;

    let mut group_fetch_client = Client::connect(&fetch_addr.to_string()).await?;
    for partition in [0u32, 1u32] {
        let response = group_fetch_client
            .group_fetch(
                "workers",
                topic,
                &member_id,
                generation,
                partition,
                0,
                1024 * 1024,
                None,
            )
            .await?;

        match response {
            Response::Fetched { records } => {
                assert_eq!(1, records.len());
                assert_eq!(0, records[0].offset);
            }
            other => panic!("unexpected group fetch response: {other:?}"),
        }

        let commit = group_fetch_client
            .commit_offset("workers", topic, &member_id, generation, partition, 1, None)
            .await?;
        assert!(matches!(
            commit,
            Response::OffsetCommitted { partition: committed_partition, offset, .. }
                if committed_partition == partition && offset == 1
        ));
    }

    heartbeat_handle.stop().await?;

    let left = consumer
        .leave_group("workers", topic, &member_id, generation, None)
        .await?;
    assert!(matches!(left, Response::GroupLeft { .. }));

    node1.stop().await;
    node2.stop().await;
    Ok(())
}
