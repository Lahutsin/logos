use std::collections::HashMap;
use std::fs;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse error: {0}")]
    Parse(#[from] serde_json::Error),
}

pub type MetadataResult<T> = Result<T, MetadataError>;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct PartitionAssignment {
    pub leader: String,
    #[serde(default)]
    pub followers: Vec<String>,
    #[serde(default)]
    pub epoch: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetadataFile {
    pub self_id: Option<String>,
    pub nodes: HashMap<String, String>,
    pub topics: HashMap<String, HashMap<u32, PartitionAssignment>>,
}

#[derive(Clone, Debug)]
pub struct Metadata {
    self_id: String,
    source_path: Option<PathBuf>,
    state: Arc<RwLock<MetadataState>>,
}

#[derive(Clone, Debug)]
struct MetadataState {
    nodes: HashMap<String, String>,
    topics: HashMap<String, HashMap<u32, PartitionAssignment>>,
    generation: u64,
}

impl Metadata {
    pub fn load(path: Option<&Path>, default_self: String) -> MetadataResult<Self> {
        match path {
            Some(p) => {
                let bytes = fs::read(p)?;
                let parsed: MetadataFile = serde_json::from_slice(&bytes)?;
                let self_id = parsed.self_id.clone().unwrap_or(default_self);
                let state = MetadataState {
                    nodes: parsed.nodes,
                    topics: parsed.topics,
                    generation: 0,
                };
                Ok(Self {
                    self_id,
                    source_path: Some(p.to_path_buf()),
                    state: Arc::new(RwLock::new(state)),
                })
            }
            None => Ok(Self::single_node(default_self)),
        }
    }

    pub fn single_node(self_id: String) -> Self {
        let state = MetadataState {
            nodes: HashMap::from([(self_id.clone(), "0.0.0.0:0".to_string())]),
            topics: HashMap::new(),
            generation: 0,
        };
        Self {
            self_id,
            source_path: None,
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub fn leader(&self, topic: &str, partition: u32) -> Option<String> {
        self.with_state(|s| {
            s.topics
                .get(topic)
                .and_then(|parts| parts.get(&partition))
                .map(|p| p.leader.clone())
        })
    }

    pub fn followers(&self, topic: &str, partition: u32) -> Vec<String> {
        self.with_state(|s| {
            s.topics
                .get(topic)
                .and_then(|parts| parts.get(&partition))
                .map(|p| p.followers.clone())
                .unwrap_or_default()
        })
    }

    pub fn leader_epoch(&self, topic: &str, partition: u32) -> Option<u64> {
        self.with_state(|s| {
            s.topics
                .get(topic)
                .and_then(|parts| parts.get(&partition))
                .map(|p| p.epoch)
        })
    }

    pub fn partitions_for_topic(&self, topic: &str) -> Vec<u32> {
        self.with_state(|s| {
            let mut partitions: Vec<u32> = s
                .topics
                .get(topic)
                .map(|parts| parts.keys().copied().collect())
                .unwrap_or_default();
            partitions.sort_unstable();
            partitions
        })
    }

    pub fn leaders_for_topic(&self, topic: &str) -> Vec<String> {
        self.with_state(|s| {
            let mut leaders: Vec<String> = s
                .topics
                .get(topic)
                .map(|parts| {
                    parts
                        .values()
                        .map(|assignment| assignment.leader.clone())
                        .collect()
                })
                .unwrap_or_default();
            leaders.sort();
            leaders.dedup();
            leaders
        })
    }

    pub fn node_ids(&self) -> Vec<String> {
        self.with_state(|s| {
            let mut nodes: Vec<String> = s.nodes.keys().cloned().collect();
            nodes.sort();
            nodes
        })
    }

    pub fn consumer_group_coordinator(&self, group_id: &str) -> Option<String> {
        let nodes = self.node_ids();
        if nodes.is_empty() {
            return None;
        }

        let mut hasher = StableHasher::default();
        hasher.write(group_id.as_bytes());
        let index = (hasher.finish() as usize) % nodes.len();
        nodes.get(index).cloned()
    }

    pub fn address(&self, node_id: &str) -> Option<String> {
        self.with_state(|s| s.nodes.get(node_id).cloned())
    }

    pub fn is_local_leader(&self, topic: &str, partition: u32) -> bool {
        self.leader(topic, partition)
            .map(|l| l == self.self_id)
            .unwrap_or(!self.has_source())
    }

    pub fn is_local_follower(&self, topic: &str, partition: u32, leader: &str) -> bool {
        if self.self_id == leader {
            return false;
        }
        self.with_state(|s| {
            s.topics
                .get(topic)
                .and_then(|parts| parts.get(&partition))
                .map(|p| p.leader == leader && p.followers.contains(&self.self_id))
                .unwrap_or(false)
        })
    }

    pub fn generation(&self) -> u64 {
        self.with_state(|s| s.generation)
    }

    pub fn refresh(&self) -> MetadataResult<bool> {
        let path = match &self.source_path {
            Some(p) => p,
            None => return Ok(false),
        };
        let bytes = fs::read(path)?;
        let parsed: MetadataFile = serde_json::from_slice(&bytes)?;

        let mut guard = self.state.write();
        if guard.nodes == parsed.nodes && guard.topics == parsed.topics {
            return Ok(false);
        }

        guard.nodes = parsed.nodes;
        guard.topics = parsed.topics;
        guard.generation = guard.generation.saturating_add(1);
        Ok(true)
    }

    pub fn has_source(&self) -> bool {
        self.source_path.is_some()
    }

    pub fn self_id(&self) -> &str {
        &self.self_id
    }

    fn with_state<R>(&self, f: impl FnOnce(&MetadataState) -> R) -> R {
        let guard = self.state.read();
        f(&guard)
    }
}

#[derive(Default)]
struct StableHasher(u64);

impl Hasher for StableHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.0 == 0 {
            0xcbf29ce484222325
        } else {
            self.0
        };

        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }

        self.0 = hash;
    }

    fn finish(&self) -> u64 {
        if self.0 == 0 {
            0xcbf29ce484222325
        } else {
            self.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn unknown_partition_is_not_local_leader_for_managed_metadata() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metadata.json");
        let payload = serde_json::json!({
            "self_id": "node-1",
            "nodes": {
                "node-1": "127.0.0.1:9092"
            },
            "topics": {
                "known": {
                    "0": {
                        "leader": "node-1",
                        "followers": [],
                        "epoch": 1
                    }
                }
            }
        });
        std::fs::write(&path, serde_json::to_vec(&payload).unwrap()).unwrap();

        let metadata = Metadata::load(Some(&path), "node-1".to_string()).unwrap();

        assert!(!metadata.is_local_leader("unknown-topic", 0));
        assert!(!metadata.is_local_leader("known", 99));
    }

    #[test]
    fn unknown_partition_is_local_leader_in_single_node_mode() {
        let metadata = Metadata::single_node("node-1".to_string());
        assert!(metadata.is_local_leader("any-topic", 0));
    }

    #[test]
    fn consumer_group_coordinator_is_stable() {
        let metadata = Metadata::single_node("node-1".to_string());
        assert_eq!(
            Some("node-1".to_string()),
            metadata.consumer_group_coordinator("workers")
        );
    }
}
