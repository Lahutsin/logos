use std::collections::{BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::protocol::{
    ConsumerGroupAssignment, ConsumerGroupMemberState, Offset, ReplicatedConsumerGroupState,
};

#[derive(Clone)]
pub struct ConsumerGroupCoordinator {
    inner: Arc<CoordinatorInner>,
}

struct CoordinatorInner {
    groups: Mutex<HashMap<String, GroupState>>,
    next_member_id: AtomicU64,
    heartbeat_interval: Duration,
    session_timeout: Duration,
    log_path: Option<PathBuf>,
    node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOutcome {
    pub group_id: String,
    pub member_id: String,
    pub generation: u64,
    pub heartbeat_interval_ms: u64,
    pub session_timeout_ms: u64,
    pub assignments: Vec<ConsumerGroupAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupError {
    UnknownGroup,
    UnknownMember,
    TopicMismatch,
    PartitionNotAssigned,
    StaleGeneration { current_generation: u64 },
    OffsetRegression { current_offset: Offset },
    InvalidRequest(&'static str),
    Persistence(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatOutcome {
    pub group_id: String,
    pub member_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOffsetOutcome {
    pub group_id: String,
    pub member_id: String,
    pub generation: u64,
    pub topic: String,
    pub partition: u32,
    pub offset: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitOffsetInput<'a> {
    pub group_id: &'a str,
    pub topic: &'a str,
    pub member_id: &'a str,
    pub generation: u64,
    pub partition: u32,
    pub offset: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveOutcome {
    pub group_id: String,
    pub member_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupState {
    topic: String,
    generation: u64,
    version: u64,
    partitions: Vec<u32>,
    members: HashMap<String, MemberState>,
    committed_offsets: HashMap<u32, Offset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MemberState {
    join_seq: u64,
    last_heartbeat_ms: u64,
    partitions: Vec<u32>,
}

impl ConsumerGroupCoordinator {
    pub fn new(heartbeat_interval_ms: u64, session_timeout_ms: u64) -> Self {
        Self::from_parts(
            None,
            "local".to_string(),
            heartbeat_interval_ms,
            session_timeout_ms,
            HashMap::new(),
            1,
        )
    }

    pub fn persistent(
        log_path: PathBuf,
        node_id: String,
        heartbeat_interval_ms: u64,
        session_timeout_ms: u64,
    ) -> anyhow::Result<Self> {
        let groups = load_log(&log_path, session_timeout_ms.max(1))?;
        let next_member_id = compute_next_member_id(&groups, &node_id);
        Ok(Self::from_parts(
            Some(log_path),
            node_id,
            heartbeat_interval_ms,
            session_timeout_ms,
            groups,
            next_member_id,
        ))
    }

    fn from_parts(
        log_path: Option<PathBuf>,
        node_id: String,
        heartbeat_interval_ms: u64,
        session_timeout_ms: u64,
        groups: HashMap<String, GroupState>,
        next_member_id: u64,
    ) -> Self {
        let heartbeat_interval = Duration::from_millis(heartbeat_interval_ms.max(1));
        let session_timeout =
            Duration::from_millis(session_timeout_ms.max(heartbeat_interval_ms.max(1)));
        Self {
            inner: Arc::new(CoordinatorInner {
                groups: Mutex::new(groups),
                next_member_id: AtomicU64::new(next_member_id.max(1)),
                heartbeat_interval,
                session_timeout,
                log_path,
                node_id,
            }),
        }
    }

    pub fn join(
        &self,
        group_id: &str,
        topic: &str,
        member_id: Option<&str>,
        partitions: &[u32],
    ) -> Result<JoinOutcome, GroupError> {
        validate_id(group_id, "group_id")?;
        validate_id(topic, "topic")?;

        let mut groups = self.inner.groups.lock();
        let desired_partitions = normalize_partitions(partitions);
        let now = now_millis();
        let group = groups
            .entry(group_id.to_string())
            .or_insert_with(|| GroupState {
                topic: topic.to_string(),
                generation: 0,
                version: 0,
                partitions: Vec::new(),
                members: HashMap::new(),
                committed_offsets: HashMap::new(),
            });

        if group.topic != topic {
            return Err(GroupError::TopicMismatch);
        }

        let mut state_changed =
            cleanup_group(group, &desired_partitions, now, self.inner.session_timeout);
        let mut needs_rebalance = state_changed;

        let member_id = if let Some(existing) = member_id {
            validate_id(existing, "member_id")?;
            if let Some(member) = group.members.get_mut(existing) {
                member.last_heartbeat_ms = now;
                state_changed = true;
                existing.to_string()
            } else {
                let next = self.inner.next_member_id.fetch_add(1, Ordering::Relaxed);
                let generated = format!("member-{}-{next}", self.inner.node_id);
                let effective_id = if existing.trim().is_empty() {
                    generated
                } else {
                    existing.to_string()
                };
                group.members.insert(
                    effective_id.clone(),
                    MemberState {
                        join_seq: next,
                        last_heartbeat_ms: now,
                        partitions: Vec::new(),
                    },
                );
                state_changed = true;
                needs_rebalance = true;
                effective_id
            }
        } else {
            let next = self.inner.next_member_id.fetch_add(1, Ordering::Relaxed);
            let generated = format!("member-{}-{next}", self.inner.node_id);
            group.members.insert(
                generated.clone(),
                MemberState {
                    join_seq: next,
                    last_heartbeat_ms: now,
                    partitions: Vec::new(),
                },
            );
            state_changed = true;
            needs_rebalance = true;
            generated
        };

        if needs_rebalance {
            rebalance_group(group);
        }

        let entry = if state_changed {
            bump_group_version(group);
            Some(state_record(group_id, group))
        } else {
            None
        };

        let assignments = assignments_for(group, &member_id);
        let outcome = JoinOutcome {
            group_id: group_id.to_string(),
            member_id,
            generation: group.generation,
            heartbeat_interval_ms: self.inner.heartbeat_interval.as_millis() as u64,
            session_timeout_ms: self.inner.session_timeout.as_millis() as u64,
            assignments,
        };
        drop(groups);
        self.persist_log_entry(entry)?;
        Ok(outcome)
    }

    pub fn heartbeat(
        &self,
        group_id: &str,
        topic: &str,
        member_id: &str,
        generation: u64,
        partitions: &[u32],
    ) -> Result<HeartbeatOutcome, GroupError> {
        validate_id(group_id, "group_id")?;
        validate_id(topic, "topic")?;
        validate_id(member_id, "member_id")?;

        let mut groups = self.inner.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return Err(GroupError::UnknownGroup);
        };
        if group.topic != topic {
            return Err(GroupError::TopicMismatch);
        }

        let desired_partitions = normalize_partitions(partitions);
        let now = now_millis();
        let mut state_changed = false;
        let mut needs_rebalance = false;
        if cleanup_group(group, &desired_partitions, now, self.inner.session_timeout) {
            state_changed = true;
            needs_rebalance = true;
        }
        if needs_rebalance {
            rebalance_group(group);
        }

        if generation != group.generation {
            let entry = if state_changed {
                bump_group_version(group);
                Some(state_record(group_id, group))
            } else {
                None
            };
            let current_generation = group.generation;
            drop(groups);
            self.persist_log_entry(entry)?;
            return Err(GroupError::StaleGeneration { current_generation });
        }

        let Some(member) = group.members.get_mut(member_id) else {
            let entry = if state_changed {
                bump_group_version(group);
                Some(state_record(group_id, group))
            } else {
                None
            };
            drop(groups);
            self.persist_log_entry(entry)?;
            return Err(GroupError::UnknownMember);
        };
        member.last_heartbeat_ms = now;
        state_changed = true;

        let outcome = HeartbeatOutcome {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            generation: group.generation,
        };
        let entry = if state_changed {
            bump_group_version(group);
            Some(state_record(group_id, group))
        } else {
            None
        };
        drop(groups);
        self.persist_log_entry(entry)?;
        Ok(outcome)
    }

    pub fn commit_offset(
        &self,
        request: CommitOffsetInput<'_>,
        partitions: &[u32],
    ) -> Result<CommitOffsetOutcome, GroupError> {
        let CommitOffsetInput {
            group_id,
            topic,
            member_id,
            generation,
            partition,
            offset,
        } = request;

        validate_id(group_id, "group_id")?;
        validate_id(topic, "topic")?;
        validate_id(member_id, "member_id")?;

        let mut groups = self.inner.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return Err(GroupError::UnknownGroup);
        };
        if group.topic != topic {
            return Err(GroupError::TopicMismatch);
        }

        let desired_partitions = normalize_partitions(partitions);
        let now = now_millis();
        let mut state_changed = false;
        let mut needs_rebalance = false;
        if cleanup_group(group, &desired_partitions, now, self.inner.session_timeout) {
            state_changed = true;
            needs_rebalance = true;
        }
        if needs_rebalance {
            rebalance_group(group);
        }

        if generation != group.generation {
            let entry = if state_changed {
                bump_group_version(group);
                Some(state_record(group_id, group))
            } else {
                None
            };
            let current_generation = group.generation;
            drop(groups);
            self.persist_log_entry(entry)?;
            return Err(GroupError::StaleGeneration { current_generation });
        }

        let Some(member) = group.members.get(member_id) else {
            let entry = if state_changed {
                bump_group_version(group);
                Some(state_record(group_id, group))
            } else {
                None
            };
            drop(groups);
            self.persist_log_entry(entry)?;
            return Err(GroupError::UnknownMember);
        };

        if !member.partitions.contains(&partition) {
            let entry = if state_changed {
                bump_group_version(group);
                Some(state_record(group_id, group))
            } else {
                None
            };
            drop(groups);
            self.persist_log_entry(entry)?;
            return Err(GroupError::PartitionNotAssigned);
        }

        if let Some(current) = group.committed_offsets.get(&partition).copied() {
            if offset < current {
                let entry = if state_changed {
                    bump_group_version(group);
                    Some(state_record(group_id, group))
                } else {
                    None
                };
                drop(groups);
                self.persist_log_entry(entry)?;
                return Err(GroupError::OffsetRegression {
                    current_offset: current,
                });
            }
        }

        if group.committed_offsets.get(&partition).copied() != Some(offset) {
            group.committed_offsets.insert(partition, offset);
            state_changed = true;
        }

        let outcome = CommitOffsetOutcome {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            generation: group.generation,
            topic: topic.to_string(),
            partition,
            offset,
        };
        let entry = if state_changed {
            bump_group_version(group);
            Some(state_record(group_id, group))
        } else {
            None
        };
        drop(groups);
        self.persist_log_entry(entry)?;
        Ok(outcome)
    }

    pub fn authorize_fetch(
        &self,
        group_id: &str,
        topic: &str,
        member_id: &str,
        generation: u64,
        partition: u32,
        partitions: &[u32],
    ) -> Result<(), GroupError> {
        validate_id(group_id, "group_id")?;
        validate_id(topic, "topic")?;
        validate_id(member_id, "member_id")?;

        let mut groups = self.inner.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return Err(GroupError::UnknownGroup);
        };
        if group.topic != topic {
            return Err(GroupError::TopicMismatch);
        }

        let desired_partitions = normalize_partitions(partitions);
        let now = now_millis();
        let mut state_changed = false;
        let mut needs_rebalance = false;
        if cleanup_group(group, &desired_partitions, now, self.inner.session_timeout) {
            state_changed = true;
            needs_rebalance = true;
        }
        if needs_rebalance {
            rebalance_group(group);
        }

        let result = ensure_generation_and_owner(group, member_id, generation, partition);
        let entry = if state_changed {
            bump_group_version(group);
            Some(state_record(group_id, group))
        } else {
            None
        };
        drop(groups);
        self.persist_log_entry(entry)?;
        result
    }

    pub fn leave(
        &self,
        group_id: &str,
        topic: &str,
        member_id: &str,
        generation: u64,
        partitions: &[u32],
    ) -> Result<LeaveOutcome, GroupError> {
        validate_id(group_id, "group_id")?;
        validate_id(topic, "topic")?;
        validate_id(member_id, "member_id")?;

        let mut groups = self.inner.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return Err(GroupError::UnknownGroup);
        };
        if group.topic != topic {
            return Err(GroupError::TopicMismatch);
        }

        let desired_partitions = normalize_partitions(partitions);
        let now = now_millis();
        let mut state_changed = false;
        let mut needs_rebalance = false;
        if cleanup_group(group, &desired_partitions, now, self.inner.session_timeout) {
            state_changed = true;
            needs_rebalance = true;
        }
        if needs_rebalance {
            rebalance_group(group);
        }

        if generation != group.generation {
            let entry = if state_changed {
                bump_group_version(group);
                Some(state_record(group_id, group))
            } else {
                None
            };
            let current_generation = group.generation;
            drop(groups);
            self.persist_log_entry(entry)?;
            return Err(GroupError::StaleGeneration { current_generation });
        }

        if group.members.remove(member_id).is_none() {
            let entry = if state_changed {
                bump_group_version(group);
                Some(state_record(group_id, group))
            } else {
                None
            };
            drop(groups);
            self.persist_log_entry(entry)?;
            return Err(GroupError::UnknownMember);
        }

        rebalance_group(group);
        state_changed = true;

        let outcome = LeaveOutcome {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            generation: group.generation,
        };
        let entry = if state_changed {
            bump_group_version(group);
            Some(state_record(group_id, group))
        } else {
            None
        };
        drop(groups);
        self.persist_log_entry(entry)?;
        Ok(outcome)
    }

    pub fn export_group_state(&self, group_id: &str) -> Option<ReplicatedConsumerGroupState> {
        let groups = self.inner.groups.lock();
        groups
            .get(group_id)
            .map(|group| state_record(group_id, group))
    }

    pub fn apply_replicated_state(
        &self,
        state: ReplicatedConsumerGroupState,
    ) -> Result<bool, GroupError> {
        let mut groups = self.inner.groups.lock();
        let apply = match groups.get(&state.group_id) {
            Some(existing) => state.version > existing.version,
            None => true,
        };

        if !apply {
            return Ok(false);
        }

        let group_id = state.group_id.clone();
        groups.insert(group_id, group_from_record(&state));
        maybe_advance_member_counter(
            &self.inner.node_id,
            &self.inner.next_member_id,
            groups.values(),
        );
        drop(groups);
        self.persist_log_entry(Some(state))?;
        Ok(true)
    }

    fn persist_log_entry(
        &self,
        entry: Option<ReplicatedConsumerGroupState>,
    ) -> Result<(), GroupError> {
        let Some(entry) = entry else {
            return Ok(());
        };
        let Some(path) = self.inner.log_path.as_ref() else {
            return Ok(());
        };

        append_log_entry(path, &entry).map_err(|err| GroupError::Persistence(err.to_string()))
    }
}

fn ensure_generation_and_owner(
    group: &GroupState,
    member_id: &str,
    generation: u64,
    partition: u32,
) -> Result<(), GroupError> {
    if generation != group.generation {
        return Err(GroupError::StaleGeneration {
            current_generation: group.generation,
        });
    }

    let Some(member) = group.members.get(member_id) else {
        return Err(GroupError::UnknownMember);
    };

    if !member.partitions.contains(&partition) {
        return Err(GroupError::PartitionNotAssigned);
    }

    Ok(())
}

fn assignments_for(group: &GroupState, member_id: &str) -> Vec<ConsumerGroupAssignment> {
    let Some(member) = group.members.get(member_id) else {
        return Vec::new();
    };

    let mut assignments: Vec<ConsumerGroupAssignment> = member
        .partitions
        .iter()
        .map(|partition| ConsumerGroupAssignment {
            topic: group.topic.clone(),
            partition: *partition,
            offset: group.committed_offsets.get(partition).copied().unwrap_or(0),
            leader_hint: None,
        })
        .collect();
    assignments.sort_by_key(|assignment| assignment.partition);
    assignments
}

fn cleanup_group(
    group: &mut GroupState,
    desired_partitions: &[u32],
    now_ms: u64,
    session_timeout: Duration,
) -> bool {
    let mut changed = false;
    group.members.retain(|_, member| {
        let alive =
            now_ms.saturating_sub(member.last_heartbeat_ms) <= session_timeout.as_millis() as u64;
        if !alive {
            changed = true;
        }
        alive
    });

    if group.partitions != desired_partitions {
        group.partitions = desired_partitions.to_vec();
        changed = true;
    }

    changed
}

fn rebalance_group(group: &mut GroupState) {
    group.generation = group.generation.saturating_add(1);
    for member in group.members.values_mut() {
        member.partitions.clear();
    }

    let mut members: Vec<(String, u64)> = group
        .members
        .iter()
        .map(|(member_id, member)| (member_id.clone(), member.join_seq))
        .collect();
    members.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

    if members.is_empty() {
        return;
    }

    for (index, partition) in group.partitions.iter().enumerate() {
        let member_id = &members[index % members.len()].0;
        if let Some(member) = group.members.get_mut(member_id) {
            member.partitions.push(*partition);
        }
    }

    for member in group.members.values_mut() {
        member.partitions.sort_unstable();
    }
}

fn bump_group_version(group: &mut GroupState) {
    group.version = group.version.saturating_add(1);
}

fn state_record(group_id: &str, group: &GroupState) -> ReplicatedConsumerGroupState {
    let mut committed_offsets: Vec<(u32, Offset)> = group
        .committed_offsets
        .iter()
        .map(|(partition, offset)| (*partition, *offset))
        .collect();
    committed_offsets.sort_by_key(|(partition, _)| *partition);

    let mut members: Vec<ConsumerGroupMemberState> = group
        .members
        .iter()
        .map(|(member_id, member)| ConsumerGroupMemberState {
            member_id: member_id.clone(),
            join_seq: member.join_seq,
            last_heartbeat_ms: member.last_heartbeat_ms,
            partitions: member.partitions.clone(),
        })
        .collect();
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));

    ReplicatedConsumerGroupState {
        group_id: group_id.to_string(),
        topic: group.topic.clone(),
        version: group.version,
        generation: group.generation,
        partitions: group.partitions.clone(),
        committed_offsets,
        members,
    }
}

fn group_from_record(record: &ReplicatedConsumerGroupState) -> GroupState {
    let committed_offsets = record.committed_offsets.iter().copied().collect();
    let members = record
        .members
        .iter()
        .map(|member| {
            (
                member.member_id.clone(),
                MemberState {
                    join_seq: member.join_seq,
                    last_heartbeat_ms: member.last_heartbeat_ms,
                    partitions: member.partitions.clone(),
                },
            )
        })
        .collect();

    GroupState {
        topic: record.topic.clone(),
        generation: record.generation,
        version: record.version,
        partitions: normalize_partitions(&record.partitions),
        members,
        committed_offsets,
    }
}

fn normalize_partitions(partitions: &[u32]) -> Vec<u32> {
    let mut unique = BTreeSet::new();
    for partition in partitions {
        unique.insert(*partition);
    }
    unique.into_iter().collect()
}

fn validate_id(value: &str, field: &'static str) -> Result<(), GroupError> {
    if value.trim().is_empty() {
        return Err(GroupError::InvalidRequest(field));
    }
    Ok(())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn load_log(path: &Path, session_timeout_ms: u64) -> anyhow::Result<HashMap<String, GroupState>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut groups: HashMap<String, GroupState> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let record: ReplicatedConsumerGroupState = serde_json::from_str(trimmed)?;
        let apply = match groups.get(&record.group_id) {
            Some(existing) => record.version > existing.version,
            None => true,
        };
        if apply {
            groups.insert(record.group_id.clone(), group_from_record(&record));
        }
    }

    let now = now_millis();
    for group in groups.values_mut() {
        let desired_partitions = normalize_partitions(&group.partitions);
        if cleanup_group(
            group,
            &desired_partitions,
            now,
            Duration::from_millis(session_timeout_ms.max(1)),
        ) {
            rebalance_group(group);
            bump_group_version(group);
        }
    }

    Ok(groups)
}

fn append_log_entry(path: &Path, entry: &ReplicatedConsumerGroupState) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, entry)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

fn compute_next_member_id(groups: &HashMap<String, GroupState>, node_id: &str) -> u64 {
    let mut max_seen = 0u64;
    for group in groups.values() {
        for member_id in group.members.keys() {
            if let Some(seq) = parse_member_sequence(member_id, node_id) {
                max_seen = max_seen.max(seq);
            }
        }
    }
    max_seen.saturating_add(1).max(1)
}

fn maybe_advance_member_counter<'a>(
    node_id: &str,
    next_member_id: &AtomicU64,
    groups: impl Iterator<Item = &'a GroupState>,
) {
    let mut max_seen = 0u64;
    for group in groups {
        for member_id in group.members.keys() {
            if let Some(seq) = parse_member_sequence(member_id, node_id) {
                max_seen = max_seen.max(seq);
            }
        }
    }

    let desired = max_seen.saturating_add(1).max(1);
    let mut current = next_member_id.load(Ordering::Relaxed);
    while desired > current {
        match next_member_id.compare_exchange(
            current,
            desired,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn parse_member_sequence(member_id: &str, node_id: &str) -> Option<u64> {
    let prefix = format!("member-{node_id}-");
    member_id
        .strip_prefix(&prefix)
        .and_then(|suffix| suffix.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_partition_has_single_owner() {
        let coordinator = ConsumerGroupCoordinator::new(100, 500);
        let first = coordinator.join("workers", "jobs", None, &[0]).unwrap();
        let second = coordinator.join("workers", "jobs", None, &[0]).unwrap();

        let first = coordinator
            .join("workers", "jobs", Some(&first.member_id), &[0])
            .unwrap();

        assert_eq!(
            vec![0],
            first
                .assignments
                .iter()
                .map(|assignment| assignment.partition)
                .collect::<Vec<_>>()
        );
        assert!(second.assignments.is_empty());

        assert!(coordinator
            .authorize_fetch(
                "workers",
                "jobs",
                &first.member_id,
                first.generation,
                0,
                &[0]
            )
            .is_ok());
        assert_eq!(
            Err(GroupError::PartitionNotAssigned),
            coordinator.authorize_fetch(
                "workers",
                "jobs",
                &second.member_id,
                second.generation,
                0,
                &[0],
            )
        );
    }

    #[test]
    fn heartbeats_and_rebalance_evict_stale_members() {
        let coordinator = ConsumerGroupCoordinator::new(5, 10);
        let first = coordinator.join("workers", "jobs", None, &[0, 1]).unwrap();
        let second = coordinator.join("workers", "jobs", None, &[0, 1]).unwrap();

        std::thread::sleep(Duration::from_millis(15));

        let rejoined = coordinator
            .join("workers", "jobs", Some(&second.member_id), &[0, 1])
            .unwrap();

        assert!(rejoined.generation > second.generation);
        assert_eq!(
            vec![0, 1],
            rejoined
                .assignments
                .iter()
                .map(|assignment| assignment.partition)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            Err(GroupError::StaleGeneration {
                current_generation: rejoined.generation,
            }),
            coordinator.heartbeat(
                "workers",
                "jobs",
                &first.member_id,
                first.generation,
                &[0, 1],
            )
        );
    }

    #[test]
    fn commit_offsets_are_monotonic() {
        let coordinator = ConsumerGroupCoordinator::new(100, 500);
        let joined = coordinator.join("workers", "jobs", None, &[0]).unwrap();

        let committed = coordinator
            .commit_offset(
                CommitOffsetInput {
                    group_id: "workers",
                    topic: "jobs",
                    member_id: &joined.member_id,
                    generation: joined.generation,
                    partition: 0,
                    offset: 10,
                },
                &[0],
            )
            .unwrap();
        assert_eq!(10, committed.offset);

        assert_eq!(
            Err(GroupError::OffsetRegression { current_offset: 10 }),
            coordinator.commit_offset(
                CommitOffsetInput {
                    group_id: "workers",
                    topic: "jobs",
                    member_id: &joined.member_id,
                    generation: joined.generation,
                    partition: 0,
                    offset: 9,
                },
                &[0],
            )
        );

        let refreshed = coordinator
            .join("workers", "jobs", Some(&joined.member_id), &[0])
            .unwrap();
        assert_eq!(10, refreshed.assignments[0].offset);
    }

    #[test]
    fn leave_group_removes_member_and_bumps_generation() {
        let coordinator = ConsumerGroupCoordinator::new(100, 500);
        let first = coordinator.join("workers", "jobs", None, &[0, 1]).unwrap();
        let second = coordinator.join("workers", "jobs", None, &[0, 1]).unwrap();
        let first = coordinator
            .join("workers", "jobs", Some(&first.member_id), &[0, 1])
            .unwrap();

        let left = coordinator
            .leave(
                "workers",
                "jobs",
                &second.member_id,
                second.generation,
                &[0, 1],
            )
            .unwrap();
        assert!(left.generation > first.generation);

        let refreshed = coordinator
            .join("workers", "jobs", Some(&first.member_id), &[0, 1])
            .unwrap();
        assert_eq!(
            vec![0, 1],
            refreshed
                .assignments
                .iter()
                .map(|assignment| assignment.partition)
                .collect::<Vec<_>>()
        );
    }
}
