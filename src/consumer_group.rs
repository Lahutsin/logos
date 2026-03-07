use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::protocol::{ConsumerGroupAssignment, Offset};

#[derive(Clone)]
pub struct ConsumerGroupCoordinator {
    inner: Arc<CoordinatorInner>,
}

struct CoordinatorInner {
    groups: Mutex<HashMap<String, GroupState>>,
    next_member_id: AtomicU64,
    heartbeat_interval: Duration,
    session_timeout: Duration,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveOutcome {
    pub group_id: String,
    pub member_id: String,
    pub generation: u64,
}

#[derive(Debug)]
struct GroupState {
    topic: String,
    generation: u64,
    partitions: Vec<u32>,
    members: HashMap<String, MemberState>,
    committed_offsets: HashMap<u32, Offset>,
}

#[derive(Debug)]
struct MemberState {
    join_seq: u64,
    last_heartbeat: Instant,
    partitions: Vec<u32>,
}

impl ConsumerGroupCoordinator {
    pub fn new(heartbeat_interval_ms: u64, session_timeout_ms: u64) -> Self {
        let heartbeat_interval = Duration::from_millis(heartbeat_interval_ms.max(1));
        let session_timeout = Duration::from_millis(session_timeout_ms.max(heartbeat_interval_ms.max(1)));
        Self {
            inner: Arc::new(CoordinatorInner {
                groups: Mutex::new(HashMap::new()),
                next_member_id: AtomicU64::new(1),
                heartbeat_interval,
                session_timeout,
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
        let now = Instant::now();
        let group = groups.entry(group_id.to_string()).or_insert_with(|| GroupState {
            topic: topic.to_string(),
            generation: 0,
            partitions: Vec::new(),
            members: HashMap::new(),
            committed_offsets: HashMap::new(),
        });

        if group.topic != topic {
            return Err(GroupError::TopicMismatch);
        }

        let mut needs_rebalance = cleanup_group(group, &desired_partitions, now, self.inner.session_timeout);
        let member_id = if let Some(existing) = member_id {
            validate_id(existing, "member_id")?;
            if let Some(member) = group.members.get_mut(existing) {
                member.last_heartbeat = now;
                existing.to_string()
            } else {
                group.members.insert(
                    existing.to_string(),
                    MemberState {
                        join_seq: self.inner.next_member_id.fetch_add(1, Ordering::Relaxed),
                        last_heartbeat: now,
                        partitions: Vec::new(),
                    },
                );
                needs_rebalance = true;
                existing.to_string()
            }
        } else {
            let next = self.inner.next_member_id.fetch_add(1, Ordering::Relaxed);
            let member_id = format!("member-{next}");
            group.members.insert(
                member_id.clone(),
                MemberState {
                    join_seq: next,
                    last_heartbeat: now,
                    partitions: Vec::new(),
                },
            );
            needs_rebalance = true;
            member_id
        };

        if needs_rebalance {
            rebalance_group(group);
        }

        let assignments = assignments_for(group, &member_id);
        Ok(JoinOutcome {
            group_id: group_id.to_string(),
            member_id,
            generation: group.generation,
            heartbeat_interval_ms: self.inner.heartbeat_interval.as_millis() as u64,
            session_timeout_ms: self.inner.session_timeout.as_millis() as u64,
            assignments,
        })
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
        let now = Instant::now();
        if cleanup_group(group, &desired_partitions, now, self.inner.session_timeout) {
            rebalance_group(group);
        }

        if generation != group.generation {
            return Err(GroupError::StaleGeneration {
                current_generation: group.generation,
            });
        }

        let Some(member) = group.members.get_mut(member_id) else {
            return Err(GroupError::UnknownMember);
        };
        member.last_heartbeat = now;

        Ok(HeartbeatOutcome {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            generation: group.generation,
        })
    }

    pub fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        member_id: &str,
        generation: u64,
        partition: u32,
        offset: Offset,
        partitions: &[u32],
    ) -> Result<CommitOffsetOutcome, GroupError> {
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
        let now = Instant::now();
        if cleanup_group(group, &desired_partitions, now, self.inner.session_timeout) {
            rebalance_group(group);
        }

        ensure_generation_and_owner(group, member_id, generation, partition)?;

        if let Some(current) = group.committed_offsets.get(&partition).copied() {
            if offset < current {
                return Err(GroupError::OffsetRegression {
                    current_offset: current,
                });
            }
        }

        group.committed_offsets.insert(partition, offset);
        Ok(CommitOffsetOutcome {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            generation: group.generation,
            topic: topic.to_string(),
            partition,
            offset,
        })
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
        let now = Instant::now();
        if cleanup_group(group, &desired_partitions, now, self.inner.session_timeout) {
            rebalance_group(group);
        }

        ensure_generation_and_owner(group, member_id, generation, partition)
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
        let now = Instant::now();
        if cleanup_group(group, &desired_partitions, now, self.inner.session_timeout) {
            rebalance_group(group);
        }

        if generation != group.generation {
            return Err(GroupError::StaleGeneration {
                current_generation: group.generation,
            });
        }

        if group.members.remove(member_id).is_none() {
            return Err(GroupError::UnknownMember);
        }

        rebalance_group(group);

        Ok(LeaveOutcome {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            generation: group.generation,
        })
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
        })
        .collect();
    assignments.sort_by_key(|assignment| assignment.partition);
    assignments
}

fn cleanup_group(
    group: &mut GroupState,
    desired_partitions: &[u32],
    now: Instant,
    session_timeout: Duration,
) -> bool {
    let mut changed = false;
    group.members.retain(|_, member| {
        let alive = now.duration_since(member.last_heartbeat) <= session_timeout;
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

        assert_eq!(vec![0], first.assignments.iter().map(|a| a.partition).collect::<Vec<_>>());
        assert!(second.assignments.is_empty());

        assert!(coordinator
            .authorize_fetch("workers", "jobs", &first.member_id, first.generation, 0, &[0])
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
                "workers",
                "jobs",
                &joined.member_id,
                joined.generation,
                0,
                10,
                &[0],
            )
            .unwrap();
        assert_eq!(10, committed.offset);

        assert_eq!(
            Err(GroupError::OffsetRegression { current_offset: 10 }),
            coordinator.commit_offset(
                "workers",
                "jobs",
                &joined.member_id,
                joined.generation,
                0,
                9,
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