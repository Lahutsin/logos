use serde::{Deserialize, Serialize};

pub type Offset = u64;
pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition: u32,
    pub records: Vec<Record>,
    #[serde(default)]
    pub auth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicateRequest {
    pub leader_id: String,
    pub leader_epoch: u64,
    pub topic: String,
    pub partition: u32,
    pub entries: Vec<ReplicaRecord>,
    #[serde(default)]
    pub auth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaRecord {
    pub offset: Offset,
    pub record: Record,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchRequest {
    pub topic: String,
    pub partition: u32,
    pub offset: Offset,
    pub max_bytes: u32,
    #[serde(default)]
    pub auth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub topic: String,
    #[serde(default)]
    pub member_id: Option<String>,
    #[serde(default)]
    pub auth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub topic: String,
    pub member_id: String,
    pub generation: u64,
    #[serde(default)]
    pub auth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitOffsetRequest {
    pub group_id: String,
    pub topic: String,
    pub member_id: String,
    pub generation: u64,
    pub partition: u32,
    pub offset: Offset,
    #[serde(default)]
    pub auth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupFetchRequest {
    pub group_id: String,
    pub topic: String,
    pub member_id: String,
    pub generation: u64,
    pub partition: u32,
    pub offset: Offset,
    pub max_bytes: u32,
    #[serde(default)]
    pub auth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub topic: String,
    pub member_id: String,
    pub generation: u64,
    #[serde(default)]
    pub auth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidateGroupFetchRequest {
    pub group_id: String,
    pub topic: String,
    pub member_id: String,
    pub generation: u64,
    pub partition: u32,
    #[serde(default)]
    pub auth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsumerGroupAssignment {
    pub topic: String,
    pub partition: u32,
    pub offset: Offset,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Request {
    Produce(ProduceRequest),
    Replicate(ReplicateRequest),
    Fetch(FetchRequest),
    Health,
    Handshake { client_version: u16 },
    JoinGroup(JoinGroupRequest),
    Heartbeat(HeartbeatRequest),
    CommitOffset(CommitOffsetRequest),
    GroupFetch(GroupFetchRequest),
    LeaveGroup(LeaveGroupRequest),
    ValidateGroupFetch(ValidateGroupFetchRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchedRecord {
    pub offset: Offset,
    pub record: Record,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Response {
    Produced {
        base_offset: Offset,
        last_offset: Offset,
        acks: u32,
    },
    Fetched {
        records: Vec<FetchedRecord>,
    },
    HealthOk,
    HandshakeOk {
        server_version: u16,
    },
    NotLeader {
        leader: Option<String>,
    },
    Error(String),
    GroupJoined {
        group_id: String,
        member_id: String,
        generation: u64,
        heartbeat_interval_ms: u64,
        session_timeout_ms: u64,
        assignments: Vec<ConsumerGroupAssignment>,
    },
    HeartbeatOk {
        group_id: String,
        member_id: String,
        generation: u64,
    },
    OffsetCommitted {
        group_id: String,
        member_id: String,
        generation: u64,
        topic: String,
        partition: u32,
        offset: Offset,
    },
    RebalanceRequired {
        group_id: String,
        generation: u64,
    },
    GroupLeft {
        group_id: String,
        member_id: String,
        generation: u64,
    },
    GroupFetchAuthorized {
        group_id: String,
        member_id: String,
        generation: u64,
        topic: String,
        partition: u32,
    },
}

pub fn encode<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> anyhow::Result<T> {
    Ok(bincode::deserialize(bytes)?)
}
