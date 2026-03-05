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
pub enum Request {
    Produce(ProduceRequest),
    Replicate(ReplicateRequest),
    Fetch(FetchRequest),
    Health,
    Handshake { client_version: u16 },
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
}

pub fn encode<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> anyhow::Result<T> {
    Ok(bincode::deserialize(bytes)?)
}
