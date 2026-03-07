use std::env;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: String,
    pub data_dir: PathBuf,
    pub segment_bytes: u64,
    pub max_connections: usize,
    pub retention_bytes: Option<u64>,
    pub retention_segments: Option<usize>,
    pub index_stride: usize,
    pub node_id: String,
    pub metadata_path: Option<PathBuf>,
    pub metadata_refresh_ms: u64,
    pub auth_path: Option<PathBuf>,
    pub replication_ack_quorum: usize,
    pub replication_timeout_ms: u64,
    pub replication_retries: usize,
    pub replication_backoff_ms: u64,
    pub consumer_group_heartbeat_ms: u64,
    pub consumer_group_session_timeout_ms: u64,
    pub replication_auth_token: Option<String>,
    pub admin_bind: String,
    pub admin_token: Option<String>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_ca: Option<PathBuf>,
    pub tls_client_ca: Option<PathBuf>,
    pub tls_domain: Option<String>,
    pub fsync: bool,
    pub max_frame_bytes: usize,
    pub max_batch_bytes: u64,
    pub require_tls: bool,
    pub require_auth: bool,
}

impl Config {
    pub fn from_env() -> Self {
        let bind_addr = env::var("RK_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:9092".to_string());
        let data_dir = env::var("RK_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data"));
        let segment_bytes = env::var("RK_SEGMENT_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(128 * 1024 * 1024);
        let max_connections = env::var("RK_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1024);

        let retention_bytes = env::var("RK_RETENTION_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());
        let retention_segments = env::var("RK_RETENTION_SEGMENTS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok());

        let index_stride = env::var("RK_INDEX_STRIDE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(16);

        let node_id = env::var("RK_NODE_ID").unwrap_or_else(|_| "node-1".to_string());
        let metadata_path = env::var("RK_METADATA_PATH").ok().map(PathBuf::from);

        let metadata_refresh_ms = env::var("RK_METADATA_REFRESH_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1_000);

        let auth_path = env::var("RK_AUTH_PATH").ok().map(PathBuf::from);

        let replication_ack_quorum = env::var("RK_REPLICATION_ACKS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(1);

        let replication_timeout_ms = env::var("RK_REPLICATION_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1000);

        let replication_retries = env::var("RK_REPLICATION_RETRIES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2);

        let replication_backoff_ms = env::var("RK_REPLICATION_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(200);

        let consumer_group_heartbeat_ms = env::var("RK_CONSUMER_GROUP_HEARTBEAT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3_000);

        let consumer_group_session_timeout_ms = env::var("RK_CONSUMER_GROUP_SESSION_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(15_000);

        let replication_auth_token = env::var("RK_REPLICATION_TOKEN").ok();

        let admin_bind = env::var("RK_ADMIN_ADDR").unwrap_or_else(|_| "127.0.0.1:9100".to_string());
        let admin_token = env::var("RK_ADMIN_TOKEN")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        let tls_cert = env::var("RK_TLS_CERT").ok().map(PathBuf::from);
        let tls_key = env::var("RK_TLS_KEY").ok().map(PathBuf::from);
        let tls_ca = env::var("RK_TLS_CA").ok().map(PathBuf::from);
        let tls_client_ca = env::var("RK_TLS_CLIENT_CA").ok().map(PathBuf::from);
        let tls_domain = env::var("RK_TLS_DOMAIN").ok();

        let fsync = env::var("RK_FSYNC")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        let max_frame_bytes = env::var("RK_MAX_FRAME_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4 * 1024 * 1024);

        let max_batch_bytes = env::var("RK_MAX_BATCH_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(4 * 1024 * 1024);

        let require_tls = env::var("RK_REQUIRE_TLS")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);

        let require_auth = env::var("RK_REQUIRE_AUTH")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        Self {
            bind_addr,
            data_dir,
            segment_bytes,
            max_connections,
            retention_bytes,
            retention_segments,
            index_stride,
            node_id,
            metadata_path,
            metadata_refresh_ms,
            auth_path,
            replication_ack_quorum,
            replication_timeout_ms,
            replication_retries,
            replication_backoff_ms,
            consumer_group_heartbeat_ms,
            consumer_group_session_timeout_ms,
            replication_auth_token,
            admin_bind,
            admin_token,
            tls_cert,
            tls_key,
            tls_ca,
            tls_client_ca,
            tls_domain,
            fsync,
            max_frame_bytes,
            max_batch_bytes,
            require_tls,
            require_auth,
        }
    }
}
