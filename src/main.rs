use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{self, Duration};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::get,
    routing::post,
    Json, Router,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use logos::broker::{Broker, BrokerConfig};
use logos::config::Config;
use logos::metadata::Metadata;
use logos::metrics::render_prometheus;
use logos::protocol::{self, Request, Response};
use logos::replication::Replicator;
use logos::security::{build_tls_endpoints, Authz, TlsOptions};
use logos::storage::Storage;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::Semaphore;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;

    let config = Config::from_env();
    info!(?config, "starting logos");

    let admin_addr: SocketAddr = config
        .admin_bind
        .parse()
        .map_err(|err| anyhow::anyhow!("invalid RK_ADMIN_ADDR '{}': {err}", config.admin_bind))?;

    if !admin_addr.ip().is_loopback() && config.admin_token.is_none() {
        anyhow::bail!("RK_ADMIN_TOKEN is required when RK_ADMIN_ADDR is not loopback");
    }

    let metadata = Arc::new(Metadata::load(
        config.metadata_path.as_deref(),
        config.node_id.clone(),
    )?);

    let storage = Storage::open(
        &config.data_dir,
        config.segment_bytes,
        config.retention_bytes,
        config.retention_segments,
        config.index_stride,
        config.fsync,
    )?;

    let authz = Authz::load(config.auth_path.as_deref())?;

    if config.require_auth && !authz.enabled() {
        anyhow::bail!("require_auth=true but RK_AUTH_PATH is not configured");
    }

    let tls = build_tls_endpoints(&TlsOptions {
        cert: config.tls_cert.clone(),
        key: config.tls_key.clone(),
        ca_cert: config.tls_ca.clone(),
        client_ca_cert: config.tls_client_ca.clone(),
        domain: config.tls_domain.clone(),
    })?;

    if config.require_tls {
        if tls.acceptor.is_none() || tls.connector.is_none() {
            anyhow::bail!(
                "require_tls=true but TLS acceptor/connector not fully configured (cert/key/ca)"
            );
        }
        if config
            .tls_domain
            .as_deref()
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .is_none()
        {
            anyhow::bail!("require_tls=true but RK_TLS_DOMAIN is missing or empty");
        }
        if tls.server_name.is_none() {
            anyhow::bail!("require_tls=true but RK_TLS_DOMAIN is invalid for TLS SNI/server name");
        }
    }

    if config.require_auth && config.replication_auth_token.is_none() {
        anyhow::bail!(
            "require_auth=true but RK_REPLICATION_TOKEN is missing for inter-node replication"
        );
    }

    if metadata.has_source() {
        let md = metadata.clone();
        let interval_ms = config.metadata_refresh_ms;
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(interval_ms));
            loop {
                ticker.tick().await;
                if let Err(err) = md.refresh() {
                    warn!(%err, "metadata refresh failed");
                }
            }
        });
    }

    let replicator = Replicator::new(
        metadata.clone(),
        config.replication_timeout_ms,
        config.replication_retries,
        config.replication_backoff_ms,
        tls.clone(),
        config.replication_auth_token.clone(),
    );

    let broker = Broker::new(
        storage.clone(),
        replicator,
        metadata,
        BrokerConfig {
            ack_quorum: config.replication_ack_quorum,
            authz,
            max_batch_bytes: config.max_batch_bytes,
            consumer_group_heartbeat_ms: config.consumer_group_heartbeat_ms,
            consumer_group_session_timeout_ms: config.consumer_group_session_timeout_ms,
        },
    )?;

    run_server(config, broker, tls, storage, admin_addr).await
}

fn init_tracing() -> anyhow::Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .init();
    Ok(())
}

async fn run_server(
    config: Config,
    broker: Broker,
    tls: logos::security::TlsEndpoints,
    storage: Storage,
    admin_addr: SocketAddr,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&config.bind_addr).await?;
    let limiter = Arc::new(Semaphore::new(config.max_connections));

    // Admin/metrics server
    let storage_clone = storage.clone();
    let admin_token = config.admin_token.clone();
    tokio::spawn(async move {
        let router = Router::new()
            .route("/healthz", get(health_handler))
            .route("/metrics", get(metrics_handler))
            .route("/admin/compact", post(compact_handler))
            .route("/admin/retention", post(retention_handler))
            .with_state(AdminState {
                storage: storage_clone,
                token: admin_token,
            });
        if let Err(err) = axum::Server::bind(&admin_addr)
            .serve(router.into_make_service())
            .await
        {
            warn!(%err, "admin server failed");
        }
    });

    info!(addr = %config.bind_addr, "listening");

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, _peer) = match accept_result {
                    Ok(pair) => pair,
                    Err(err) => {
                        warn!(%err, "accept failed");
                        continue;
                    }
                };

                let permit = match limiter.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(err) => {
                        warn!(%err, "connection semaphore poisoned");
                        continue;
                    }
                };

                let broker_clone = broker.clone();
                let max_frame_bytes = config.max_frame_bytes;
                if let Some(acceptor) = tls.acceptor.clone() {
                    tokio::spawn(async move {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                let result = handle_connection(tls_stream, broker_clone, max_frame_bytes).await;
                                if let Err(err) = result {
                                    warn!(%err, "connection closed with error");
                                }
                            }
                            Err(err) => {
                                warn!(%err, "tls accept failed");
                            }
                        }
                        drop(permit);
                    });
                } else {
                    tokio::spawn(async move {
                        let result = handle_connection(stream, broker_clone, max_frame_bytes).await;
                        if let Err(err) = result {
                            warn!(%err, "connection closed with error");
                        }
                        drop(permit);
                    });
                }
            }
            _ = signal::ctrl_c() => {
                info!("shutdown requested");
                break;
            }
        }
    }

    Ok(())
}

async fn handle_connection<S>(
    stream: S,
    broker: Broker,
    max_frame_bytes: usize,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    info!("accepted connection");

    let codec = LengthDelimitedCodec::builder()
        .length_field_length(4)
        .max_frame_length(max_frame_bytes)
        .new_codec();

    let mut framed = Framed::new(stream, codec);
    let mut handshake_done = false;

    while let Some(frame) = framed.next().await {
        let bytes = match frame {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(%err, "frame error");
                break;
            }
        };

        let request: Request = match protocol::decode(&bytes) {
            Ok(req) => req,
            Err(err) => {
                warn!(%err, "failed to decode request");
                let response = Response::Error("invalid request frame".to_string());
                if let Ok(encoded) = protocol::encode(&response) {
                    let _ = framed.send(Bytes::from(encoded)).await;
                }
                break;
            }
        };

        if !handshake_done {
            match request {
                Request::Handshake { client_version } => {
                    if client_version != protocol::PROTOCOL_VERSION {
                        let response = Response::Error(format!(
                            "protocol version mismatch: client {} server {}",
                            client_version,
                            protocol::PROTOCOL_VERSION
                        ));
                        let encoded = protocol::encode(&response)?;
                        framed.send(Bytes::from(encoded)).await?;
                        break;
                    }

                    handshake_done = true;
                    let response = Response::HandshakeOk {
                        server_version: protocol::PROTOCOL_VERSION,
                    };
                    let encoded = protocol::encode(&response)?;
                    framed.send(Bytes::from(encoded)).await?;
                    continue;
                }
                _ => {
                    let response = Response::Error("handshake required".to_string());
                    let encoded = protocol::encode(&response)?;
                    framed.send(Bytes::from(encoded)).await?;
                    break;
                }
            }
        }

        if matches!(request, Request::Handshake { .. }) {
            let response = Response::Error("handshake already completed".to_string());
            let encoded = protocol::encode(&response)?;
            framed.send(Bytes::from(encoded)).await?;
            continue;
        }

        let response = broker.handle(request).await;
        let encoded = protocol::encode(&response)?;
        framed.send(Bytes::from(encoded)).await?;
    }

    info!("connection closed");
    Ok(())
}

#[derive(Clone)]
struct AdminState {
    storage: Storage,
    token: Option<String>,
}

fn authorize_admin(
    expected: &Option<String>,
    headers: &axum::http::HeaderMap,
) -> Result<(), StatusCode> {
    let required = expected
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim());
    let expected_header = format!("Bearer {required}");
    if provided != Some(expected_header.as_str()) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(())
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn metrics_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<String, StatusCode> {
    authorize_admin(&state.token, &headers)?;
    Ok(render_prometheus())
}

#[derive(Deserialize)]
struct CompactReq {
    topic: String,
    partition: u32,
}

async fn compact_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(req): Json<CompactReq>,
) -> Result<String, StatusCode> {
    authorize_admin(&state.token, &headers)?;
    state
        .storage
        .compact_async(&req.topic, req.partition)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok("ok".to_string())
}

async fn retention_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<String, StatusCode> {
    authorize_admin(&state.token, &headers)?;
    state
        .storage
        .run_retention_async()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok("ok".to_string())
}
