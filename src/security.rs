use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio_rustls::rustls::{self, Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig, ServerName};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::Deserialize;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct TlsOptions {
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub ca_cert: Option<PathBuf>,
    pub client_ca_cert: Option<PathBuf>,
    pub domain: Option<String>,
}

#[derive(Clone)]
pub struct TlsEndpoints {
    pub acceptor: Option<tokio_rustls::TlsAcceptor>,
    pub connector: Option<tokio_rustls::TlsConnector>,
    pub server_name: Option<ServerName>,
}

impl TlsEndpoints {
    pub fn is_enabled(&self) -> bool {
        self.acceptor.is_some() || self.connector.is_some()
    }
}

pub fn build_tls_endpoints(opts: &TlsOptions) -> anyhow::Result<TlsEndpoints> {
    let (acceptor, server_cert, server_key) = if let (Some(cert), Some(key)) = (opts.cert.as_ref(), opts.key.as_ref()) {
        let certs = load_certs(cert)?;
        let key = load_key(key)?;
        let mut server = if let Some(ca) = opts.client_ca_cert.as_ref() {
            let mut roots = RootCertStore::empty();
            for c in load_certs(ca)? {
                roots.add(&c)?;
            }
            let verifier = rustls::server::AllowAnyAuthenticatedClient::new(roots);
            ServerConfig::builder()
                .with_safe_defaults()
                .with_client_cert_verifier(Arc::new(verifier))
                .with_single_cert(certs.clone(), key.clone())?
        } else {
            ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(certs.clone(), key.clone())?
        };
        server.alpn_protocols.push(b"rk".to_vec());
        (Some(tokio_rustls::TlsAcceptor::from(Arc::new(server))), Some(certs), Some(key))
    } else {
        (None, None, None)
    };

    let connector = if let Some(ca_path) = opts.ca_cert.as_ref() {
        let mut roots = RootCertStore::empty();
        for cert in load_certs(ca_path)? {
            roots.add(&cert)?;
        }
        let cfg = if let (Some(certs), Some(key)) = (server_cert, server_key) {
            ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(roots)
                .with_client_auth_cert(certs, key)?
        } else {
            ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(roots)
                .with_no_client_auth()
        };
        Some(tokio_rustls::TlsConnector::from(Arc::new(cfg)))
    } else {
        None
    };

    let server_name = opts
        .domain
        .as_ref()
        .and_then(|d| ServerName::try_from(d.as_str()).ok());

    Ok(TlsEndpoints {
        acceptor,
        connector,
        server_name,
    })
}

fn load_certs(path: &Path) -> anyhow::Result<Vec<Certificate>> {
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let certs = certs(&mut reader)
        .map_err(|_| anyhow::anyhow!("invalid cert"))?
        .into_iter()
        .map(Certificate)
        .collect();
    Ok(certs)
}

fn load_key(path: &Path) -> anyhow::Result<PrivateKey> {
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    if let Some(key) = pkcs8_private_keys(&mut reader)?.into_iter().next() {
        return Ok(PrivateKey(key));
    }
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    if let Some(key) = rsa_private_keys(&mut reader)?.into_iter().next() {
        return Ok(PrivateKey(key));
    }
    Err(anyhow::anyhow!("no private key found"))
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("authn required")]
    Missing,
    #[error("invalid token")]
    Invalid,
    #[error("not authorized for topic")] 
    Forbidden,
    #[error("quota exceeded")]
    Quota,
}

#[derive(Debug, Clone, Copy)]
pub enum Action {
    Produce,
    Fetch,
    Replicate,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Principal {
    pub name: String,
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub allow_produce: bool,
    #[serde(default)]
    pub allow_fetch: bool,
    #[serde(default)]
    pub allow_replicate: bool,
    #[serde(default)]
    pub quota_bytes_per_sec: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct AuthFile(pub HashMap<String, Principal>);

#[derive(Clone)]
pub struct Authz {
    enabled: bool,
    principals: Arc<RwLock<HashMap<String, Principal>>>,
    quotas: Arc<Mutex<HashMap<String, QuotaState>>>,
}

#[derive(Debug)]
struct QuotaState {
    window_start: Instant,
    used: u64,
}

impl Authz {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        if let Some(p) = path {
            let bytes = std::fs::read(p)?;
            let parsed: AuthFile = serde_json::from_slice(&bytes)?;
            Ok(Self {
                enabled: true,
                principals: Arc::new(RwLock::new(parsed.0)),
                quotas: Arc::new(Mutex::new(HashMap::new())),
            })
        } else {
            Ok(Self {
                enabled: false,
                principals: Arc::new(RwLock::new(HashMap::new())),
                quotas: Arc::new(Mutex::new(HashMap::new())),
            })
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub async fn authorize(&self, token: Option<&str>, topic: &str, action: Action, bytes: u64) -> Result<(), AuthError> {
        if !self.enabled {
            return Ok(());
        }
        let token = token.ok_or(AuthError::Missing)?;
        let principal = {
            let guard = self.principals.read();
            guard.get(token).cloned()
        }
        .ok_or(AuthError::Invalid)?;

        if !topic_allowed(topic, &principal.topics) {
            return Err(AuthError::Forbidden);
        }

        match action {
            Action::Produce if !principal.allow_produce => return Err(AuthError::Forbidden),
            Action::Fetch if !principal.allow_fetch => return Err(AuthError::Forbidden),
            Action::Replicate if !principal.allow_replicate => return Err(AuthError::Forbidden),
            _ => {}
        }

        if let Some(limit) = principal.quota_bytes_per_sec {
            let mut guard = self.quotas.lock().await;
            let state = guard.entry(token.to_string()).or_insert(QuotaState {
                window_start: Instant::now(),
                used: 0,
            });
            let now = Instant::now();
            if now.duration_since(state.window_start) >= Duration::from_secs(1) {
                state.window_start = now;
                state.used = 0;
            }
            if state.used.saturating_add(bytes) > limit {
                return Err(AuthError::Quota);
            }
            state.used = state.used.saturating_add(bytes);
        }

        Ok(())
    }
}

fn topic_allowed(topic: &str, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return false;
    }
    allowed.iter().any(|t| t == "*" || t == topic)
}
