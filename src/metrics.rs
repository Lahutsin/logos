use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts, Registry, TextEncoder};

#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    pub requests: IntCounterVec,
    pub bytes: IntCounterVec,
}

pub static METRICS: Lazy<Metrics> = Lazy::new(|| {
    let registry = Registry::new();

    let requests = IntCounterVec::new(
        Opts::new("rk_requests_total", "Total requests by type and status"),
        &["type", "status"],
    )
    .expect("requests counter");
    registry
        .register(Box::new(requests.clone()))
        .expect("register requests");

    let bytes = IntCounterVec::new(
        Opts::new("rk_bytes_total", "Bytes by direction"),
        &["direction"],
    )
    .expect("bytes counter");
    registry
        .register(Box::new(bytes.clone()))
        .expect("register bytes");

    Metrics {
        registry,
        requests,
        bytes,
    }
});

pub fn inc_request(kind: &str, status: &str) {
    METRICS.requests.with_label_values(&[kind, status]).inc();
}

pub fn add_bytes(direction: &str, val: u64) {
    METRICS.bytes
        .with_label_values(&[direction])
    .inc_by(val);
}

pub fn render_prometheus() -> String {
    let encoder = TextEncoder::new();
    let metric_families = METRICS.registry.gather();
    let mut buf = String::new();
    encoder.encode_utf8(&metric_families, &mut buf).expect("encode metrics");
    buf
}
