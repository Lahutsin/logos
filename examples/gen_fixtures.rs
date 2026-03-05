use std::fs;
use std::path::Path;

use logos::protocol::{
    self, FetchRequest, FetchedRecord, ProduceRequest, Record, Request, Response,
};

fn ensure_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn write_fixture(path: &str, bytes: &[u8]) -> std::io::Result<()> {
    let full = Path::new(path);
    ensure_dir(full)?;
    fs::write(full, bytes)
}

fn fixture_produce_request() -> Request {
    Request::Produce(ProduceRequest {
        topic: "compat".to_string(),
        partition: 0,
        records: vec![
            Record {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                timestamp: 1,
            },
            Record {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                timestamp: 2,
            },
        ],
        auth: Some("token-a".to_string()),
    })
}

fn fixture_fetch_request() -> Request {
    Request::Fetch(FetchRequest {
        topic: "compat".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 4096,
        auth: None,
    })
}

fn fixture_produced_response() -> Response {
    Response::Produced {
        base_offset: 10,
        last_offset: 11,
        acks: 2,
    }
}

fn fixture_fetched_response() -> Response {
    Response::Fetched {
        records: vec![
            FetchedRecord {
                offset: 10,
                record: Record {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    timestamp: 1,
                },
            },
            FetchedRecord {
                offset: 11,
                record: Record {
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec(),
                    timestamp: 2,
                },
            },
        ],
    }
}

fn fixture_not_leader_response() -> Response {
    Response::NotLeader {
        leader: Some("node-x".to_string()),
    }
}

fn fixture_handshake_request() -> Request {
    Request::Handshake {
        client_version: protocol::PROTOCOL_VERSION,
    }
}

fn fixture_handshake_response() -> Response {
    Response::HandshakeOk {
        server_version: protocol::PROTOCOL_VERSION,
    }
}

fn main() -> anyhow::Result<()> {
    let req_fixtures = vec![
        (
            "tests/fixtures/v1/produce_req.bin",
            fixture_produce_request(),
        ),
        ("tests/fixtures/v1/fetch_req.bin", fixture_fetch_request()),
        (
            "tests/fixtures/v1/handshake_req.bin",
            fixture_handshake_request(),
        ),
    ];

    for (path, req) in req_fixtures {
        let bytes = protocol::encode(&req)?;
        write_fixture(path, &bytes)?;
    }

    // Responses
    let resp_fixtures = vec![
        (
            "tests/fixtures/v1/produced_resp.bin",
            fixture_produced_response(),
        ),
        (
            "tests/fixtures/v1/fetched_resp.bin",
            fixture_fetched_response(),
        ),
        (
            "tests/fixtures/v1/not_leader_resp.bin",
            fixture_not_leader_response(),
        ),
        (
            "tests/fixtures/v1/handshake_resp.bin",
            fixture_handshake_response(),
        ),
    ];

    for (path, resp) in resp_fixtures {
        let bytes = protocol::encode(&resp)?;
        write_fixture(path, &bytes)?;
    }

    println!("wrote fixtures to tests/fixtures/v1");
    Ok(())
}
