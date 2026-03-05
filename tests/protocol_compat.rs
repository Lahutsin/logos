use logos::protocol::{self, FetchedRecord, Record, Request, Response};

fn fixture(path: &str) -> &'static [u8] {
    match path {
        "produce_req" => include_bytes!("fixtures/v1/produce_req.bin"),
        "fetch_req" => include_bytes!("fixtures/v1/fetch_req.bin"),
        "handshake_req" => include_bytes!("fixtures/v1/handshake_req.bin"),
        "produced_resp" => include_bytes!("fixtures/v1/produced_resp.bin"),
        "fetched_resp" => include_bytes!("fixtures/v1/fetched_resp.bin"),
        "not_leader_resp" => include_bytes!("fixtures/v1/not_leader_resp.bin"),
        "handshake_resp" => include_bytes!("fixtures/v1/handshake_resp.bin"),
        other => panic!("unknown fixture {other}"),
    }
}

fn expected_produce_request() -> Request {
    Request::Produce(protocol::ProduceRequest {
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

fn expected_fetch_request() -> Request {
    Request::Fetch(protocol::FetchRequest {
        topic: "compat".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 4096,
        auth: None,
    })
}

fn expected_produced_response() -> Response {
    Response::Produced {
        base_offset: 10,
        last_offset: 11,
        acks: 2,
    }
}

fn expected_fetched_response() -> Response {
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

fn expected_not_leader_response() -> Response {
    Response::NotLeader {
        leader: Some("node-x".to_string()),
    }
}

fn expected_handshake_request() -> Request {
    Request::Handshake {
        client_version: protocol::PROTOCOL_VERSION,
    }
}

fn expected_handshake_response() -> Response {
    Response::HandshakeOk {
        server_version: protocol::PROTOCOL_VERSION,
    }
}

#[test]
fn decode_produce_request_v1() {
    let req: Request = protocol::decode(fixture("produce_req")).expect("decode produce");
    assert_eq!(expected_produce_request(), req);
}

#[test]
fn encode_produce_request_matches_fixture() {
    let encoded = protocol::encode(&expected_produce_request()).expect("encode produce");
    assert_eq!(fixture("produce_req"), encoded.as_slice());
}

#[test]
fn decode_fetch_request_v1() {
    let req: Request = protocol::decode(fixture("fetch_req")).expect("decode fetch");
    assert_eq!(expected_fetch_request(), req);
}

#[test]
fn encode_fetch_request_matches_fixture() {
    let encoded = protocol::encode(&expected_fetch_request()).expect("encode fetch");
    assert_eq!(fixture("fetch_req"), encoded.as_slice());
}

#[test]
fn decode_produced_response_v1() {
    let resp: Response = protocol::decode(fixture("produced_resp")).expect("decode produced");
    assert_eq!(expected_produced_response(), resp);
}

#[test]
fn encode_produced_response_matches_fixture() {
    let encoded = protocol::encode(&expected_produced_response()).expect("encode produced");
    assert_eq!(fixture("produced_resp"), encoded.as_slice());
}

#[test]
fn decode_fetched_response_v1() {
    let resp: Response = protocol::decode(fixture("fetched_resp")).expect("decode fetched");
    assert_eq!(expected_fetched_response(), resp);
}

#[test]
fn encode_fetched_response_matches_fixture() {
    let encoded = protocol::encode(&expected_fetched_response()).expect("encode fetched");
    assert_eq!(fixture("fetched_resp"), encoded.as_slice());
}

#[test]
fn decode_not_leader_response_v1() {
    let resp: Response = protocol::decode(fixture("not_leader_resp")).expect("decode not leader");
    assert_eq!(expected_not_leader_response(), resp);
}

#[test]
fn encode_not_leader_response_matches_fixture() {
    let encoded = protocol::encode(&expected_not_leader_response()).expect("encode not leader");
    assert_eq!(fixture("not_leader_resp"), encoded.as_slice());
}

#[test]
fn decode_handshake_request_v1() {
    let req: Request = protocol::decode(fixture("handshake_req")).expect("decode handshake req");
    assert_eq!(expected_handshake_request(), req);
}

#[test]
fn encode_handshake_request_matches_fixture() {
    let encoded = protocol::encode(&expected_handshake_request()).expect("encode handshake req");
    assert_eq!(fixture("handshake_req"), encoded.as_slice());
}

#[test]
fn decode_handshake_response_v1() {
    let resp: Response =
        protocol::decode(fixture("handshake_resp")).expect("decode handshake resp");
    assert_eq!(expected_handshake_response(), resp);
}

#[test]
fn encode_handshake_response_matches_fixture() {
    let encoded = protocol::encode(&expected_handshake_response()).expect("encode handshake resp");
    assert_eq!(fixture("handshake_resp"), encoded.as_slice());
}
