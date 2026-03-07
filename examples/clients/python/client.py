import os
import socket
import struct
from dataclasses import dataclass


REQUEST_PRODUCE = 0
REQUEST_HANDSHAKE = 4
REQUEST_JOIN_GROUP = 5
REQUEST_HEARTBEAT = 6
REQUEST_COMMIT_OFFSET = 7
REQUEST_GROUP_FETCH = 8
REQUEST_LEAVE_GROUP = 9

RESPONSE_PRODUCED = 0
RESPONSE_FETCHED = 1
RESPONSE_HANDSHAKE_OK = 3
RESPONSE_NOT_LEADER = 4
RESPONSE_ERROR = 5
RESPONSE_GROUP_JOINED = 6
RESPONSE_HEARTBEAT_OK = 7
RESPONSE_OFFSET_COMMITTED = 8
RESPONSE_REBALANCE_REQUIRED = 9
RESPONSE_GROUP_LEFT = 10


@dataclass
class ConsumerGroupAssignment:
    topic: str
    partition: int
    offset: int
    leader_hint: str | None


@dataclass
class GroupJoined:
    group_id: str
    member_id: str
    generation: int
    heartbeat_interval_ms: int
    session_timeout_ms: int
    assignments: list[ConsumerGroupAssignment]


@dataclass
class FetchedRecord:
    offset: int
    key: bytes
    value: bytes
    timestamp: int


class Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def read_exact(self, size: int) -> bytes:
        chunk = self.data[self.pos : self.pos + size]
        if len(chunk) != size:
            raise ValueError("unexpected end of response")
        self.pos += size
        return chunk

    def read_u8(self) -> int:
        return self.read_exact(1)[0]

    def read_u16(self) -> int:
        return struct.unpack("<H", self.read_exact(2))[0]

    def read_u32(self) -> int:
        return struct.unpack("<I", self.read_exact(4))[0]

    def read_u64(self) -> int:
        return struct.unpack("<Q", self.read_exact(8))[0]

    def read_i64(self) -> int:
        return struct.unpack("<q", self.read_exact(8))[0]

    def read_bytes(self) -> bytes:
        size = self.read_u64()
        return self.read_exact(size)

    def read_string(self) -> str:
        return self.read_bytes().decode("utf-8")

    def read_opt_string(self) -> str | None:
        tag = self.read_u8()
        if tag == 0:
            return None
        if tag != 1:
            raise ValueError(f"unexpected option tag: {tag}")
        return self.read_string()


def write_string(buf: bytearray, value: str) -> None:
    data = value.encode("utf-8")
    buf += struct.pack("<Q", len(data))
    buf += data


def write_opt_string(buf: bytearray, value: str | None) -> None:
    if value is None:
        buf += b"\x00"
    else:
        buf += b"\x01"
        write_string(buf, value)


def build_handshake(client_version: int = 1) -> bytes:
    return struct.pack("<IH", REQUEST_HANDSHAKE, client_version)


def build_produce(
    topic: str, partition: int, records: list[tuple[bytes, bytes, int]], auth: str | None
) -> bytes:
    if not records:
        raise ValueError("records must be non-empty")

    buf = bytearray()
    buf += struct.pack("<I", REQUEST_PRODUCE)
    write_string(buf, topic)
    buf += struct.pack("<I", partition)
    buf += struct.pack("<Q", len(records))
    for key, value, timestamp in records:
        buf += struct.pack("<Q", len(key))
        buf += key
        buf += struct.pack("<Q", len(value))
        buf += value
        buf += struct.pack("<q", timestamp)
    write_opt_string(buf, auth)
    return bytes(buf)


def build_join_group(
    group_id: str, topic: str, member_id: str | None, auth: str | None
) -> bytes:
    buf = bytearray()
    buf += struct.pack("<I", REQUEST_JOIN_GROUP)
    write_string(buf, group_id)
    write_string(buf, topic)
    write_opt_string(buf, member_id)
    write_opt_string(buf, auth)
    return bytes(buf)


def build_heartbeat(
    group_id: str, topic: str, member_id: str, generation: int, auth: str | None
) -> bytes:
    buf = bytearray()
    buf += struct.pack("<I", REQUEST_HEARTBEAT)
    write_string(buf, group_id)
    write_string(buf, topic)
    write_string(buf, member_id)
    buf += struct.pack("<Q", generation)
    write_opt_string(buf, auth)
    return bytes(buf)


def build_group_fetch(
    group_id: str,
    topic: str,
    member_id: str,
    generation: int,
    partition: int,
    offset: int,
    max_bytes: int,
    auth: str | None,
) -> bytes:
    buf = bytearray()
    buf += struct.pack("<I", REQUEST_GROUP_FETCH)
    write_string(buf, group_id)
    write_string(buf, topic)
    write_string(buf, member_id)
    buf += struct.pack("<Q", generation)
    buf += struct.pack("<I", partition)
    buf += struct.pack("<Q", offset)
    buf += struct.pack("<I", max_bytes)
    write_opt_string(buf, auth)
    return bytes(buf)


def build_commit_offset(
    group_id: str,
    topic: str,
    member_id: str,
    generation: int,
    partition: int,
    offset: int,
    auth: str | None,
) -> bytes:
    buf = bytearray()
    buf += struct.pack("<I", REQUEST_COMMIT_OFFSET)
    write_string(buf, group_id)
    write_string(buf, topic)
    write_string(buf, member_id)
    buf += struct.pack("<Q", generation)
    buf += struct.pack("<I", partition)
    buf += struct.pack("<Q", offset)
    write_opt_string(buf, auth)
    return bytes(buf)


def build_leave_group(
    group_id: str, topic: str, member_id: str, generation: int, auth: str | None
) -> bytes:
    buf = bytearray()
    buf += struct.pack("<I", REQUEST_LEAVE_GROUP)
    write_string(buf, group_id)
    write_string(buf, topic)
    write_string(buf, member_id)
    buf += struct.pack("<Q", generation)
    write_opt_string(buf, auth)
    return bytes(buf)


def parse_response(payload: bytes) -> tuple[str, object]:
    reader = Reader(payload)
    variant = reader.read_u32()

    if variant == RESPONSE_PRODUCED:
        return (
            "Produced",
            {
                "base_offset": reader.read_u64(),
                "last_offset": reader.read_u64(),
                "acks": reader.read_u32(),
            },
        )
    if variant == RESPONSE_FETCHED:
        count = reader.read_u64()
        records: list[FetchedRecord] = []
        for _ in range(count):
            offset = reader.read_u64()
            key = reader.read_bytes()
            value = reader.read_bytes()
            timestamp = reader.read_i64()
            records.append(FetchedRecord(offset, key, value, timestamp))
        return ("Fetched", records)
    if variant == RESPONSE_HANDSHAKE_OK:
        return ("HandshakeOk", {"server_version": reader.read_u16()})
    if variant == RESPONSE_NOT_LEADER:
        return ("NotLeader", {"leader": reader.read_opt_string()})
    if variant == RESPONSE_ERROR:
        return ("Error", reader.read_string())
    if variant == RESPONSE_GROUP_JOINED:
        group_id = reader.read_string()
        member_id = reader.read_string()
        generation = reader.read_u64()
        heartbeat_interval_ms = reader.read_u64()
        session_timeout_ms = reader.read_u64()
        assignments_count = reader.read_u64()
        assignments: list[ConsumerGroupAssignment] = []
        for _ in range(assignments_count):
            assignments.append(
                ConsumerGroupAssignment(
                    topic=reader.read_string(),
                    partition=reader.read_u32(),
                    offset=reader.read_u64(),
                    leader_hint=reader.read_opt_string(),
                )
            )
        return (
            "GroupJoined",
            GroupJoined(
                group_id,
                member_id,
                generation,
                heartbeat_interval_ms,
                session_timeout_ms,
                assignments,
            ),
        )
    if variant == RESPONSE_HEARTBEAT_OK:
        return (
            "HeartbeatOk",
            {
                "group_id": reader.read_string(),
                "member_id": reader.read_string(),
                "generation": reader.read_u64(),
            },
        )
    if variant == RESPONSE_OFFSET_COMMITTED:
        return (
            "OffsetCommitted",
            {
                "group_id": reader.read_string(),
                "member_id": reader.read_string(),
                "generation": reader.read_u64(),
                "topic": reader.read_string(),
                "partition": reader.read_u32(),
                "offset": reader.read_u64(),
            },
        )
    if variant == RESPONSE_REBALANCE_REQUIRED:
        return (
            "RebalanceRequired",
            {"group_id": reader.read_string(), "generation": reader.read_u64()},
        )
    if variant == RESPONSE_GROUP_LEFT:
        return (
            "GroupLeft",
            {
                "group_id": reader.read_string(),
                "member_id": reader.read_string(),
                "generation": reader.read_u64(),
            },
        )

    return (f"Unknown({variant})", payload.hex())


def send_frame(sock: socket.socket, payload: bytes) -> None:
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise ConnectionError("connection closed")
        chunks += chunk
    return bytes(chunks)


def recv_frame(sock: socket.socket) -> bytes:
    header = recv_exact(sock, 4)
    (length,) = struct.unpack(">I", header)
    return recv_exact(sock, length)


def request(sock: socket.socket, payload: bytes) -> tuple[str, object]:
    send_frame(sock, payload)
    return parse_response(recv_frame(sock))


def normalize_auth(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def parse_address(leader_hint: str | None, fallback_host: str, fallback_port: int) -> tuple[str, int]:
    if not leader_hint:
        return (fallback_host, fallback_port)
    if ":" not in leader_hint:
        return (fallback_host, fallback_port)
    host, port_text = leader_hint.rsplit(":", 1)
    if not host or not port_text.isdigit():
        return (fallback_host, fallback_port)
    return (host, int(port_text))


def expect(name: str, response: tuple[str, object]) -> object:
    actual, payload = response
    if actual == name:
        return payload
    if actual == "Error":
        raise RuntimeError(str(payload))
    if actual == "NotLeader":
        raise RuntimeError(f"broker is not leader: {payload}")
    if actual == "RebalanceRequired":
        raise RuntimeError(f"consumer group rebalance required: {payload}")
    raise RuntimeError(f"unexpected response: {actual} {payload}")


def main() -> None:
    host = os.environ.get("LOGOS_HOST", "127.0.0.1")
    port = int(os.environ.get("LOGOS_PORT", "9092"))
    topic = os.environ.get("LOGOS_TOPIC", "compat")
    group_id = os.environ.get("LOGOS_GROUP_ID", f"{topic}-workers")
    auth = normalize_auth(os.environ.get("LOGOS_AUTH", "token-a"))
    max_bytes = int(os.environ.get("LOGOS_MAX_BYTES", str(1024 * 1024)))

    with socket.create_connection((host, port)) as bootstrap:
        handshake = expect("HandshakeOk", request(bootstrap, build_handshake()))
        print(f"handshake ok: server_version={handshake['server_version']}")

        produced = expect(
            "Produced",
            request(
                bootstrap,
                build_produce(
                    topic=topic,
                    partition=0,
                    records=[
                        (b"k1", b"v1", 1),
                        (b"k2", b"v2", 2),
                    ],
                    auth=auth,
                ),
            ),
        )
        print(
            "produced:",
            f"base_offset={produced['base_offset']}",
            f"last_offset={produced['last_offset']}",
            f"acks={produced['acks']}",
        )

        joined = expect(
            "GroupJoined",
            request(bootstrap, build_join_group(group_id, topic, None, auth)),
        )
        assert isinstance(joined, GroupJoined)
        print(
            "group joined:",
            f"member_id={joined.member_id}",
            f"generation={joined.generation}",
            f"assignments={[(a.partition, a.offset, a.leader_hint) for a in joined.assignments]}",
        )

        heartbeat = expect(
            "HeartbeatOk",
            request(
                bootstrap,
                build_heartbeat(
                    group_id,
                    topic,
                    joined.member_id,
                    joined.generation,
                    auth,
                ),
            ),
        )
        print(f"heartbeat ok: generation={heartbeat['generation']}")

        for assignment in joined.assignments:
            fetch_host, fetch_port = parse_address(assignment.leader_hint, host, port)
            with socket.create_connection((fetch_host, fetch_port)) as fetch_sock:
                expect("HandshakeOk", request(fetch_sock, build_handshake()))
                fetched = expect(
                    "Fetched",
                    request(
                        fetch_sock,
                        build_group_fetch(
                            group_id,
                            assignment.topic,
                            joined.member_id,
                            joined.generation,
                            assignment.partition,
                            assignment.offset,
                            max_bytes,
                            auth,
                        ),
                    ),
                )
                assert isinstance(fetched, list)
                print(
                    f"group fetch partition={assignment.partition} from={fetch_host}:{fetch_port} records={len(fetched)}"
                )
                if not fetched:
                    continue
                for record in fetched:
                    print(
                        "  record:",
                        f"offset={record.offset}",
                        f"key={record.key.decode('utf-8', errors='replace')}",
                        f"value={record.value.decode('utf-8', errors='replace')}",
                        f"timestamp={record.timestamp}",
                    )

                next_offset = fetched[-1].offset + 1
                committed = expect(
                    "OffsetCommitted",
                    request(
                        bootstrap,
                        build_commit_offset(
                            group_id,
                            assignment.topic,
                            joined.member_id,
                            joined.generation,
                            assignment.partition,
                            next_offset,
                            auth,
                        ),
                    ),
                )
                print(
                    "offset committed:",
                    f"partition={committed['partition']}",
                    f"offset={committed['offset']}",
                )

        left = expect(
            "GroupLeft",
            request(
                bootstrap,
                build_leave_group(
                    group_id,
                    topic,
                    joined.member_id,
                    joined.generation,
                    auth,
                ),
            ),
        )
        print(f"group left: member_id={left['member_id']} generation={left['generation']}")


if __name__ == "__main__":
    main()
