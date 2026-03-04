import os
import socket
import struct


def write_string(buf: bytearray, s: str) -> None:
    data = s.encode("utf-8")
    buf += struct.pack("<Q", len(data))
    buf += data


def build_handshake(client_version: int = 1) -> bytes:
    buf = bytearray()
    buf += struct.pack("<I", 4)  # variant index for Handshake
    buf += struct.pack("<H", client_version)
    return bytes(buf)


def build_produce(topic: str, partition: int, records: list[tuple[bytes, bytes, int]], auth: str | None) -> bytes:
    buf = bytearray()
    buf += struct.pack("<I", 0)  # variant index for Produce

    write_string(buf, topic)
    buf += struct.pack("<I", partition)

    buf += struct.pack("<Q", len(records))
    for key, value, ts in records:
        buf += struct.pack("<Q", len(key))
        buf += key
        buf += struct.pack("<Q", len(value))
        buf += value
        buf += struct.pack("<q", ts)

    if auth is None:
        buf += b"\x00"  # Option::None
    else:
        buf += b"\x01"  # Option::Some
        write_string(buf, auth)

    return bytes(buf)


def send_frame(sock: socket.socket, payload: bytes) -> None:
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def recv_frame(sock: socket.socket) -> bytes:
    header = sock.recv(4)
    if len(header) < 4:
        raise ConnectionError("connection closed")
    (length,) = struct.unpack(">I", header)
    data = b""
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise ConnectionError("connection closed mid-frame")
        data += chunk
    return data


def main() -> None:
    host = os.environ.get("LOGOS_HOST", "127.0.0.1")
    port = int(os.environ.get("LOGOS_PORT", "9092"))
    with socket.create_connection((host, port)) as sock:
        hs_payload = build_handshake()
        send_frame(sock, hs_payload)
        hs = recv_frame(sock)
        print(f"handshake resp={hs.hex()}")

        prod_payload = build_produce(
            topic="compat",
            partition=0,
            records=[
                (b"k1", b"v1", 1),
                (b"k2", b"v2", 2),
            ],
            auth="token-a",
        )
        send_frame(sock, prod_payload)
        prod = recv_frame(sock)
        print(f"produce resp={prod.hex()}")


if __name__ == "__main__":
    main()
