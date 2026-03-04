import os
import socket
import struct
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "v1"


def load(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


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
        send_frame(sock, load("handshake_req.bin"))
        hs = recv_frame(sock)
        print(f"handshake resp={hs.hex()}")

        send_frame(sock, load("produce_req.bin"))
        prod = recv_frame(sock)
        print(f"produce resp={prod.hex()}")


if __name__ == "__main__":
    main()
