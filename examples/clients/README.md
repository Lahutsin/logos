Logos protocol quickstart (non-Rust clients)
============================================

These examples now cover the updated consumer-group flow. Each client builds `bincode`-compatible payloads manually, wraps them with the broker's u32 big-endian frame length, and parses the responses enough to drive a short happy-path run.

Flow
- Send `Handshake { client_version: 1 }` -> read `HandshakeOk`.
- Send `Produce { topic, partition: 0, records, auth? }` -> read `Produced`.
	- `records` must be non-empty; the broker rejects empty produce batches.
- Send `JoinGroup { group_id, topic, member_id?, auth? }` -> read `GroupJoined`.
- Send one `Heartbeat { group_id, topic, member_id, generation, auth? }` -> read `HeartbeatOk`.
- For every returned assignment:
	- connect to `leader_hint` when it looks like `host:port`, otherwise fall back to the bootstrap broker;
	- send `GroupFetch { group_id, topic, member_id, generation, partition, offset, max_bytes, auth? }`;
	- print fetched records;
	- send `CommitOffset` with `last_seen_offset + 1`.
- Send `LeaveGroup` on shutdown.
- Override connection and request parameters with:
	- `LOGOS_HOST` / `LOGOS_PORT` (defaults `127.0.0.1:9092`)
	- `LOGOS_TOPIC` (default `compat`)
	- `LOGOS_GROUP_ID` (default `compat-workers`)
	- `LOGOS_AUTH` (default `token-a`; set empty to disable)
	- `LOGOS_MAX_BYTES` (default `1048576`)

End-to-end flow diagram (produce -> group consume)
```mermaid
flowchart LR
	P[Producer client] -->|1. Handshake| B[Broker]
	P -->|2. Produce(topic, partition, records, auth)| B
	B -->|3. AuthZ + leader check| B
	B -->|4. Append to local WAL| L[(Leader partition log)]
	B -->|5. Replicate to followers when acks > 1| F[(Follower logs)]
	B -->|6. Produced(base_offset, last_offset, acks)| P

	C[Group consumer client] -->|7. JoinGroup(group_id, topic)| B
	B -->|8. GroupJoined(assignments, leader_hint)| C
	C -->|9. Heartbeat(group_id, member_id, generation)| B
	C -->|10. GroupFetch(group_id, partition, offset)| L
	L -->|11. Fetched(records)| C
	C -->|12. CommitOffset(last_offset + 1)| B
	C -->|13. LeaveGroup| B
```

Redirect and retry behavior
- If `Produce` returns `NotLeader { leader }`, reconnect to the reported leader and retry.
- These examples prefer `leader_hint` from `GroupJoined` for `GroupFetch`. If a hint is missing or does not look like `host:port`, they fall back to the bootstrap broker.
- Group consumers should commit `last_processed_offset + 1`; do not assume every intermediate offset still exists after compaction.
- For long-running consumers, keep heartbeats running on a dedicated connection and rejoin on `RebalanceRequired { generation }`.
- With `RK_REPLICATION_ACKS >= 2`, producer ack waits for leader + follower durability.

Broker setup
- Quick demo (auth/TLS off): `RK_REQUIRE_AUTH=false RK_REQUIRE_TLS=false cargo run --release`
- With auth on (default): create `auth.json` like:
	```json
	{"token-a": {"name": "demo-client", "topics": ["compat"], "allow_produce": true, "allow_fetch": true}}
	```
	then run `RK_AUTH_PATH=./auth.json cargo run --release` so `token-a` can produce and fetch.

Java
----
```bash
javac examples/clients/java/Client.java
java -cp examples/clients/java Client
```

Kotlin
------
```bash
kotlinc examples/clients/kotlin/KotlinClient.kt -include-runtime -d logos-kotlin.jar
java -jar logos-kotlin.jar
```

C# (.NET)
---------
```bash
csc -out:logos-client.exe examples/clients/csharp/Program.cs
./logos-client.exe
```

Python
------
```bash
python examples/clients/python/client.py
```

Go
--
```bash
go run ./examples/clients/go
```

Notes
- These clients construct payloads directly; protocol fixtures remain under [tests/fixtures/v1](tests/fixtures/v1) for compatibility tests and for regenerating via `cargo run --quiet --example gen_fixtures`.
- `Replicate` is an internal broker-to-broker request; if you implement it outside Rust, `entries` must be non-empty.
- `ValidateGroupFetch` and `ReplicateCoordinatorState` are internal broker-to-broker requests; client examples should not send them.
- For production, use an SDK that manages auth tokens, TLS, heartbeats, and rebalance handling instead of these minimal examples.
