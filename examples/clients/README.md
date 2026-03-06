Logos protocol quickstart (non-Rust clients)
============================================

These examples build the handshake, produce, and fetch requests in code using `bincode`-compatible encoders and wrap them with a u32 big-endian length prefix (the broker framing). Each client prints the raw responses in hex.

Flow
- Send `Handshake { client_version: 1 }` → read response.
- Send `Produce { topic: "compat", partition: 0, records: [(k1,v1,ts=1), (k2,v2,ts=2)], auth: token-a }` → read response.
	- `records` must be non-empty; the broker rejects empty produce batches.
- Send `Fetch { topic: "compat", partition: 0, offset: 0, max_bytes: 1048576, auth: token-a }` → read response.
	- `Fetch` returns records from the requested offset on the node you connect to.
	- After compaction, offsets can be sparse, so a fetch from a removed offset resumes at the next visible record.
- Override host/port with `LOGOS_HOST` / `LOGOS_PORT` (defaults `127.0.0.1:9092`).

End-to-end flow diagram (produce -> consume)
```mermaid
flowchart LR
	P[Producer client] -->|1. Handshake| B[Broker]
	P -->|2. Produce(topic, partition, records, auth)| B
	B -->|3. AuthZ + leader check| B
	B -->|4. Append to local WAL| L[(Leader partition log)]
	B -->|5. Replicate to followers when acks > 1| F[(Follower logs)]
	B -->|6. Produced(base_offset, last_offset, acks)| P

	C[Consumer client] -->|7. Fetch(topic, partition, offset, max_bytes, auth)| B
	B -->|8. Read from partition log| L
	B -->|9. Fetched(records)| C
	C -->|10. Next fetch uses last_offset + 1| C
```

Redirect and retry behavior
- If `Produce` or `Fetch` returns `NotLeader { leader }`, reconnect to the reported leader and retry.
- Consumers should keep track of the last consumed offset and increment it between fetch calls; do not assume every intermediate offset still exists after compaction.
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
- For production, use an SDK that manages auth tokens and TLS instead of these minimal examples.
