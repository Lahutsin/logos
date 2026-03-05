Logos protocol quickstart (non-Rust clients)
============================================

These examples build the handshake and produce requests in code using `bincode`-compatible encoders and wrap them with a u32 big-endian length prefix (the broker framing). Each client prints the raw responses in hex.

Flow
- Send `Handshake { client_version: 1 }` → read response.
- Send `Produce { topic: "compat", partition: 0, records: [(k1,v1,ts=1), (k2,v2,ts=2)], auth: token-a }` → read response.
	- `records` must be non-empty; the broker rejects empty produce batches.
- Override host/port with `LOGOS_HOST` / `LOGOS_PORT` (defaults `127.0.0.1:9092`).

Broker setup
- Quick demo (auth/TLS off): `RK_REQUIRE_AUTH=false RK_REQUIRE_TLS=false cargo run --release`
- With auth on (default): create `auth.json` like:
	```json
	{"token-a": {"name": "demo-writer", "topics": ["compat"], "allow_produce": true}}
	```
	then run `RK_AUTH_PATH=./auth.json cargo run --release` so `token-a` can produce.

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
- For production, use an SDK that manages auth tokens and TLS instead of these minimal examples.
