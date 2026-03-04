Logos protocol quickstart (non-Rust clients)
============================================

These examples demonstrate a minimal TCP client for Logos using frozen binary frames (bincode) from `tests/fixtures/v1`. They do not build requests dynamically: each program reads a prepared payload and wraps it with a length prefix (u32 big-endian) as required by the protocol.

Important: the frames do not include `auth`, so run the broker with auth/TLS disabled for the demo (`RK_REQUIRE_AUTH=false RK_REQUIRE_TLS=false`). Do not point these fixtures at production clusters; use an SDK/client that sets auth tokens and TLS for production.

Flow: send handshake → read response → send `produce_req.bin` → read `produced_resp.bin`.

Frame files:
- `tests/fixtures/v1/handshake_req.bin`
- `tests/fixtures/v1/produce_req.bin` (topic `compat`, partition 0, two records)

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
- The examples read and print responses in hex without decoding bincode.
- Address/port can be overridden via `LOGOS_HOST` and `LOGOS_PORT` env vars.
- To generate requests dynamically in other languages, implement bincode (or bridge through a small Rust gRPC/HTTP proxy over the SDK). For brokers with TLS/Auth enabled, use an SDK/client that handles tokens and TLS.
