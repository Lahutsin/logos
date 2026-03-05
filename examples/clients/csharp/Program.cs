using System;
using System.IO;
using System.Net.Sockets;
using System.Buffers.Binary;

class Program
{
    // Minimal bincode-compatible encoders for handshake and produce so payloads can be built in code.
    static byte[] BuildHandshakePayload(ushort clientVersion = 1)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        bw.Write((uint)4); // Request::Handshake variant index
        bw.Write(clientVersion);
        bw.Flush();
        return ms.ToArray();
    }

    static byte[] BuildProducePayload(
        string topic,
        uint partition,
        (byte[] key, byte[] value, long timestamp)[] records,
        string? authToken)
    {
        if (records.Length == 0)
            throw new ArgumentException("records must be non-empty", nameof(records));

        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);

        // Request::Produce variant index (see protocol.rs order)
        bw.Write((uint)0);

        // topic: String (u64 len + bytes)
        var topicBytes = System.Text.Encoding.UTF8.GetBytes(topic);
        bw.Write((ulong)topicBytes.Length);
        bw.Write(topicBytes);

        // partition: u32
        bw.Write(partition);

        // records: Vec<Record>
        bw.Write((ulong)records.Length);
        foreach (var r in records)
        {
            bw.Write((ulong)r.key.Length);
            bw.Write(r.key);
            bw.Write((ulong)r.value.Length);
            bw.Write(r.value);
            bw.Write(r.timestamp);
        }

        // auth: Option<String> (u8 tag then string if Some)
        if (authToken is null)
        {
            bw.Write((byte)0); // None
        }
        else
        {
            bw.Write((byte)1); // Some
            var authBytes = System.Text.Encoding.UTF8.GetBytes(authToken);
            bw.Write((ulong)authBytes.Length);
            bw.Write(authBytes);
        }

        bw.Flush();
        return ms.ToArray();
    }

    static void SendFrame(NetworkStream stream, byte[] payload)
    {
        Span<byte> lenBuf = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(lenBuf, payload.Length);
        stream.Write(lenBuf);
        stream.Write(payload);
        stream.Flush();
    }

    static byte[] RecvFrame(NetworkStream stream)
    {
        Span<byte> lenBuf = stackalloc byte[4];
        int read = stream.Read(lenBuf);
        if (read < 4) throw new IOException("connection closed");
        int len = BinaryPrimitives.ReadInt32BigEndian(lenBuf);
        byte[] payload = new byte[len];
        int off = 0;
        while (off < len)
        {
            int r = stream.Read(payload, off, len - off);
            if (r <= 0) throw new IOException("connection closed mid-frame");
            off += r;
        }
        return payload;
    }

    static void Main(string[] args)
    {
        var host = Environment.GetEnvironmentVariable("LOGOS_HOST") ?? "127.0.0.1";
        var port = int.TryParse(Environment.GetEnvironmentVariable("LOGOS_PORT"), out var p) ? p : 9092;
        using var client = new TcpClient(host, port);
        using var stream = client.GetStream();

        // Build handshake request directly in code.
        var hsPayload = BuildHandshakePayload();
        SendFrame(stream, hsPayload);
        var hs = RecvFrame(stream);
        Console.WriteLine("handshake resp=" + Convert.ToHexString(hs));

        // Build a Produce request: topic "compat", partition 0, two records, auth token "token-a".
        var prodPayload = BuildProducePayload(
            topic: "compat",
            partition: 0,
            records: new[]
            {
                (key: new byte[] {(byte)'k', (byte)'1'}, value: new byte[] {(byte)'v', (byte)'1'}, timestamp: 1L),
                (key: new byte[] {(byte)'k', (byte)'2'}, value: new byte[] {(byte)'v', (byte)'2'}, timestamp: 2L),
            },
            authToken: "token-a");
        SendFrame(stream, prodPayload);
        var prod = RecvFrame(stream);
        Console.WriteLine("produce resp=" + Convert.ToHexString(prod));
    }
}
