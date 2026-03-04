using System;
using System.IO;
using System.Net.Sockets;
using System.Buffers.Binary;

class Program
{
    static byte[] Load(string rel) => File.ReadAllBytes(Path.Combine("tests/fixtures/v1", rel));

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

        SendFrame(stream, Load("handshake_req.bin"));
        var hs = RecvFrame(stream);
        Console.WriteLine("handshake resp=" + Convert.ToHexString(hs));

        SendFrame(stream, Load("produce_req.bin"));
        var prod = RecvFrame(stream);
        Console.WriteLine("produce resp=" + Convert.ToHexString(prod));
    }
}
