using System;
using System.Buffers.Binary;
using System.IO;
using System.Net.Sockets;
using System.Text;

class Program
{
    const uint RequestProduce = 0;
    const uint RequestHandshake = 4;
    const uint RequestJoinGroup = 5;
    const uint RequestHeartbeat = 6;
    const uint RequestCommitOffset = 7;
    const uint RequestGroupFetch = 8;
    const uint RequestLeaveGroup = 9;

    const uint ResponseProduced = 0;
    const uint ResponseFetched = 1;
    const uint ResponseHandshakeOk = 3;
    const uint ResponseNotLeader = 4;
    const uint ResponseError = 5;
    const uint ResponseGroupJoined = 6;
    const uint ResponseHeartbeatOk = 7;
    const uint ResponseOffsetCommitted = 8;
    const uint ResponseRebalanceRequired = 9;
    const uint ResponseGroupLeft = 10;

    sealed class ConsumerGroupAssignment
    {
        public string Topic = "";
        public uint Partition;
        public ulong Offset;
        public string? LeaderHint;
    }

    sealed class GroupJoined
    {
        public string GroupId = "";
        public string MemberId = "";
        public ulong Generation;
        public ulong HeartbeatIntervalMs;
        public ulong SessionTimeoutMs;
        public ConsumerGroupAssignment[] Assignments = Array.Empty<ConsumerGroupAssignment>();
    }

    sealed class FetchedRecord
    {
        public ulong Offset;
        public byte[] Key = Array.Empty<byte>();
        public byte[] Value = Array.Empty<byte>();
        public long Timestamp;
    }

    sealed class Produced
    {
        public ulong BaseOffset;
        public ulong LastOffset;
        public uint Acks;
    }

    sealed class HandshakeOk
    {
        public ushort ServerVersion;
    }

    sealed class HeartbeatOk
    {
        public string GroupId = "";
        public string MemberId = "";
        public ulong Generation;
    }

    sealed class OffsetCommitted
    {
        public string GroupId = "";
        public string MemberId = "";
        public ulong Generation;
        public string Topic = "";
        public uint Partition;
        public ulong Offset;
    }

    sealed class GroupLeft
    {
        public string GroupId = "";
        public string MemberId = "";
        public ulong Generation;
    }

    sealed class NotLeader
    {
        public string? Leader;
    }

    sealed class RebalanceRequired
    {
        public string GroupId = "";
        public ulong Generation;
    }

    sealed class Reader
    {
        readonly BinaryReader reader;

        public Reader(byte[] payload)
        {
            reader = new BinaryReader(new MemoryStream(payload, writable: false));
        }

        public byte ReadU8() => reader.ReadByte();
        public ushort ReadU16() => reader.ReadUInt16();
        public uint ReadU32() => reader.ReadUInt32();
        public ulong ReadU64() => reader.ReadUInt64();
        public long ReadI64() => reader.ReadInt64();

        public byte[] ReadBytes()
        {
            var length = checked((int)ReadU64());
            var data = reader.ReadBytes(length);
            if (data.Length != length)
                throw new EndOfStreamException("unexpected end of response");
            return data;
        }

        public string ReadString() => Encoding.UTF8.GetString(ReadBytes());

        public string? ReadOptionalString()
        {
            var tag = ReadU8();
            return tag switch
            {
                0 => null,
                1 => ReadString(),
                _ => throw new InvalidDataException($"unexpected option tag: {tag}")
            };
        }
    }

    static void WriteString(BinaryWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        writer.Write((ulong)bytes.Length);
        writer.Write(bytes);
    }

    static void WriteOptionalString(BinaryWriter writer, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            writer.Write((byte)0);
            return;
        }

        writer.Write((byte)1);
        WriteString(writer, value);
    }

    static byte[] BuildHandshakePayload(ushort clientVersion = 1)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(RequestHandshake);
        writer.Write(clientVersion);
        writer.Flush();
        return ms.ToArray();
    }

    static byte[] BuildProducePayload(string topic, uint partition, (byte[] Key, byte[] Value, long Timestamp)[] records, string? authToken)
    {
        if (records.Length == 0)
            throw new ArgumentException("records must be non-empty", nameof(records));

        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(RequestProduce);
        WriteString(writer, topic);
        writer.Write(partition);
        writer.Write((ulong)records.Length);
        foreach (var record in records)
        {
            writer.Write((ulong)record.Key.Length);
            writer.Write(record.Key);
            writer.Write((ulong)record.Value.Length);
            writer.Write(record.Value);
            writer.Write(record.Timestamp);
        }
        WriteOptionalString(writer, authToken);
        writer.Flush();
        return ms.ToArray();
    }

    static byte[] BuildJoinGroupPayload(string groupId, string topic, string? memberId, string? authToken)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(RequestJoinGroup);
        WriteString(writer, groupId);
        WriteString(writer, topic);
        WriteOptionalString(writer, memberId);
        WriteOptionalString(writer, authToken);
        writer.Flush();
        return ms.ToArray();
    }

    static byte[] BuildHeartbeatPayload(string groupId, string topic, string memberId, ulong generation, string? authToken)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(RequestHeartbeat);
        WriteString(writer, groupId);
        WriteString(writer, topic);
        WriteString(writer, memberId);
        writer.Write(generation);
        WriteOptionalString(writer, authToken);
        writer.Flush();
        return ms.ToArray();
    }

    static byte[] BuildGroupFetchPayload(string groupId, string topic, string memberId, ulong generation, uint partition, ulong offset, uint maxBytes, string? authToken)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(RequestGroupFetch);
        WriteString(writer, groupId);
        WriteString(writer, topic);
        WriteString(writer, memberId);
        writer.Write(generation);
        writer.Write(partition);
        writer.Write(offset);
        writer.Write(maxBytes);
        WriteOptionalString(writer, authToken);
        writer.Flush();
        return ms.ToArray();
    }

    static byte[] BuildCommitOffsetPayload(string groupId, string topic, string memberId, ulong generation, uint partition, ulong offset, string? authToken)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(RequestCommitOffset);
        WriteString(writer, groupId);
        WriteString(writer, topic);
        WriteString(writer, memberId);
        writer.Write(generation);
        writer.Write(partition);
        writer.Write(offset);
        WriteOptionalString(writer, authToken);
        writer.Flush();
        return ms.ToArray();
    }

    static byte[] BuildLeaveGroupPayload(string groupId, string topic, string memberId, ulong generation, string? authToken)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(RequestLeaveGroup);
        WriteString(writer, groupId);
        WriteString(writer, topic);
        WriteString(writer, memberId);
        writer.Write(generation);
        WriteOptionalString(writer, authToken);
        writer.Flush();
        return ms.ToArray();
    }

    static void SendFrame(NetworkStream stream, byte[] payload)
    {
        Span<byte> lenBuf = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(lenBuf, (uint)payload.Length);
        stream.Write(lenBuf);
        stream.Write(payload);
        stream.Flush();
    }

    static byte[] RecvFrame(NetworkStream stream)
    {
        Span<byte> lenBuf = stackalloc byte[4];
        int read = stream.Read(lenBuf);
        if (read < 4)
            throw new IOException("connection closed");
        int length = checked((int)BinaryPrimitives.ReadUInt32BigEndian(lenBuf));
        byte[] payload = new byte[length];
        int offset = 0;
        while (offset < length)
        {
            int chunk = stream.Read(payload, offset, length - offset);
            if (chunk <= 0)
                throw new IOException("connection closed mid-frame");
            offset += chunk;
        }
        return payload;
    }

    static (string Kind, object Value) SendRequest(NetworkStream stream, byte[] payload)
    {
        SendFrame(stream, payload);
        return ParseResponse(RecvFrame(stream));
    }

    static (string Kind, object Value) ParseResponse(byte[] payload)
    {
        var reader = new Reader(payload);
        uint variant = reader.ReadU32();

        switch (variant)
        {
            case ResponseProduced:
                return ("Produced", new Produced
                {
                    BaseOffset = reader.ReadU64(),
                    LastOffset = reader.ReadU64(),
                    Acks = reader.ReadU32()
                });
            case ResponseFetched:
                {
                    ulong count = reader.ReadU64();
                    var records = new FetchedRecord[count];
                    for (ulong i = 0; i < count; i++)
                    {
                        records[i] = new FetchedRecord
                        {
                            Offset = reader.ReadU64(),
                            Key = reader.ReadBytes(),
                            Value = reader.ReadBytes(),
                            Timestamp = reader.ReadI64()
                        };
                    }
                    return ("Fetched", records);
                }
            case ResponseHandshakeOk:
                return ("HandshakeOk", new HandshakeOk { ServerVersion = reader.ReadU16() });
            case ResponseNotLeader:
                return ("NotLeader", new NotLeader { Leader = reader.ReadOptionalString() });
            case ResponseError:
                return ("Error", reader.ReadString());
            case ResponseGroupJoined:
                {
                    var response = new GroupJoined
                    {
                        GroupId = reader.ReadString(),
                        MemberId = reader.ReadString(),
                        Generation = reader.ReadU64(),
                        HeartbeatIntervalMs = reader.ReadU64(),
                        SessionTimeoutMs = reader.ReadU64()
                    };
                    ulong count = reader.ReadU64();
                    response.Assignments = new ConsumerGroupAssignment[count];
                    for (ulong i = 0; i < count; i++)
                    {
                        response.Assignments[i] = new ConsumerGroupAssignment
                        {
                            Topic = reader.ReadString(),
                            Partition = reader.ReadU32(),
                            Offset = reader.ReadU64(),
                            LeaderHint = reader.ReadOptionalString()
                        };
                    }
                    return ("GroupJoined", response);
                }
            case ResponseHeartbeatOk:
                return ("HeartbeatOk", new HeartbeatOk
                {
                    GroupId = reader.ReadString(),
                    MemberId = reader.ReadString(),
                    Generation = reader.ReadU64()
                });
            case ResponseOffsetCommitted:
                return ("OffsetCommitted", new OffsetCommitted
                {
                    GroupId = reader.ReadString(),
                    MemberId = reader.ReadString(),
                    Generation = reader.ReadU64(),
                    Topic = reader.ReadString(),
                    Partition = reader.ReadU32(),
                    Offset = reader.ReadU64()
                });
            case ResponseRebalanceRequired:
                return ("RebalanceRequired", new RebalanceRequired
                {
                    GroupId = reader.ReadString(),
                    Generation = reader.ReadU64()
                });
            case ResponseGroupLeft:
                return ("GroupLeft", new GroupLeft
                {
                    GroupId = reader.ReadString(),
                    MemberId = reader.ReadString(),
                    Generation = reader.ReadU64()
                });
            default:
                return ($"Unknown({variant})", payload);
        }
    }

    static T Expect<T>(string expected, (string Kind, object Value) response)
    {
        if (response.Kind == expected)
            return (T)response.Value;

        if (response.Kind == "Error")
            throw new InvalidOperationException("broker error: " + response.Value);

        if (response.Kind == "NotLeader")
            throw new InvalidOperationException("broker returned NotLeader");

        if (response.Kind == "RebalanceRequired")
            throw new InvalidOperationException("consumer group rebalance required");

        throw new InvalidOperationException($"unexpected response: {response.Kind}");
    }

    static string? NormalizeAuth(string? value)
    {
        if (value is null)
            return null;
        value = value.Trim();
        return value.Length == 0 ? null : value;
    }

    static (string Host, int Port) ParseAddress(string? leaderHint, string fallbackHost, int fallbackPort)
    {
        if (string.IsNullOrWhiteSpace(leaderHint))
            return (fallbackHost, fallbackPort);

        int separator = leaderHint.LastIndexOf(':');
        if (separator <= 0)
            return (fallbackHost, fallbackPort);

        string host = leaderHint[..separator];
        if (!int.TryParse(leaderHint[(separator + 1)..], out int port))
            return (fallbackHost, fallbackPort);

        return (host, port);
    }

    static void Main(string[] args)
    {
        string host = Environment.GetEnvironmentVariable("LOGOS_HOST") ?? "127.0.0.1";
        int port = int.TryParse(Environment.GetEnvironmentVariable("LOGOS_PORT"), out int parsedPort) ? parsedPort : 9092;
        string topic = Environment.GetEnvironmentVariable("LOGOS_TOPIC") ?? "compat";
        string groupId = Environment.GetEnvironmentVariable("LOGOS_GROUP_ID") ?? (topic + "-workers");
        string? auth = NormalizeAuth(Environment.GetEnvironmentVariable("LOGOS_AUTH") ?? "token-a");
        uint maxBytes = uint.TryParse(Environment.GetEnvironmentVariable("LOGOS_MAX_BYTES"), out uint parsedMaxBytes) ? parsedMaxBytes : 1024U * 1024U;

        using var bootstrapClient = new TcpClient(host, port);
        using var bootstrap = bootstrapClient.GetStream();

        var handshake = Expect<HandshakeOk>("HandshakeOk", SendRequest(bootstrap, BuildHandshakePayload()));
        Console.WriteLine($"handshake ok: server_version={handshake.ServerVersion}");

        var produced = Expect<Produced>(
            "Produced",
            SendRequest(
                bootstrap,
                BuildProducePayload(
                    topic,
                    0,
                    new[]
                    {
                        (Encoding.UTF8.GetBytes("k1"), Encoding.UTF8.GetBytes("v1"), 1L),
                        (Encoding.UTF8.GetBytes("k2"), Encoding.UTF8.GetBytes("v2"), 2L),
                    },
                    auth)));
        Console.WriteLine($"produced: base_offset={produced.BaseOffset} last_offset={produced.LastOffset} acks={produced.Acks}");

        var joined = Expect<GroupJoined>("GroupJoined", SendRequest(bootstrap, BuildJoinGroupPayload(groupId, topic, null, auth)));
        Console.WriteLine($"group joined: member_id={joined.MemberId} generation={joined.Generation} assignments={joined.Assignments.Length}");

        var heartbeat = Expect<HeartbeatOk>(
            "HeartbeatOk",
            SendRequest(bootstrap, BuildHeartbeatPayload(groupId, topic, joined.MemberId, joined.Generation, auth)));
        Console.WriteLine($"heartbeat ok: generation={heartbeat.Generation}");

        foreach (var assignment in joined.Assignments)
        {
            var target = ParseAddress(assignment.LeaderHint, host, port);
            using var fetchClient = new TcpClient(target.Host, target.Port);
            using var fetch = fetchClient.GetStream();

            _ = Expect<HandshakeOk>("HandshakeOk", SendRequest(fetch, BuildHandshakePayload()));
            var fetched = Expect<FetchedRecord[]>(
                "Fetched",
                SendRequest(
                    fetch,
                    BuildGroupFetchPayload(
                        groupId,
                        assignment.Topic,
                        joined.MemberId,
                        joined.Generation,
                        assignment.Partition,
                        assignment.Offset,
                        maxBytes,
                        auth)));
            Console.WriteLine($"group fetch: partition={assignment.Partition} from={target.Host}:{target.Port} records={fetched.Length}");
            if (fetched.Length == 0)
                continue;

            foreach (var item in fetched)
            {
                Console.WriteLine($"  record: offset={item.Offset} key={Encoding.UTF8.GetString(item.Key)} value={Encoding.UTF8.GetString(item.Value)} timestamp={item.Timestamp}");
            }

            ulong nextOffset = fetched[^1].Offset + 1;
            var committed = Expect<OffsetCommitted>(
                "OffsetCommitted",
                SendRequest(
                    bootstrap,
                    BuildCommitOffsetPayload(
                        groupId,
                        assignment.Topic,
                        joined.MemberId,
                        joined.Generation,
                        assignment.Partition,
                        nextOffset,
                        auth)));
            Console.WriteLine($"offset committed: partition={committed.Partition} offset={committed.Offset}");
        }

        var left = Expect<GroupLeft>(
            "GroupLeft",
            SendRequest(bootstrap, BuildLeaveGroupPayload(groupId, topic, joined.MemberId, joined.Generation, auth)));
        Console.WriteLine($"group left: member_id={left.MemberId} generation={left.Generation}");
    }
}
