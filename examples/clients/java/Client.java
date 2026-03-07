import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Client {
    private static final int REQUEST_PRODUCE = 0;
    private static final int REQUEST_HANDSHAKE = 4;
    private static final int REQUEST_JOIN_GROUP = 5;
    private static final int REQUEST_HEARTBEAT = 6;
    private static final int REQUEST_COMMIT_OFFSET = 7;
    private static final int REQUEST_GROUP_FETCH = 8;
    private static final int REQUEST_LEAVE_GROUP = 9;

    private static final int RESPONSE_PRODUCED = 0;
    private static final int RESPONSE_FETCHED = 1;
    private static final int RESPONSE_HANDSHAKE_OK = 3;
    private static final int RESPONSE_NOT_LEADER = 4;
    private static final int RESPONSE_ERROR = 5;
    private static final int RESPONSE_GROUP_JOINED = 6;
    private static final int RESPONSE_HEARTBEAT_OK = 7;
    private static final int RESPONSE_OFFSET_COMMITTED = 8;
    private static final int RESPONSE_REBALANCE_REQUIRED = 9;
    private static final int RESPONSE_GROUP_LEFT = 10;

    private static final class Rec {
        final byte[] key;
        final byte[] value;
        final long timestamp;

        Rec(byte[] key, byte[] value, long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    private static final class ConsumerGroupAssignment {
        String topic;
        int partition;
        long offset;
        String leaderHint;
    }

    private static final class GroupJoined {
        String groupId;
        String memberId;
        long generation;
        long heartbeatIntervalMs;
        long sessionTimeoutMs;
        List<ConsumerGroupAssignment> assignments;
    }

    private static final class FetchedRecord {
        long offset;
        byte[] key;
        byte[] value;
        long timestamp;
    }

    private static final class Produced {
        long baseOffset;
        long lastOffset;
        int acks;
    }

    private static final class HandshakeOk {
        int serverVersion;
    }

    private static final class HeartbeatOk {
        String groupId;
        String memberId;
        long generation;
    }

    private static final class OffsetCommitted {
        String groupId;
        String memberId;
        long generation;
        String topic;
        int partition;
        long offset;
    }

    private static final class GroupLeft {
        String groupId;
        String memberId;
        long generation;
    }

    private static final class NotLeader {
        String leader;
    }

    private static final class RebalanceRequired {
        String groupId;
        long generation;
    }

    private static final class ParsedResponse {
        final String kind;
        final Object value;

        ParsedResponse(String kind, Object value) {
            this.kind = kind;
            this.value = value;
        }
    }

    private static final class Reader {
        private final byte[] data;
        private int pos;

        Reader(byte[] data) {
            this.data = data;
            this.pos = 0;
        }

        private byte[] readExact(int size) throws IOException {
            if (pos + size > data.length) {
                throw new IOException("unexpected end of response");
            }
            byte[] out = new byte[size];
            System.arraycopy(data, pos, out, 0, size);
            pos += size;
            return out;
        }

        int readU8() throws IOException {
            return Byte.toUnsignedInt(readExact(1)[0]);
        }

        int readU16() throws IOException {
            return Short.toUnsignedInt(ByteBuffer.wrap(readExact(2)).order(ByteOrder.LITTLE_ENDIAN).getShort());
        }

        int readU32() throws IOException {
            return ByteBuffer.wrap(readExact(4)).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }

        long readU64() throws IOException {
            return ByteBuffer.wrap(readExact(8)).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }

        long readI64() throws IOException {
            return ByteBuffer.wrap(readExact(8)).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }

        byte[] readBytes() throws IOException {
            int length = Math.toIntExact(readU64());
            return readExact(length);
        }

        String readString() throws IOException {
            return new String(readBytes(), StandardCharsets.UTF_8);
        }

        String readOptionalString() throws IOException {
            int tag = readU8();
            if (tag == 0) {
                return null;
            }
            if (tag != 1) {
                throw new IOException("unexpected option tag: " + tag);
            }
            return readString();
        }
    }

    private static void writeU16LE(ByteArrayOutputStream out, int value) {
        out.writeBytes(ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) value).array());
    }

    private static void writeU32LE(ByteArrayOutputStream out, long value) {
        out.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt((int) value).array());
    }

    private static void writeU64LE(ByteArrayOutputStream out, long value) {
        out.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array());
    }

    private static void writeString(ByteArrayOutputStream out, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeU64LE(out, bytes.length);
        out.writeBytes(bytes);
    }

    private static void writeOptionalString(ByteArrayOutputStream out, String value) {
        if (value == null || value.isBlank()) {
            out.write(0);
            return;
        }
        out.write(1);
        writeString(out, value);
    }

    private static byte[] buildHandshake(short clientVersion) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32LE(out, REQUEST_HANDSHAKE);
        writeU16LE(out, clientVersion);
        return out.toByteArray();
    }

    private static byte[] buildProduce(String topic, int partition, List<Rec> records, String auth) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("records must be non-empty");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32LE(out, REQUEST_PRODUCE);
        writeString(out, topic);
        writeU32LE(out, partition);
        writeU64LE(out, records.size());
        for (Rec record : records) {
            writeU64LE(out, record.key.length);
            out.writeBytes(record.key);
            writeU64LE(out, record.value.length);
            out.writeBytes(record.value);
            writeU64LE(out, record.timestamp);
        }
        writeOptionalString(out, auth);
        return out.toByteArray();
    }

    private static byte[] buildJoinGroup(String groupId, String topic, String memberId, String auth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32LE(out, REQUEST_JOIN_GROUP);
        writeString(out, groupId);
        writeString(out, topic);
        writeOptionalString(out, memberId);
        writeOptionalString(out, auth);
        return out.toByteArray();
    }

    private static byte[] buildHeartbeat(String groupId, String topic, String memberId, long generation, String auth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32LE(out, REQUEST_HEARTBEAT);
        writeString(out, groupId);
        writeString(out, topic);
        writeString(out, memberId);
        writeU64LE(out, generation);
        writeOptionalString(out, auth);
        return out.toByteArray();
    }

    private static byte[] buildGroupFetch(String groupId, String topic, String memberId, long generation, int partition, long offset, int maxBytes, String auth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32LE(out, REQUEST_GROUP_FETCH);
        writeString(out, groupId);
        writeString(out, topic);
        writeString(out, memberId);
        writeU64LE(out, generation);
        writeU32LE(out, partition);
        writeU64LE(out, offset);
        writeU32LE(out, maxBytes);
        writeOptionalString(out, auth);
        return out.toByteArray();
    }

    private static byte[] buildCommitOffset(String groupId, String topic, String memberId, long generation, int partition, long offset, String auth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32LE(out, REQUEST_COMMIT_OFFSET);
        writeString(out, groupId);
        writeString(out, topic);
        writeString(out, memberId);
        writeU64LE(out, generation);
        writeU32LE(out, partition);
        writeU64LE(out, offset);
        writeOptionalString(out, auth);
        return out.toByteArray();
    }

    private static byte[] buildLeaveGroup(String groupId, String topic, String memberId, long generation, String auth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32LE(out, REQUEST_LEAVE_GROUP);
        writeString(out, groupId);
        writeString(out, topic);
        writeString(out, memberId);
        writeU64LE(out, generation);
        writeOptionalString(out, auth);
        return out.toByteArray();
    }

    private static void sendFrame(DataOutputStream out, byte[] payload) throws IOException {
        ByteBuffer len = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(payload.length);
        out.write(len.array());
        out.write(payload);
        out.flush();
    }

    private static byte[] recvFrame(DataInputStream in) throws IOException {
        byte[] lenBuf = in.readNBytes(4);
        if (lenBuf.length < 4) {
            throw new IOException("connection closed");
        }
        int length = ByteBuffer.wrap(lenBuf).order(ByteOrder.BIG_ENDIAN).getInt();
        byte[] payload = in.readNBytes(length);
        if (payload.length < length) {
            throw new IOException("connection closed mid-frame");
        }
        return payload;
    }

    private static ParsedResponse sendRequest(DataOutputStream out, DataInputStream in, byte[] payload) throws IOException {
        sendFrame(out, payload);
        return parseResponse(recvFrame(in));
    }

    private static ParsedResponse parseResponse(byte[] payload) throws IOException {
        Reader reader = new Reader(payload);
        int variant = reader.readU32();
        switch (variant) {
            case RESPONSE_PRODUCED -> {
                Produced produced = new Produced();
                produced.baseOffset = reader.readU64();
                produced.lastOffset = reader.readU64();
                produced.acks = reader.readU32();
                return new ParsedResponse("Produced", produced);
            }
            case RESPONSE_FETCHED -> {
                int count = Math.toIntExact(reader.readU64());
                List<FetchedRecord> records = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    FetchedRecord item = new FetchedRecord();
                    item.offset = reader.readU64();
                    item.key = reader.readBytes();
                    item.value = reader.readBytes();
                    item.timestamp = reader.readI64();
                    records.add(item);
                }
                return new ParsedResponse("Fetched", records);
            }
            case RESPONSE_HANDSHAKE_OK -> {
                HandshakeOk ok = new HandshakeOk();
                ok.serverVersion = reader.readU16();
                return new ParsedResponse("HandshakeOk", ok);
            }
            case RESPONSE_NOT_LEADER -> {
                NotLeader notLeader = new NotLeader();
                notLeader.leader = reader.readOptionalString();
                return new ParsedResponse("NotLeader", notLeader);
            }
            case RESPONSE_ERROR -> {
                return new ParsedResponse("Error", reader.readString());
            }
            case RESPONSE_GROUP_JOINED -> {
                GroupJoined joined = new GroupJoined();
                joined.groupId = reader.readString();
                joined.memberId = reader.readString();
                joined.generation = reader.readU64();
                joined.heartbeatIntervalMs = reader.readU64();
                joined.sessionTimeoutMs = reader.readU64();
                int count = Math.toIntExact(reader.readU64());
                joined.assignments = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    ConsumerGroupAssignment assignment = new ConsumerGroupAssignment();
                    assignment.topic = reader.readString();
                    assignment.partition = reader.readU32();
                    assignment.offset = reader.readU64();
                    assignment.leaderHint = reader.readOptionalString();
                    joined.assignments.add(assignment);
                }
                return new ParsedResponse("GroupJoined", joined);
            }
            case RESPONSE_HEARTBEAT_OK -> {
                HeartbeatOk ok = new HeartbeatOk();
                ok.groupId = reader.readString();
                ok.memberId = reader.readString();
                ok.generation = reader.readU64();
                return new ParsedResponse("HeartbeatOk", ok);
            }
            case RESPONSE_OFFSET_COMMITTED -> {
                OffsetCommitted committed = new OffsetCommitted();
                committed.groupId = reader.readString();
                committed.memberId = reader.readString();
                committed.generation = reader.readU64();
                committed.topic = reader.readString();
                committed.partition = reader.readU32();
                committed.offset = reader.readU64();
                return new ParsedResponse("OffsetCommitted", committed);
            }
            case RESPONSE_REBALANCE_REQUIRED -> {
                RebalanceRequired required = new RebalanceRequired();
                required.groupId = reader.readString();
                required.generation = reader.readU64();
                return new ParsedResponse("RebalanceRequired", required);
            }
            case RESPONSE_GROUP_LEFT -> {
                GroupLeft left = new GroupLeft();
                left.groupId = reader.readString();
                left.memberId = reader.readString();
                left.generation = reader.readU64();
                return new ParsedResponse("GroupLeft", left);
            }
            default -> {
                return new ParsedResponse("Unknown(" + variant + ")", payload);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T expect(String expected, ParsedResponse response) {
        if (response.kind.equals(expected)) {
            return (T) response.value;
        }
        if (response.kind.equals("Error")) {
            throw new IllegalStateException("broker error: " + response.value);
        }
        if (response.kind.equals("NotLeader")) {
            throw new IllegalStateException("broker returned NotLeader");
        }
        if (response.kind.equals("RebalanceRequired")) {
            throw new IllegalStateException("consumer group rebalance required");
        }
        throw new IllegalStateException("unexpected response: " + response.kind);
    }

    private static String normalizeAuth(String value) {
        if (value == null) {
            return null;
        }
        value = value.trim();
        return value.isEmpty() ? null : value;
    }

    private static String[] parseAddress(String leaderHint, String fallbackHost, int fallbackPort) {
        if (leaderHint == null || leaderHint.isBlank()) {
            return new String[]{fallbackHost, Integer.toString(fallbackPort)};
        }
        int separator = leaderHint.lastIndexOf(':');
        if (separator <= 0) {
            return new String[]{fallbackHost, Integer.toString(fallbackPort)};
        }
        String host = leaderHint.substring(0, separator);
        String port = leaderHint.substring(separator + 1);
        try {
            Integer.parseInt(port);
            return new String[]{host, port};
        } catch (NumberFormatException ignored) {
            return new String[]{fallbackHost, Integer.toString(fallbackPort)};
        }
    }

    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("LOGOS_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("LOGOS_PORT", "9092"));
        String topic = System.getenv().getOrDefault("LOGOS_TOPIC", "compat");
        String groupId = System.getenv().getOrDefault("LOGOS_GROUP_ID", topic + "-workers");
        String auth = normalizeAuth(System.getenv().getOrDefault("LOGOS_AUTH", "token-a"));
        int maxBytes = Integer.parseInt(System.getenv().getOrDefault("LOGOS_MAX_BYTES", Integer.toString(1024 * 1024)));

        try (Socket bootstrapSocket = new Socket(host, port)) {
            DataOutputStream out = new DataOutputStream(bootstrapSocket.getOutputStream());
            DataInputStream in = new DataInputStream(bootstrapSocket.getInputStream());

            HandshakeOk handshake = expect("HandshakeOk", sendRequest(out, in, buildHandshake((short) 1)));
            System.out.println("handshake ok: server_version=" + handshake.serverVersion);

            Produced produced = expect(
                    "Produced",
                    sendRequest(
                            out,
                            in,
                            buildProduce(
                                    topic,
                                    0,
                                    List.of(
                                            new Rec("k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8), 1L),
                                            new Rec("k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8), 2L)
                                    ),
                                    auth)));
            System.out.println("produced: base_offset=" + produced.baseOffset + " last_offset=" + produced.lastOffset + " acks=" + produced.acks);

            GroupJoined joined = expect("GroupJoined", sendRequest(out, in, buildJoinGroup(groupId, topic, null, auth)));
            System.out.println("group joined: member_id=" + joined.memberId + " generation=" + joined.generation + " assignments=" + joined.assignments.size());

            HeartbeatOk heartbeat = expect("HeartbeatOk", sendRequest(out, in, buildHeartbeat(groupId, topic, joined.memberId, joined.generation, auth)));
            System.out.println("heartbeat ok: generation=" + heartbeat.generation);

            for (ConsumerGroupAssignment assignment : joined.assignments) {
                String[] target = parseAddress(assignment.leaderHint, host, port);
                try (Socket fetchSocket = new Socket(target[0], Integer.parseInt(target[1]))) {
                    DataOutputStream fetchOut = new DataOutputStream(fetchSocket.getOutputStream());
                    DataInputStream fetchIn = new DataInputStream(fetchSocket.getInputStream());

                    expect("HandshakeOk", sendRequest(fetchOut, fetchIn, buildHandshake((short) 1)));
                    List<FetchedRecord> fetched = expect(
                            "Fetched",
                            sendRequest(
                                    fetchOut,
                                    fetchIn,
                                    buildGroupFetch(
                                            groupId,
                                            assignment.topic,
                                            joined.memberId,
                                            joined.generation,
                                            assignment.partition,
                                            assignment.offset,
                                            maxBytes,
                                            auth)));
                    System.out.println("group fetch: partition=" + assignment.partition + " from=" + target[0] + ":" + target[1] + " records=" + fetched.size());
                    if (fetched.isEmpty()) {
                        continue;
                    }
                    for (FetchedRecord record : fetched) {
                        System.out.println("  record: offset=" + record.offset + " key=" + new String(record.key, StandardCharsets.UTF_8) + " value=" + new String(record.value, StandardCharsets.UTF_8) + " timestamp=" + record.timestamp);
                    }
                    long nextOffset = fetched.get(fetched.size() - 1).offset + 1;
                    OffsetCommitted committed = expect(
                            "OffsetCommitted",
                            sendRequest(
                                    out,
                                    in,
                                    buildCommitOffset(
                                            groupId,
                                            assignment.topic,
                                            joined.memberId,
                                            joined.generation,
                                            assignment.partition,
                                            nextOffset,
                                            auth)));
                    System.out.println("offset committed: partition=" + committed.partition + " offset=" + committed.offset);
                }
            }

            GroupLeft left = expect("GroupLeft", sendRequest(out, in, buildLeaveGroup(groupId, topic, joined.memberId, joined.generation, auth)));
            System.out.println("group left: member_id=" + left.memberId + " generation=" + left.generation);
        }
    }
}
