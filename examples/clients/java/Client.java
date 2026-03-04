import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

public class Client {
    private static final class Rec {
        final byte[] key;
        final byte[] value;
        final long ts;

        Rec(byte[] key, byte[] value, long ts) {
            this.key = key;
            this.value = value;
            this.ts = ts;
        }
    }

    private static void writeU16LE(ByteArrayOutputStream out, int v) {
        out.writeBytes(ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) v).array());
    }

    private static void writeU32LE(ByteArrayOutputStream out, long v) {
        out.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt((int) v).array());
    }

    private static void writeU64LE(ByteArrayOutputStream out, long v) {
        out.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array());
    }

    private static void writeI64LE(ByteArrayOutputStream out, long v) {
        writeU64LE(out, v);
    }

    private static void writeString(ByteArrayOutputStream out, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        writeU64LE(out, b.length);
        out.writeBytes(b);
    }

    private static byte[] buildHandshake(short clientVersion) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32LE(out, 4); // variant index for Handshake
        writeU16LE(out, clientVersion);
        return out.toByteArray();
    }

    private static byte[] buildProduce(String topic, int partition, List<Rec> records, String auth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32LE(out, 0); // variant index for Produce

        writeString(out, topic);
        writeU32LE(out, partition);

        writeU64LE(out, records.size());
        for (Rec r : records) {
            writeU64LE(out, r.key.length);
            out.writeBytes(r.key);
            writeU64LE(out, r.value.length);
            out.writeBytes(r.value);
            writeI64LE(out, r.ts);
        }

        if (auth == null) {
            out.write(0); // Option::None
        } else {
            out.write(1); // Option::Some
            writeString(out, auth);
        }

        return out.toByteArray();
    }

    private static void sendFrame(DataOutputStream out, byte[] payload) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(payload.length);
        out.write(buf.array());
        out.write(payload);
        out.flush();
    }

    private static byte[] recvFrame(DataInputStream in) throws IOException {
        byte[] lenBuf = in.readNBytes(4);
        if (lenBuf.length < 4) throw new IOException("connection closed");
        int len = ByteBuffer.wrap(lenBuf).order(ByteOrder.BIG_ENDIAN).getInt();
        return in.readNBytes(len);
    }

    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("LOGOS_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("LOGOS_PORT", "9092"));

        try (Socket sock = new Socket(host, port)) {
            DataOutputStream out = new DataOutputStream(sock.getOutputStream());
            DataInputStream in = new DataInputStream(sock.getInputStream());

            byte[] hsPayload = buildHandshake((short) 1);
            sendFrame(out, hsPayload);
            byte[] handshakeResp = recvFrame(in);
            System.out.println("handshake resp=" + HexFormat.of().formatHex(handshakeResp));

            byte[] producePayload = buildProduce(
                    "compat",
                    0,
                    List.of(
                            new Rec("k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8), 1L),
                            new Rec("k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8), 2L)
                    ),
                    "token-a");
            sendFrame(out, producePayload);
            byte[] producedResp = recvFrame(in);
            System.out.println("produce resp=" + HexFormat.of().formatHex(producedResp));
        }
    }
}
