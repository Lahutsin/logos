import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HexFormat;

public class Client {
    private static byte[] load(String relPath) throws IOException {
        return Files.readAllBytes(Paths.get("tests/fixtures/v1/" + relPath));
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

            sendFrame(out, load("handshake_req.bin"));
            byte[] handshakeResp = recvFrame(in);
            System.out.println("handshake resp=" + HexFormat.of().formatHex(handshakeResp));

            sendFrame(out, load("produce_req.bin"));
            byte[] producedResp = recvFrame(in);
            System.out.println("produce resp=" + HexFormat.of().formatHex(producedResp));
        }
    }
}
