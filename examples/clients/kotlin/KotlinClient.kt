import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Paths
import java.util.HexFormat

fun load(rel: String): ByteArray = Files.readAllBytes(Paths.get("tests/fixtures/v1/$rel"))

fun sendFrame(out: DataOutputStream, payload: ByteArray) {
    val len = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(payload.size).array()
    out.write(len)
    out.write(payload)
    out.flush()
}

fun recvFrame(`in`: DataInputStream): ByteArray {
    val lenBuf = `in`.readNBytes(4)
    if (lenBuf.size < 4) error("connection closed")
    val len = ByteBuffer.wrap(lenBuf).order(ByteOrder.BIG_ENDIAN).int
    return `in`.readNBytes(len)
}

fun main() {
    val host = System.getenv("LOGOS_HOST") ?: "127.0.0.1"
    val port = (System.getenv("LOGOS_PORT") ?: "9092").toInt()
    Socket(host, port).use { sock ->
        val out = DataOutputStream(sock.getOutputStream())
        val `in` = DataInputStream(sock.getInputStream())

        sendFrame(out, load("handshake_req.bin"))
        val hs = recvFrame(`in`)
        println("handshake resp=${HexFormat.of().formatHex(hs)}")

        sendFrame(out, load("produce_req.bin"))
        val prod = recvFrame(`in`)
        println("produce resp=${HexFormat.of().formatHex(prod)}")
    }
}
