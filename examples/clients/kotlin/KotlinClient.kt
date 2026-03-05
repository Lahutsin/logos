import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.util.HexFormat

data class Rec(val key: ByteArray, val value: ByteArray, val timestamp: Long)

private fun writeU16LE(out: ByteArrayOutputStream, v: Int) {
    out.writeBytes(ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort(v.toShort()).array())
}

private fun writeU32LE(out: ByteArrayOutputStream, v: Long) {
    out.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v.toInt()).array())
}

private fun writeU64LE(out: ByteArrayOutputStream, v: Long) {
    out.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array())
}

private fun writeI64LE(out: ByteArrayOutputStream, v: Long) = writeU64LE(out, v)

private fun writeString(out: ByteArrayOutputStream, s: String) {
    val b = s.toByteArray(StandardCharsets.UTF_8)
    writeU64LE(out, b.size.toLong())
    out.writeBytes(b)
}

private fun buildHandshake(clientVersion: Short = 1): ByteArray {
    val out = ByteArrayOutputStream()
    writeU32LE(out, 4) // variant index for Handshake
    writeU16LE(out, clientVersion.toInt())
    return out.toByteArray()
}

private fun buildProduce(topic: String, partition: Int, records: List<Rec>, auth: String?): ByteArray {
    require(records.isNotEmpty()) { "records must be non-empty" }

    val out = ByteArrayOutputStream()
    writeU32LE(out, 0) // variant index for Produce

    writeString(out, topic)
    writeU32LE(out, partition.toLong())

    writeU64LE(out, records.size.toLong())
    for (r in records) {
        writeU64LE(out, r.key.size.toLong())
        out.writeBytes(r.key)
        writeU64LE(out, r.value.size.toLong())
        out.writeBytes(r.value)
        writeI64LE(out, r.timestamp)
    }

    if (auth == null) {
        out.write(0) // Option::None
    } else {
        out.write(1) // Option::Some
        writeString(out, auth)
    }

    return out.toByteArray()
}

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

        val hsPayload = buildHandshake()
        sendFrame(out, hsPayload)
        val hs = recvFrame(`in`)
        println("handshake resp=${HexFormat.of().formatHex(hs)}")

        val prodPayload = buildProduce(
            topic = "compat",
            partition = 0,
            records = listOf(
                Rec("k1".toByteArray(StandardCharsets.UTF_8), "v1".toByteArray(StandardCharsets.UTF_8), 1),
                Rec("k2".toByteArray(StandardCharsets.UTF_8), "v2".toByteArray(StandardCharsets.UTF_8), 2),
            ),
            auth = "token-a"
        )
        sendFrame(out, prodPayload)
        val prod = recvFrame(`in`)
        println("produce resp=${HexFormat.of().formatHex(prod)}")
    }
}
