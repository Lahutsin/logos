import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

private const val REQUEST_PRODUCE = 0L
private const val REQUEST_HANDSHAKE = 4L
private const val REQUEST_JOIN_GROUP = 5L
private const val REQUEST_HEARTBEAT = 6L
private const val REQUEST_COMMIT_OFFSET = 7L
private const val REQUEST_GROUP_FETCH = 8L
private const val REQUEST_LEAVE_GROUP = 9L

private const val RESPONSE_PRODUCED = 0
private const val RESPONSE_FETCHED = 1
private const val RESPONSE_HANDSHAKE_OK = 3
private const val RESPONSE_NOT_LEADER = 4
private const val RESPONSE_ERROR = 5
private const val RESPONSE_GROUP_JOINED = 6
private const val RESPONSE_HEARTBEAT_OK = 7
private const val RESPONSE_OFFSET_COMMITTED = 8
private const val RESPONSE_REBALANCE_REQUIRED = 9
private const val RESPONSE_GROUP_LEFT = 10

data class Rec(val key: ByteArray, val value: ByteArray, val timestamp: Long)
data class ConsumerGroupAssignment(val topic: String, val partition: Int, val offset: Long, val leaderHint: String?)
data class GroupJoined(
    val groupId: String,
    val memberId: String,
    val generation: Long,
    val heartbeatIntervalMs: Long,
    val sessionTimeoutMs: Long,
    val assignments: List<ConsumerGroupAssignment>,
)
data class FetchedRecord(val offset: Long, val key: ByteArray, val value: ByteArray, val timestamp: Long)
data class Produced(val baseOffset: Long, val lastOffset: Long, val acks: Int)
data class HandshakeOk(val serverVersion: Int)
data class HeartbeatOk(val groupId: String, val memberId: String, val generation: Long)
data class OffsetCommitted(val groupId: String, val memberId: String, val generation: Long, val topic: String, val partition: Int, val offset: Long)
data class GroupLeft(val groupId: String, val memberId: String, val generation: Long)
data class NotLeader(val leader: String?)
data class RebalanceRequired(val groupId: String, val generation: Long)
data class ParsedResponse(val kind: String, val value: Any)

private class Reader(private val data: ByteArray) {
    private var pos = 0

    private fun readExact(size: Int): ByteArray {
        require(pos + size <= data.size) { "unexpected end of response" }
        val out = data.copyOfRange(pos, pos + size)
        pos += size
        return out
    }

    fun readU8(): Int = readExact(1)[0].toInt() and 0xff
    fun readU16(): Int = ByteBuffer.wrap(readExact(2)).order(ByteOrder.LITTLE_ENDIAN).short.toInt() and 0xffff
    fun readU32(): Int = ByteBuffer.wrap(readExact(4)).order(ByteOrder.LITTLE_ENDIAN).int
    fun readU64(): Long = ByteBuffer.wrap(readExact(8)).order(ByteOrder.LITTLE_ENDIAN).long
    fun readI64(): Long = ByteBuffer.wrap(readExact(8)).order(ByteOrder.LITTLE_ENDIAN).long
    fun readBytes(): ByteArray = readExact(readU64().toInt())
    fun readString(): String = readBytes().toString(StandardCharsets.UTF_8)

    fun readOptionalString(): String? {
        return when (val tag = readU8()) {
            0 -> null
            1 -> readString()
            else -> error("unexpected option tag: $tag")
        }
    }
}

private fun writeU16LE(out: ByteArrayOutputStream, value: Int) {
    out.writeBytes(ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort(value.toShort()).array())
}

private fun writeU32LE(out: ByteArrayOutputStream, value: Long) {
    out.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value.toInt()).array())
}

private fun writeU64LE(out: ByteArrayOutputStream, value: Long) {
    out.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array())
}

private fun writeString(out: ByteArrayOutputStream, value: String) {
    val bytes = value.toByteArray(StandardCharsets.UTF_8)
    writeU64LE(out, bytes.size.toLong())
    out.writeBytes(bytes)
}

private fun writeOptionalString(out: ByteArrayOutputStream, value: String?) {
    if (value.isNullOrBlank()) {
        out.write(0)
    } else {
        out.write(1)
        writeString(out, value)
    }
}

private fun buildHandshake(clientVersion: Short = 1): ByteArray {
    val out = ByteArrayOutputStream()
    writeU32LE(out, REQUEST_HANDSHAKE)
    writeU16LE(out, clientVersion.toInt())
    return out.toByteArray()
}

private fun buildProduce(topic: String, partition: Int, records: List<Rec>, auth: String?): ByteArray {
    require(records.isNotEmpty()) { "records must be non-empty" }
    val out = ByteArrayOutputStream()
    writeU32LE(out, REQUEST_PRODUCE)
    writeString(out, topic)
    writeU32LE(out, partition.toLong())
    writeU64LE(out, records.size.toLong())
    for (record in records) {
        writeU64LE(out, record.key.size.toLong())
        out.writeBytes(record.key)
        writeU64LE(out, record.value.size.toLong())
        out.writeBytes(record.value)
        writeU64LE(out, record.timestamp)
    }
    writeOptionalString(out, auth)
    return out.toByteArray()
}

private fun buildJoinGroup(groupId: String, topic: String, memberId: String?, auth: String?): ByteArray {
    val out = ByteArrayOutputStream()
    writeU32LE(out, REQUEST_JOIN_GROUP)
    writeString(out, groupId)
    writeString(out, topic)
    writeOptionalString(out, memberId)
    writeOptionalString(out, auth)
    return out.toByteArray()
}

private fun buildHeartbeat(groupId: String, topic: String, memberId: String, generation: Long, auth: String?): ByteArray {
    val out = ByteArrayOutputStream()
    writeU32LE(out, REQUEST_HEARTBEAT)
    writeString(out, groupId)
    writeString(out, topic)
    writeString(out, memberId)
    writeU64LE(out, generation)
    writeOptionalString(out, auth)
    return out.toByteArray()
}

private fun buildGroupFetch(groupId: String, topic: String, memberId: String, generation: Long, partition: Int, offset: Long, maxBytes: Int, auth: String?): ByteArray {
    val out = ByteArrayOutputStream()
    writeU32LE(out, REQUEST_GROUP_FETCH)
    writeString(out, groupId)
    writeString(out, topic)
    writeString(out, memberId)
    writeU64LE(out, generation)
    writeU32LE(out, partition.toLong())
    writeU64LE(out, offset)
    writeU32LE(out, maxBytes.toLong())
    writeOptionalString(out, auth)
    return out.toByteArray()
}

private fun buildCommitOffset(groupId: String, topic: String, memberId: String, generation: Long, partition: Int, offset: Long, auth: String?): ByteArray {
    val out = ByteArrayOutputStream()
    writeU32LE(out, REQUEST_COMMIT_OFFSET)
    writeString(out, groupId)
    writeString(out, topic)
    writeString(out, memberId)
    writeU64LE(out, generation)
    writeU32LE(out, partition.toLong())
    writeU64LE(out, offset)
    writeOptionalString(out, auth)
    return out.toByteArray()
}

private fun buildLeaveGroup(groupId: String, topic: String, memberId: String, generation: Long, auth: String?): ByteArray {
    val out = ByteArrayOutputStream()
    writeU32LE(out, REQUEST_LEAVE_GROUP)
    writeString(out, groupId)
    writeString(out, topic)
    writeString(out, memberId)
    writeU64LE(out, generation)
    writeOptionalString(out, auth)
    return out.toByteArray()
}

private fun sendFrame(out: DataOutputStream, payload: ByteArray) {
    val len = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(payload.size).array()
    out.write(len)
    out.write(payload)
    out.flush()
}

private fun recvFrame(input: DataInputStream): ByteArray {
    val lenBuf = input.readNBytes(4)
    require(lenBuf.size == 4) { "connection closed" }
    val len = ByteBuffer.wrap(lenBuf).order(ByteOrder.BIG_ENDIAN).int
    val payload = input.readNBytes(len)
    require(payload.size == len) { "connection closed mid-frame" }
    return payload
}

private fun sendRequest(out: DataOutputStream, input: DataInputStream, payload: ByteArray): ParsedResponse {
    sendFrame(out, payload)
    return parseResponse(recvFrame(input))
}

private fun parseResponse(payload: ByteArray): ParsedResponse {
    val reader = Reader(payload)
    return when (val variant = reader.readU32()) {
        RESPONSE_PRODUCED -> ParsedResponse("Produced", Produced(reader.readU64(), reader.readU64(), reader.readU32()))
        RESPONSE_FETCHED -> {
            val count = reader.readU64().toInt()
            val records = MutableList(count) {
                FetchedRecord(reader.readU64(), reader.readBytes(), reader.readBytes(), reader.readI64())
            }
            ParsedResponse("Fetched", records)
        }
        RESPONSE_HANDSHAKE_OK -> ParsedResponse("HandshakeOk", HandshakeOk(reader.readU16()))
        RESPONSE_NOT_LEADER -> ParsedResponse("NotLeader", NotLeader(reader.readOptionalString()))
        RESPONSE_ERROR -> ParsedResponse("Error", reader.readString())
        RESPONSE_GROUP_JOINED -> {
            val groupId = reader.readString()
            val memberId = reader.readString()
            val generation = reader.readU64()
            val heartbeatIntervalMs = reader.readU64()
            val sessionTimeoutMs = reader.readU64()
            val count = reader.readU64().toInt()
            val assignments = MutableList(count) {
                ConsumerGroupAssignment(reader.readString(), reader.readU32(), reader.readU64(), reader.readOptionalString())
            }
            ParsedResponse("GroupJoined", GroupJoined(groupId, memberId, generation, heartbeatIntervalMs, sessionTimeoutMs, assignments))
        }
        RESPONSE_HEARTBEAT_OK -> ParsedResponse("HeartbeatOk", HeartbeatOk(reader.readString(), reader.readString(), reader.readU64()))
        RESPONSE_OFFSET_COMMITTED -> ParsedResponse("OffsetCommitted", OffsetCommitted(reader.readString(), reader.readString(), reader.readU64(), reader.readString(), reader.readU32(), reader.readU64()))
        RESPONSE_REBALANCE_REQUIRED -> ParsedResponse("RebalanceRequired", RebalanceRequired(reader.readString(), reader.readU64()))
        RESPONSE_GROUP_LEFT -> ParsedResponse("GroupLeft", GroupLeft(reader.readString(), reader.readString(), reader.readU64()))
        else -> ParsedResponse("Unknown($variant)", payload)
    }
}

private inline fun <reified T> expect(expected: String, response: ParsedResponse): T {
    return when (response.kind) {
        expected -> response.value as T
        "Error" -> error("broker error: ${response.value}")
        "NotLeader" -> error("broker returned NotLeader")
        "RebalanceRequired" -> error("consumer group rebalance required")
        else -> error("unexpected response: ${response.kind}")
    }
}

private fun normalizeAuth(value: String?): String? = value?.trim()?.takeIf { it.isNotEmpty() }

private fun parseAddress(leaderHint: String?, fallbackHost: String, fallbackPort: Int): Pair<String, Int> {
    if (leaderHint.isNullOrBlank()) {
        return fallbackHost to fallbackPort
    }
    val separator = leaderHint.lastIndexOf(':')
    if (separator <= 0) {
        return fallbackHost to fallbackPort
    }
    val host = leaderHint.substring(0, separator)
    val port = leaderHint.substring(separator + 1).toIntOrNull() ?: return fallbackHost to fallbackPort
    return host to port
}

fun main() {
    val host = System.getenv("LOGOS_HOST") ?: "127.0.0.1"
    val port = (System.getenv("LOGOS_PORT") ?: "9092").toInt()
    val topic = System.getenv("LOGOS_TOPIC") ?: "compat"
    val groupId = System.getenv("LOGOS_GROUP_ID") ?: "$topic-workers"
    val auth = normalizeAuth(System.getenv("LOGOS_AUTH") ?: "token-a")
    val maxBytes = (System.getenv("LOGOS_MAX_BYTES") ?: "${1024 * 1024}").toInt()

    Socket(host, port).use { bootstrapSocket ->
        val out = DataOutputStream(bootstrapSocket.getOutputStream())
        val input = DataInputStream(bootstrapSocket.getInputStream())

        val handshake = expect<HandshakeOk>("HandshakeOk", sendRequest(out, input, buildHandshake()))
        println("handshake ok: server_version=${handshake.serverVersion}")

        val produced = expect<Produced>(
            "Produced",
            sendRequest(
                out,
                input,
                buildProduce(
                    topic = topic,
                    partition = 0,
                    records = listOf(
                        Rec("k1".toByteArray(StandardCharsets.UTF_8), "v1".toByteArray(StandardCharsets.UTF_8), 1),
                        Rec("k2".toByteArray(StandardCharsets.UTF_8), "v2".toByteArray(StandardCharsets.UTF_8), 2),
                    ),
                    auth = auth,
                ),
            ),
        )
        println("produced: base_offset=${produced.baseOffset} last_offset=${produced.lastOffset} acks=${produced.acks}")

        val joined = expect<GroupJoined>("GroupJoined", sendRequest(out, input, buildJoinGroup(groupId, topic, null, auth)))
        println("group joined: member_id=${joined.memberId} generation=${joined.generation} assignments=${joined.assignments.size}")

        val heartbeat = expect<HeartbeatOk>("HeartbeatOk", sendRequest(out, input, buildHeartbeat(groupId, topic, joined.memberId, joined.generation, auth)))
        println("heartbeat ok: generation=${heartbeat.generation}")

        for (assignment in joined.assignments) {
            val (fetchHost, fetchPort) = parseAddress(assignment.leaderHint, host, port)
            Socket(fetchHost, fetchPort).use { fetchSocket ->
                val fetchOut = DataOutputStream(fetchSocket.getOutputStream())
                val fetchIn = DataInputStream(fetchSocket.getInputStream())

                expect<HandshakeOk>("HandshakeOk", sendRequest(fetchOut, fetchIn, buildHandshake()))
                val fetched = expect<List<FetchedRecord>>(
                    "Fetched",
                    sendRequest(
                        fetchOut,
                        fetchIn,
                        buildGroupFetch(groupId, assignment.topic, joined.memberId, joined.generation, assignment.partition, assignment.offset, maxBytes, auth),
                    ),
                )
                println("group fetch: partition=${assignment.partition} from=$fetchHost:$fetchPort records=${fetched.size}")
                if (fetched.isEmpty()) {
                    continue
                }
                fetched.forEach { record ->
                    println(
                        "  record: offset=${record.offset} key=${record.key.toString(StandardCharsets.UTF_8)} " +
                            "value=${record.value.toString(StandardCharsets.UTF_8)} timestamp=${record.timestamp}"
                    )
                }
                val nextOffset = fetched.last().offset + 1
                val committed = expect<OffsetCommitted>(
                    "OffsetCommitted",
                    sendRequest(
                        out,
                        input,
                        buildCommitOffset(groupId, assignment.topic, joined.memberId, joined.generation, assignment.partition, nextOffset, auth),
                    ),
                )
                println("offset committed: partition=${committed.partition} offset=${committed.offset}")
            }
        }

        val left = expect<GroupLeft>("GroupLeft", sendRequest(out, input, buildLeaveGroup(groupId, topic, joined.memberId, joined.generation, auth)))
        println("group left: member_id=${left.memberId} generation=${left.generation}")
    }
}
