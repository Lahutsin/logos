package main

import (
    "bytes"
    "encoding/binary"
    "fmt"
    "io"
    "net"
    "os"
    "strconv"
)

const (
    requestProduce      uint32 = 0
    requestHandshake    uint32 = 4
    requestJoinGroup    uint32 = 5
    requestHeartbeat    uint32 = 6
    requestCommitOffset uint32 = 7
    requestGroupFetch   uint32 = 8
    requestLeaveGroup   uint32 = 9

    responseProduced          uint32 = 0
    responseFetched           uint32 = 1
    responseHandshakeOK       uint32 = 3
    responseNotLeader         uint32 = 4
    responseError             uint32 = 5
    responseGroupJoined       uint32 = 6
    responseHeartbeatOK       uint32 = 7
    responseOffsetCommitted   uint32 = 8
    responseRebalanceRequired uint32 = 9
    responseGroupLeft         uint32 = 10
)

type record struct {
    key       []byte
    value     []byte
    timestamp int64
}

type consumerGroupAssignment struct {
    Topic      string
    Partition  uint32
    Offset     uint64
    LeaderHint *string
}

type groupJoined struct {
    GroupID             string
    MemberID            string
    Generation          uint64
    HeartbeatIntervalMS uint64
    SessionTimeoutMS    uint64
    Assignments         []consumerGroupAssignment
}

type fetchedRecord struct {
    Offset    uint64
    Key       []byte
    Value     []byte
    Timestamp int64
}

type produced struct {
    BaseOffset uint64
    LastOffset uint64
    Acks       uint32
}

type handshakeOK struct {
    ServerVersion uint16
}

type heartbeatOK struct {
    GroupID    string
    MemberID   string
    Generation uint64
}

type offsetCommitted struct {
    GroupID    string
    MemberID   string
    Generation uint64
    Topic      string
    Partition  uint32
    Offset     uint64
}

type groupLeft struct {
    GroupID    string
    MemberID   string
    Generation uint64
}

type notLeader struct {
    Leader *string
}

type rebalanceRequired struct {
    GroupID    string
    Generation uint64
}

func sendFrame(conn net.Conn, payload []byte) error {
    var lenBuf [4]byte
    binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
    if _, err := conn.Write(lenBuf[:]); err != nil {
        return err
    }
    if _, err := conn.Write(payload); err != nil {
        return err
    }
    return nil
}

func recvFrame(conn net.Conn) ([]byte, error) {
    var lenBuf [4]byte
    if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
        return nil, err
    }
    length := binary.BigEndian.Uint32(lenBuf[:])
    buf := make([]byte, length)
    if _, err := io.ReadFull(conn, buf); err != nil {
        return nil, err
    }
    return buf, nil
}

func sendRequest(conn net.Conn, payload []byte) (string, any, error) {
    if err := sendFrame(conn, payload); err != nil {
        return "", nil, err
    }
    frame, err := recvFrame(conn)
    if err != nil {
        return "", nil, err
    }
    return decodeResponse(frame)
}

func writeString(buf *bytes.Buffer, value string) {
    data := []byte(value)
    _ = binary.Write(buf, binary.LittleEndian, uint64(len(data)))
    _, _ = buf.Write(data)
}

func writeOptString(buf *bytes.Buffer, value *string) {
    if value == nil {
        _ = buf.WriteByte(0)
        return
    }
    _ = buf.WriteByte(1)
    writeString(buf, *value)
}

func buildHandshake(clientVersion uint16) []byte {
    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, requestHandshake)
    _ = binary.Write(&buf, binary.LittleEndian, clientVersion)
    return buf.Bytes()
}

func buildProduce(topic string, partition uint32, records []record, auth *string) []byte {
    if len(records) == 0 {
        panic("records must be non-empty")
    }

    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, requestProduce)
    writeString(&buf, topic)
    _ = binary.Write(&buf, binary.LittleEndian, partition)
    _ = binary.Write(&buf, binary.LittleEndian, uint64(len(records)))
    for _, item := range records {
        _ = binary.Write(&buf, binary.LittleEndian, uint64(len(item.key)))
        _, _ = buf.Write(item.key)
        _ = binary.Write(&buf, binary.LittleEndian, uint64(len(item.value)))
        _, _ = buf.Write(item.value)
        _ = binary.Write(&buf, binary.LittleEndian, item.timestamp)
    }
    writeOptString(&buf, auth)
    return buf.Bytes()
}

func buildJoinGroup(groupID string, topic string, memberID *string, auth *string) []byte {
    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, requestJoinGroup)
    writeString(&buf, groupID)
    writeString(&buf, topic)
    writeOptString(&buf, memberID)
    writeOptString(&buf, auth)
    return buf.Bytes()
}

func buildHeartbeat(groupID string, topic string, memberID string, generation uint64, auth *string) []byte {
    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, requestHeartbeat)
    writeString(&buf, groupID)
    writeString(&buf, topic)
    writeString(&buf, memberID)
    _ = binary.Write(&buf, binary.LittleEndian, generation)
    writeOptString(&buf, auth)
    return buf.Bytes()
}

func buildGroupFetch(groupID string, topic string, memberID string, generation uint64, partition uint32, offset uint64, maxBytes uint32, auth *string) []byte {
    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, requestGroupFetch)
    writeString(&buf, groupID)
    writeString(&buf, topic)
    writeString(&buf, memberID)
    _ = binary.Write(&buf, binary.LittleEndian, generation)
    _ = binary.Write(&buf, binary.LittleEndian, partition)
    _ = binary.Write(&buf, binary.LittleEndian, offset)
    _ = binary.Write(&buf, binary.LittleEndian, maxBytes)
    writeOptString(&buf, auth)
    return buf.Bytes()
}

func buildCommitOffset(groupID string, topic string, memberID string, generation uint64, partition uint32, offset uint64, auth *string) []byte {
    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, requestCommitOffset)
    writeString(&buf, groupID)
    writeString(&buf, topic)
    writeString(&buf, memberID)
    _ = binary.Write(&buf, binary.LittleEndian, generation)
    _ = binary.Write(&buf, binary.LittleEndian, partition)
    _ = binary.Write(&buf, binary.LittleEndian, offset)
    writeOptString(&buf, auth)
    return buf.Bytes()
}

func buildLeaveGroup(groupID string, topic string, memberID string, generation uint64, auth *string) []byte {
    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, requestLeaveGroup)
    writeString(&buf, groupID)
    writeString(&buf, topic)
    writeString(&buf, memberID)
    _ = binary.Write(&buf, binary.LittleEndian, generation)
    writeOptString(&buf, auth)
    return buf.Bytes()
}

func readU8(reader *bytes.Reader) (uint8, error) {
    var value uint8
    err := binary.Read(reader, binary.LittleEndian, &value)
    return value, err
}

func readU16(reader *bytes.Reader) (uint16, error) {
    var value uint16
    err := binary.Read(reader, binary.LittleEndian, &value)
    return value, err
}

func readU32(reader *bytes.Reader) (uint32, error) {
    var value uint32
    err := binary.Read(reader, binary.LittleEndian, &value)
    return value, err
}

func readU64(reader *bytes.Reader) (uint64, error) {
    var value uint64
    err := binary.Read(reader, binary.LittleEndian, &value)
    return value, err
}

func readI64(reader *bytes.Reader) (int64, error) {
    var value int64
    err := binary.Read(reader, binary.LittleEndian, &value)
    return value, err
}

func readBytes(reader *bytes.Reader) ([]byte, error) {
    length, err := readU64(reader)
    if err != nil {
        return nil, err
    }
    data := make([]byte, length)
    _, err = io.ReadFull(reader, data)
    return data, err
}

func readString(reader *bytes.Reader) (string, error) {
    data, err := readBytes(reader)
    if err != nil {
        return "", err
    }
    return string(data), nil
}

func readOptString(reader *bytes.Reader) (*string, error) {
    tag, err := readU8(reader)
    if err != nil {
        return nil, err
    }
    if tag == 0 {
        return nil, nil
    }
    if tag != 1 {
        return nil, fmt.Errorf("unexpected option tag: %d", tag)
    }
    value, err := readString(reader)
    if err != nil {
        return nil, err
    }
    return &value, nil
}

func decodeResponse(payload []byte) (string, any, error) {
    reader := bytes.NewReader(payload)
    variant, err := readU32(reader)
    if err != nil {
        return "", nil, err
    }

    switch variant {
    case responseProduced:
        baseOffset, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        lastOffset, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        acks, err := readU32(reader)
        if err != nil {
            return "", nil, err
        }
        return "Produced", produced{BaseOffset: baseOffset, LastOffset: lastOffset, Acks: acks}, nil
    case responseFetched:
        count, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        records := make([]fetchedRecord, 0, count)
        for i := uint64(0); i < count; i++ {
            offset, err := readU64(reader)
            if err != nil {
                return "", nil, err
            }
            key, err := readBytes(reader)
            if err != nil {
                return "", nil, err
            }
            value, err := readBytes(reader)
            if err != nil {
                return "", nil, err
            }
            timestamp, err := readI64(reader)
            if err != nil {
                return "", nil, err
            }
            records = append(records, fetchedRecord{Offset: offset, Key: key, Value: value, Timestamp: timestamp})
        }
        return "Fetched", records, nil
    case responseHandshakeOK:
        serverVersion, err := readU16(reader)
        if err != nil {
            return "", nil, err
        }
        return "HandshakeOk", handshakeOK{ServerVersion: serverVersion}, nil
    case responseNotLeader:
        leader, err := readOptString(reader)
        if err != nil {
            return "", nil, err
        }
        return "NotLeader", notLeader{Leader: leader}, nil
    case responseError:
        message, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        return "Error", message, nil
    case responseGroupJoined:
        groupID, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        memberID, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        generation, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        heartbeatIntervalMS, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        sessionTimeoutMS, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        assignmentsCount, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        assignments := make([]consumerGroupAssignment, 0, assignmentsCount)
        for i := uint64(0); i < assignmentsCount; i++ {
            topic, err := readString(reader)
            if err != nil {
                return "", nil, err
            }
            partition, err := readU32(reader)
            if err != nil {
                return "", nil, err
            }
            offset, err := readU64(reader)
            if err != nil {
                return "", nil, err
            }
            leaderHint, err := readOptString(reader)
            if err != nil {
                return "", nil, err
            }
            assignments = append(assignments, consumerGroupAssignment{Topic: topic, Partition: partition, Offset: offset, LeaderHint: leaderHint})
        }
        return "GroupJoined", groupJoined{GroupID: groupID, MemberID: memberID, Generation: generation, HeartbeatIntervalMS: heartbeatIntervalMS, SessionTimeoutMS: sessionTimeoutMS, Assignments: assignments}, nil
    case responseHeartbeatOK:
        groupID, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        memberID, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        generation, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        return "HeartbeatOk", heartbeatOK{GroupID: groupID, MemberID: memberID, Generation: generation}, nil
    case responseOffsetCommitted:
        groupID, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        memberID, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        generation, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        topic, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        partition, err := readU32(reader)
        if err != nil {
            return "", nil, err
        }
        offset, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        return "OffsetCommitted", offsetCommitted{GroupID: groupID, MemberID: memberID, Generation: generation, Topic: topic, Partition: partition, Offset: offset}, nil
    case responseRebalanceRequired:
        groupID, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        generation, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        return "RebalanceRequired", rebalanceRequired{GroupID: groupID, Generation: generation}, nil
    case responseGroupLeft:
        groupID, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        memberID, err := readString(reader)
        if err != nil {
            return "", nil, err
        }
        generation, err := readU64(reader)
        if err != nil {
            return "", nil, err
        }
        return "GroupLeft", groupLeft{GroupID: groupID, MemberID: memberID, Generation: generation}, nil
    default:
        return fmt.Sprintf("Unknown(%d)", variant), payload, nil
    }
}

func expectResponse(expected string, actual string, value any) (any, error) {
    if actual == expected {
        return value, nil
    }

    switch actual {
    case "Error":
        return nil, fmt.Errorf("broker error: %v", value)
    case "NotLeader":
        return nil, fmt.Errorf("broker returned not leader: %+v", value)
    case "RebalanceRequired":
        return nil, fmt.Errorf("consumer group rebalance required: %+v", value)
    default:
        return nil, fmt.Errorf("unexpected response: %s %+v", actual, value)
    }
}

func normalizeAuth(value string) *string {
    if value == "" {
        return nil
    }
    return &value
}

func parseAddress(leaderHint *string, fallbackHost string, fallbackPort string) (string, string) {
    if leaderHint == nil || *leaderHint == "" {
        return fallbackHost, fallbackPort
    }
    host, port, err := net.SplitHostPort(*leaderHint)
    if err != nil || host == "" || port == "" {
        return fallbackHost, fallbackPort
    }
    return host, port
}

func main() {
    host := os.Getenv("LOGOS_HOST")
    if host == "" {
        host = "127.0.0.1"
    }
    port := os.Getenv("LOGOS_PORT")
    if port == "" {
        port = "9092"
    }
    topic := os.Getenv("LOGOS_TOPIC")
    if topic == "" {
        topic = "compat"
    }
    groupID := os.Getenv("LOGOS_GROUP_ID")
    if groupID == "" {
        groupID = topic + "-workers"
    }
    auth := normalizeAuth(os.Getenv("LOGOS_AUTH"))
    if auth == nil {
        auth = normalizeAuth("token-a")
    }
    maxBytes := uint32(1024 * 1024)
    if value := os.Getenv("LOGOS_MAX_BYTES"); value != "" {
        parsed, err := strconv.ParseUint(value, 10, 32)
        if err != nil {
            panic(err)
        }
        maxBytes = uint32(parsed)
    }

    bootstrap, err := net.Dial("tcp", net.JoinHostPort(host, port))
    if err != nil {
        panic(err)
    }
    defer bootstrap.Close()

    kind, value, err := sendRequest(bootstrap, buildHandshake(1))
    if err != nil {
        panic(err)
    }
    expected, err := expectResponse("HandshakeOk", kind, value)
    if err != nil {
        panic(err)
    }
    handshake := expected.(handshakeOK)
    fmt.Printf("handshake ok: server_version=%d\n", handshake.ServerVersion)

    kind, value, err = sendRequest(bootstrap, buildProduce(topic, 0, []record{
        {key: []byte("k1"), value: []byte("v1"), timestamp: 1},
        {key: []byte("k2"), value: []byte("v2"), timestamp: 2},
    }, auth))
    if err != nil {
        panic(err)
    }
    expected, err = expectResponse("Produced", kind, value)
    if err != nil {
        panic(err)
    }
    producedResp := expected.(produced)
    fmt.Printf("produced: base_offset=%d last_offset=%d acks=%d\n", producedResp.BaseOffset, producedResp.LastOffset, producedResp.Acks)

    kind, value, err = sendRequest(bootstrap, buildJoinGroup(groupID, topic, nil, auth))
    if err != nil {
        panic(err)
    }
    expected, err = expectResponse("GroupJoined", kind, value)
    if err != nil {
        panic(err)
    }
    joined := expected.(groupJoined)
    fmt.Printf("group joined: member_id=%s generation=%d assignments=%d\n", joined.MemberID, joined.Generation, len(joined.Assignments))

    kind, value, err = sendRequest(bootstrap, buildHeartbeat(groupID, topic, joined.MemberID, joined.Generation, auth))
    if err != nil {
        panic(err)
    }
    expected, err = expectResponse("HeartbeatOk", kind, value)
    if err != nil {
        panic(err)
    }
    heartbeatResp := expected.(heartbeatOK)
    fmt.Printf("heartbeat ok: generation=%d\n", heartbeatResp.Generation)

    for _, assignment := range joined.Assignments {
        fetchHost, fetchPort := parseAddress(assignment.LeaderHint, host, port)
        fetchConn, err := net.Dial("tcp", net.JoinHostPort(fetchHost, fetchPort))
        if err != nil {
            panic(err)
        }

        kind, value, err = sendRequest(fetchConn, buildHandshake(1))
        if err != nil {
            _ = fetchConn.Close()
            panic(err)
        }
        _, err = expectResponse("HandshakeOk", kind, value)
        if err != nil {
            _ = fetchConn.Close()
            panic(err)
        }

        kind, value, err = sendRequest(fetchConn, buildGroupFetch(groupID, assignment.Topic, joined.MemberID, joined.Generation, assignment.Partition, assignment.Offset, maxBytes, auth))
        _ = fetchConn.Close()
        if err != nil {
            panic(err)
        }
        expected, err = expectResponse("Fetched", kind, value)
        if err != nil {
            panic(err)
        }
        fetched := expected.([]fetchedRecord)
        fmt.Printf("group fetch: partition=%d from=%s:%s records=%d\n", assignment.Partition, fetchHost, fetchPort, len(fetched))
        if len(fetched) == 0 {
            continue
        }
        for _, item := range fetched {
            fmt.Printf("  record: offset=%d key=%s value=%s timestamp=%d\n", item.Offset, string(item.Key), string(item.Value), item.Timestamp)
        }
        nextOffset := fetched[len(fetched)-1].Offset + 1
        kind, value, err = sendRequest(bootstrap, buildCommitOffset(groupID, assignment.Topic, joined.MemberID, joined.Generation, assignment.Partition, nextOffset, auth))
        if err != nil {
            panic(err)
        }
        expected, err = expectResponse("OffsetCommitted", kind, value)
        if err != nil {
            panic(err)
        }
        committed := expected.(offsetCommitted)
        fmt.Printf("offset committed: partition=%d offset=%d\n", committed.Partition, committed.Offset)
    }

    kind, value, err = sendRequest(bootstrap, buildLeaveGroup(groupID, topic, joined.MemberID, joined.Generation, auth))
    if err != nil {
        panic(err)
    }
    expected, err = expectResponse("GroupLeft", kind, value)
    if err != nil {
        panic(err)
    }
    left := expected.(groupLeft)
    fmt.Printf("group left: member_id=%s generation=%d\n", left.MemberID, left.Generation)
}
