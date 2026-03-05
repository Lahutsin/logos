package main

import (
    "bytes"
    "encoding/binary"
    "fmt"
    "io"
    "net"
    "os"
)

type record struct {
    key       []byte
    value     []byte
    timestamp int64
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

func writeString(buf *bytes.Buffer, s string) {
    b := []byte(s)
    binary.Write(buf, binary.LittleEndian, uint64(len(b)))
    buf.Write(b)
}

func buildHandshake(clientVersion uint16) []byte {
    var buf bytes.Buffer
    binary.Write(&buf, binary.LittleEndian, uint32(4)) // variant index for Handshake
    binary.Write(&buf, binary.LittleEndian, clientVersion)
    return buf.Bytes()
}

func buildProduce(topic string, partition uint32, records []record, auth *string) []byte {
    if len(records) == 0 {
        panic("records must be non-empty")
    }

    var buf bytes.Buffer
    binary.Write(&buf, binary.LittleEndian, uint32(0)) // variant index for Produce

    writeString(&buf, topic)
    binary.Write(&buf, binary.LittleEndian, partition)

    binary.Write(&buf, binary.LittleEndian, uint64(len(records)))
    for _, r := range records {
        binary.Write(&buf, binary.LittleEndian, uint64(len(r.key)))
        buf.Write(r.key)
        binary.Write(&buf, binary.LittleEndian, uint64(len(r.value)))
        buf.Write(r.value)
        binary.Write(&buf, binary.LittleEndian, r.timestamp)
    }

    if auth == nil {
        buf.WriteByte(0) // Option::None
    } else {
        buf.WriteByte(1) // Option::Some
        writeString(&buf, *auth)
    }

    return buf.Bytes()
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

    conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
    if err != nil {
        panic(err)
    }
    defer conn.Close()

    handshake := buildHandshake(1)
    if err := sendFrame(conn, handshake); err != nil {
        panic(err)
    }
    hsResp, err := recvFrame(conn)
    if err != nil {
        panic(err)
    }
    fmt.Printf("handshake resp=%x\n", hsResp)

    auth := "token-a"
    produce := buildProduce("compat", 0, []record{
        {key: []byte("k1"), value: []byte("v1"), timestamp: 1},
        {key: []byte("k2"), value: []byte("v2"), timestamp: 2},
    }, &auth)
    if err := sendFrame(conn, produce); err != nil {
        panic(err)
    }
    prodResp, err := recvFrame(conn)
    if err != nil {
        panic(err)
    }
    fmt.Printf("produce resp=%x\n", prodResp)
}
