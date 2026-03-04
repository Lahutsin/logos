package main

import (
    "encoding/binary"
    "fmt"
    "io"
    "net"
    "os"
    "path/filepath"
)

func load(rel string) ([]byte, error) {
    root, err := os.Getwd()
    if err != nil {
        return nil, err
    }
    path := filepath.Join(root, "tests", "fixtures", "v1", rel)
    return os.ReadFile(path)
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

    handshake, err := load("handshake_req.bin")
    if err != nil {
        panic(err)
    }
    if err := sendFrame(conn, handshake); err != nil {
        panic(err)
    }
    hsResp, err := recvFrame(conn)
    if err != nil {
        panic(err)
    }
    fmt.Printf("handshake resp=%x\n", hsResp)

    produce, err := load("produce_req.bin")
    if err != nil {
        panic(err)
    }
    if err := sendFrame(conn, produce); err != nil {
        panic(err)
    }
    prodResp, err := recvFrame(conn)
    if err != nil {
        panic(err)
    }
    fmt.Printf("produce resp=%x\n", prodResp)
}
