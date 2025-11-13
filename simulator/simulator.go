package main

import (
    "encoding/hex"
    "fmt"
    "net"
    "time"
)

func RunSimulator() {
    serverAddr := "switchback.proxy.rlwy.net:15376"

    // Simulate multiple AVL packets with different positions/timestamps
    packets := []string{
        "000F31323334353637383930313233343508010000016635F7FBC02A520B00C8000B0F0700000000010002",
        "000F31323334353637383930313233343508010000016635F7FBC02A521B00C9000B0F0800000000010002",
        "000F31323334353637383930313233343508010000016635F7FBC02A522B00CA000B0F0900000000010002",
    }

    conn, err := net.Dial("tcp", serverAddr)
    if err != nil {
        panic(err)
    }
    defer conn.Close()

    for i, packetHex := range packets {
        data, _ := hex.DecodeString(packetHex)
        fmt.Printf("✅ Sending packet %d\n", i+1)
        conn.Write(data)

        // Optional: read ACK
        ack := make([]byte, 1)
        conn.Read(ack)
        fmt.Printf("📬 Server ACK: 0x%X\n", ack[0])

        time.Sleep(1 * time.Second)
    }

    fmt.Println("✅ All simulated packets sent successfully.")
}

func main() {
    RunSimulator()
}
