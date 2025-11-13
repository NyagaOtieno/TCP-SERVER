package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"time"
)

func main() {
	// Change to your Railway TCP proxy endpoint
	serverAddr := "switchback.proxy.rlwy.net:15376" 

	// Example simulated AVL packet in hex
	packetHex := "000F31323334353637383930313233343508010000016635F7FBC02A520B00C8000B0F0700000000010002"

	data, err := hex.DecodeString(packetHex)
	if err != nil {
		panic(err)
	}

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("✅ Connected to server, sending simulated packet...")
	_, err = conn.Write(data)
	if err != nil {
		fmt.Println("❌ Failed to send packet:", err)
		return
	}

	// Optional: wait for ACK from server
	ack := make([]byte, 1)
	_, err = conn.Read(ack)
	if err != nil {
		fmt.Println("⚠️ No ACK received:", err)
	} else {
		fmt.Printf("📬 Server ACK: 0x%X\n", ack[0])
	}

	time.Sleep(1 * time.Second)
	fmt.Println("✅ Packet sent successfully.")
}
