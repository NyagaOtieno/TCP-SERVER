package main

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"time"
)

// Run simulator only when executed explicitly
func main() {
	serverAddr := "switchback.proxy.rlwy.net:15376"

	baseLat := -1.2921
	baseLng := 36.8219

	for i := 0; i < 5; i++ { // send 5 packets
		timestamp := time.Now().UnixMilli()
		lat := baseLat + rand.Float64()*0.01
		lng := baseLng + rand.Float64()*0.01
		speed := rand.Intn(80)

		// Build a fake AVL packet
		packetHex := fmt.Sprintf("%016X0000000000000000%08X%08X%04X0000000000", timestamp, int(lng*1e7), int(lat*1e7), speed)
		data, _ := hex.DecodeString(packetHex)

		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		fmt.Println("✅ Connected, sending packet...")
		conn.Write(data)
		time.Sleep(1 * time.Second)
		fmt.Printf("✅ Packet %d sent\n", i+1)
	}
}
