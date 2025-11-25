package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"net"
	"time"
)

// Devices to simulate
var devices = []struct {
	IMEI    string
	BaseLat float64
	BaseLng float64
}{
	{"353691849836001", -1.2921, 36.8219},
	{"123456789211223873", -1.3032, 36.8148},
}

func main() {
	serverHost := "127.0.0.1"
	serverPort := 5030 // match your running TCP server port
	serverAddr := fmt.Sprintf("%s:%d", serverHost, serverPort)

	for _, d := range devices {
		fmt.Printf("📡 Simulating device %s\n", d.IMEI)

		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			fmt.Println("❌ Failed to connect:", err)
			continue
		}

		// 1️⃣ Send IMEI handshake
		imeiPacket := append([]byte{0x0F}, []byte(d.IMEI)...)
		conn.Write(imeiPacket)

		// Wait for ACK (optional)
		ack := make([]byte, 1)
		_, err = conn.Read(ack)
		if err != nil {
			fmt.Println("❌ Failed to read ACK:", err)
			conn.Close()
			continue
		}
		fmt.Println("✅ IMEI sent and ACK received")

		// 2️⃣ Send multiple AVL packets
		for i := 0; i < 5; i++ {
			packet := createFMB920AVLPacket(d.BaseLat, d.BaseLng)
			conn.Write(packet)

			// Optional backend ACK
			_, _ = conn.Read(ack)

			// Print simulation log
			fmt.Printf("🔎 Parsed 1 AVL record(s) for %s\n", d.IMEI)
			fmt.Println("📬 Backend response (200): {\"success\":true}")
			time.Sleep(1 * time.Second)
		}

		conn.Close()
		fmt.Printf("✅ Simulation completed for %s\n\n", d.IMEI)
	}
}

// createFMB920AVLPacket generates a realistic FMB920 AVL packet
func createFMB920AVLPacket(baseLat, baseLng float64) []byte {
	payload := new(bytes.Buffer)

	// Codec ID
	payload.WriteByte(0x08)
	payload.WriteByte(0x01) // 1 record

	// AVL Record
	timestamp := time.Now().Unix()
	binary.Write(payload, binary.BigEndian, uint64(timestamp))
	payload.WriteByte(0x0A) // priority

	lon := int32(baseLng*1e7) + int32(rand.Int31n(10000)-5000)
	lat := int32(baseLat*1e7) + int32(rand.Int31n(10000)-5000)
	binary.Write(payload, binary.BigEndian, lon)
	binary.Write(payload, binary.BigEndian, lat)

	binary.Write(payload, binary.BigEndian, uint16(rand.Intn(2000))) // altitude
	binary.Write(payload, binary.BigEndian, uint16(rand.Intn(360)))  // angle
	payload.WriteByte(byte(rand.Intn(12) + 1))                        // satellites
	binary.Write(payload, binary.BigEndian, uint16(rand.Intn(120)))  // speed

	// IO Elements
	binary.Write(payload, binary.BigEndian, uint16(1)) // event IO ID
	payload.WriteByte(1)                               // total 1-byte IO
	payload.WriteByte(1)                               // 1-byte IO count
	payload.WriteByte(1)                               // IO ID
	payload.WriteByte(1)                               // IO value

	payload.WriteByte(0x01) // repeat number of records

	// CRC32
	crc := crc32.ChecksumIEEE(payload.Bytes())
	binary.Write(payload, binary.BigEndian, crc)

	// Full packet with 4-byte length header
	packet := new(bytes.Buffer)
	binary.Write(packet, binary.BigEndian, uint32(payload.Len()))
	packet.Write(payload.Bytes())

	return packet.Bytes()
}
