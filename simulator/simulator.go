package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"time"
)

// Device list for simulation
var devices = []struct {
	IMEI    string
	BaseLat float64
	BaseLng float64
}{
	{"123456789211223873", -1.2921, 36.8219},
	{"1234567892123873", -1.3032, 36.8148},
}

func main() {
	serverAddr := "switchback.proxy.rlwy.net:15376"

	for _, d := range devices {
		fmt.Printf("📡 Simulating device %s\n", d.IMEI)
		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			fmt.Println("❌ Failed to connect:", err)
			continue
		}

		// Send IMEI first
		imeiPacket := append([]byte{0x0F}, []byte(d.IMEI)...)
		conn.Write(imeiPacket)

		// Wait for server ACK
		ack := make([]byte, 1)
		_, err = conn.Read(ack)
		if err != nil {
			fmt.Println("❌ Failed to read ACK:", err)
			conn.Close()
			continue
		}
		fmt.Println("✅ IMEI sent and ACK received")

		// Send multiple AVL packets
		for i := 0; i < 5; i++ { // send 5 packets per device
			packet := createAVLPacket(d.BaseLat, d.BaseLng)
			conn.Write(packet)

			// Wait for ACK from server for this packet
			_, _ = conn.Read(ack)

			fmt.Printf("📍 Packet %d sent for %s\n", i+1, d.IMEI)
			time.Sleep(1 * time.Second)
		}

		conn.Close()
		fmt.Printf("✅ Simulation completed for %s\n\n", d.IMEI)
	}
}

// createAVLPacket generates a fake 25-byte AVL packet
func createAVLPacket(baseLat, baseLng float64) []byte {
	buf := new(bytes.Buffer)

	// Timestamp (milliseconds)
	timestamp := time.Now().UnixMilli()
	binary.Write(buf, binary.BigEndian, uint64(timestamp))

	// Longitude / Latitude scaled by 1e7 and converted to int32
	lon := int32(baseLng*1e7) + int32(rand.Int31n(10000)-5000)
	lat := int32(baseLat*1e7) + int32(rand.Int31n(10000)-5000)
	binary.Write(buf, binary.BigEndian, lon)
	binary.Write(buf, binary.BigEndian, lat)

	// Altitude
	binary.Write(buf, binary.BigEndian, uint16(rand.Intn(500)))

	// Angle
	binary.Write(buf, binary.BigEndian, uint16(rand.Intn(360)))

	// Satellites
	buf.WriteByte(byte(rand.Intn(12) + 1))

	// Speed
	binary.Write(buf, binary.BigEndian, uint16(rand.Intn(120)))

	// Pad to 25 bytes if necessary
	for buf.Len() < 25 {
		buf.WriteByte(0x00)
	}

	return buf.Bytes()
}
