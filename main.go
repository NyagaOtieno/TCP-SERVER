package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log"
	"net"
	"time"
)

const listenAddr = ":5027"

func main() {
	log.SetFlags(log.LstdFlags)

	go startTCP(listenAddr)
	go startUDP(listenAddr)

	select {} // keep running
}

/* =========================
   TCP
========================= */

func startTCP(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("TCP listen error: %v", err)
	}
	log.Printf("TCP Listening on %s", addr)

	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("TCP accept error: %v", err)
			continue
		}
		go handleTCPConn(c)
	}
}

func handleTCPConn(c net.Conn) {
	defer c.Close()
	remote := c.RemoteAddr().String()
	log.Printf("TCP New connection: %s", remote)

	_ = c.SetReadDeadline(time.Now().Add(120 * time.Second))

	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			data := buf[:n]
			log.Printf("TCP RAW (%d bytes): %s", len(data), hex.EncodeToString(data))

			// 1) Teltonika "000F + ASCII IMEI"
			if imei, ok := parseTeltonikaImei(data); ok {
				log.Printf("TCP Teltonika IMEI: %s", imei)
				ack := []byte{0x01}
				_, _ = c.Write(ack)
				log.Printf("TCP Sent Teltonika IMEI ACK: %s", hex.EncodeToString(ack))
				continue
			}

			// 2) GT06 "7878" / "7979"
			if isGT06(data) {
				acks := buildGT06AcksFromChunk(data)
				for _, a := range acks {
					_, _ = c.Write(a)
					log.Printf("TCP Sent GT06 ACK: %s", hex.EncodeToString(a))
				}
				continue
			}

			// Unknown
			log.Printf("TCP Unknown packet format from %s", remote)
		}

		if err != nil {
			if err == io.EOF {
				log.Printf("TCP Connection closed: EOF (%s)", remote)
			} else {
				log.Printf("TCP Read error (%s): %v", remote, err)
			}
			return
		}
	}
}

/* =========================
   UDP
========================= */

func startUDP(addr string) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalf("UDP resolve error: %v", err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("UDP listen error: %v", err)
	}
	defer conn.Close()

	log.Printf("UDP Listening on %s", addr)

	buf := make([]byte, 4096)
	for {
		n, raddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("UDP read error: %v", err)
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])

		log.Printf("UDP RAW (%d bytes) from %s: %s", len(data), raddr.String(), hex.EncodeToString(data))

		// Teltonika IMEI over UDP is rare, but handle anyway
		if imei, ok := parseTeltonikaImei(data); ok {
			log.Printf("UDP Teltonika IMEI: %s", imei)
			ack := []byte{0x01}
			_, _ = conn.WriteToUDP(ack, raddr)
			log.Printf("UDP Sent Teltonika IMEI ACK: %s", hex.EncodeToString(ack))
			continue
		}

		// GT06
		if isGT06(data) {
			acks := buildGT06AcksFromChunk(data)
			for _, a := range acks {
				_, _ = conn.WriteToUDP(a, raddr)
				log.Printf("UDP Sent GT06 ACK: %s", hex.EncodeToString(a))
			}
			continue
		}

		log.Printf("UDP Unknown packet format from %s", raddr.String())
	}
}

/* =========================
   Teltonika IMEI detector
   Pattern: 0x00 0x0F + 15 ASCII digits
========================= */

func parseTeltonikaImei(b []byte) (string, bool) {
	if len(b) < 2+15 {
		return "", false
	}
	if b[0] != 0x00 || b[1] != 0x0F {
		return "", false
	}
	imeiBytes := b[2 : 2+15]
	for _, c := range imeiBytes {
		if c < '0' || c > '9' {
			return "", false
		}
	}
	return string(imeiBytes), true
}

/* =========================
   GT06 helpers
========================= */

func isGT06(b []byte) bool {
	return len(b) >= 5 && ((b[0] == 0x78 && b[1] == 0x78) || (b[0] == 0x79 && b[1] == 0x79))
}

// Some devices may send multiple GT06 frames back-to-back.
// This tries to split and ACK each frame found.
func buildGT06AcksFromChunk(chunk []byte) [][]byte {
	var acks [][]byte
	i := 0

	for i < len(chunk) {
		// Find start
		if i+2 > len(chunk) {
			break
		}
		if !((chunk[i] == 0x78 && chunk[i+1] == 0x78) || (chunk[i] == 0x79 && chunk[i+1] == 0x79)) {
			i++
			continue
		}

		start0 := chunk[i]
		start1 := chunk[i+1]

		// Short frame 7878: [78 78][len 1][proto 1]...[crc 2][0D 0A]
		// Long frame 7979:  [79 79][len 2][proto 1]...[crc 2][0D 0A]
		var frameLen int
		var protoIndex int
		var lenField []byte

		if start0 == 0x78 {
			if i+3 > len(chunk) {
				break
			}
			l := int(chunk[i+2])
			// total frame bytes = 2(start) + 1(len) + l + 2(crc) + 2(stop)
			frameLen = 2 + 1 + l + 2 + 2
			protoIndex = i + 3
			lenField = []byte{chunk[i+2]}
		} else {
			if i+4 > len(chunk) {
				break
			}
			l := int(binary.BigEndian.Uint16(chunk[i+2 : i+4]))
			// total frame bytes = 2(start) + 2(len) + l + 2(crc) + 2(stop)
			frameLen = 2 + 2 + l + 2 + 2
			protoIndex = i + 4
			lenField = chunk[i+2 : i+4]
		}

		if i+frameLen > len(chunk) {
			// incomplete frame
			break
		}

		frame := chunk[i : i+frameLen]
		// Must end with 0D0A
		if !(frame[len(frame)-2] == 0x0D && frame[len(frame)-1] == 0x0A) {
			i++
			continue
		}

		// Protocol
		if protoIndex >= len(frame) {
			i += frameLen
			continue
		}
		proto := frame[protoIndex]

		// Serial number is the 2 bytes just before CRC in GT06 frames:
		// [...][serial 2][crc 2][0D 0A]
		if len(frame) < 2+2+2 {
			i += frameLen
			continue
		}
		serial := frame[len(frame)-2-2-2 : len(frame)-2-2] // 2 bytes before CRC

		ack := buildGT06Ack(start0, start1, lenField, proto, serial)
		acks = append(acks, ack)

		i += frameLen
	}

	return acks
}

func buildGT06Ack(start0, start1 byte, lenField []byte, proto byte, serial []byte) []byte {
	// ACK payload content is: [LEN][PROTO][SERIAL(2)]
	// CRC16/X25 is calculated over that content only.
	// For 7878, LEN is 1 byte. For 7979, LEN is 2 bytes.
	// Typical ACK length (content bytes) = 1(proto) + 2(serial) = 3, plus LEN field itself.
	// Most devices accept LEN=0x05 for 7878 acks: 1(proto)+2(serial)+2(crc?) (but CRC not included in len in this variant).
	// However in practice, standard ACK frame used by many servers:
	// 7878 05 [proto] [serial2] [crc2] 0D0A
	// 7979 0005 [proto] [serial2] [crc2] 0D0A

	var out bytes.Buffer
	out.WriteByte(start0)
	out.WriteByte(start1)

	if start0 == 0x78 {
		out.WriteByte(0x05) // common GT06 ack len
		content := []byte{0x05, proto, serial[0], serial[1]}
		crc := crc16X25(content)
		out.WriteByte(proto)
		out.Write(serial)
		out.WriteByte(byte(crc >> 8))
		out.WriteByte(byte(crc & 0xFF))
		out.WriteByte(0x0D)
		out.WriteByte(0x0A)
		return out.Bytes()
	}

	// 7979 long header: len is 2 bytes
	out.WriteByte(0x00)
	out.WriteByte(0x05)
	content := []byte{0x00, 0x05, proto, serial[0], serial[1]}
	crc := crc16X25(content)
	out.WriteByte(proto)
	out.Write(serial)
	out.WriteByte(byte(crc >> 8))
	out.WriteByte(byte(crc & 0xFF))
	out.WriteByte(0x0D)
	out.WriteByte(0x0A)
	return out.Bytes()
}

// CRC16/X25 (also called CRC-16/IBM-SDLC in some contexts)
// poly=0x1021, init=0xFFFF, refin=true, refout=true, xorout=0xFFFF
func crc16X25(data []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if (crc & 0x0001) != 0 {
				crc = (crc >> 1) ^ 0x8408
			} else {
				crc >>= 1
			}
		}
	}
	return ^crc
}