package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log"
	"net"
)

func main() {
	addr := ":5027"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", addr)

	for {
		c, err := ln.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}
		log.Println("New connection:", c.RemoteAddr())
		go handleConn(c)
	}
}

func handleConn(c net.Conn) {
	defer c.Close()

	r := bufio.NewReader(c)
	buf := make([]byte, 0, 8192)
	tmp := make([]byte, 2048)

	for {
		n, err := r.Read(tmp)
		if n > 0 {
			chunk := tmp[:n]
			log.Printf("RAW (%d bytes): %s\n", len(chunk), hex.EncodeToString(chunk))

			buf = append(buf, chunk...)

			// Try to extract as many GT06 frames as possible from buf
			for {
				frame, rest := nextGT06Frame(buf)
				if frame == nil {
					break
				}
				buf = rest
				processGT06Frame(c, frame)
			}

			// prevent unbounded buffer growth if device sends non-GT06 forever
			if len(buf) > 200000 {
				buf = buf[len(buf)-8000:]
			}
		}
		if err != nil {
			if err == io.EOF {
				log.Println("Connection closed: EOF")
			} else {
				log.Println("read error:", err)
			}
			return
		}
	}
}

// nextGT06Frame returns one full frame and remaining bytes.
// Recognizes 7878 (1-byte length) and 7979 (2-byte length) frames ending with 0D0A.
func nextGT06Frame(b []byte) (frame []byte, rest []byte) {
	// find header 7878 or 7979
	for i := 0; i+4 < len(b); i++ {
		if b[i] == 0x78 && b[i+1] == 0x78 {
			// length is 1 byte at i+2, total = 2(start)+1(len)+len+2(crc)+2(stop)? (len already covers protocol..serial)
			if i+3 >= len(b) {
				return nil, b
			}
			l := int(b[i+2])
			total := 2 + 1 + l + 2 + 2 // start + len + (l bytes) + crc + stop
			if i+total > len(b) {
				return nil, b
			}
			f := b[i : i+total]
			if f[len(f)-2] == 0x0D && f[len(f)-1] == 0x0A {
				return f, append([]byte{}, b[i+total:]...)
			}
		}
		if b[i] == 0x79 && b[i+1] == 0x79 {
			// length is 2 bytes at i+2..i+3
			if i+5 >= len(b) {
				return nil, b
			}
			l := int(binary.BigEndian.Uint16(b[i+2 : i+4]))
			total := 2 + 2 + l + 2 + 2 // start + len2 + (l bytes) + crc + stop
			if i+total > len(b) {
				return nil, b
			}
			f := b[i : i+total]
			if f[len(f)-2] == 0x0D && f[len(f)-1] == 0x0A {
				return f, append([]byte{}, b[i+total:]...)
			}
		}
	}
	return nil, b
}

func processGT06Frame(c net.Conn, f []byte) {
	log.Printf("GT06 FRAME (%d): %s\n", len(f), hex.EncodeToString(f))

	var (
		isShort = f[0] == 0x78
		lenPos  = 2
	)
	var contentStart int
	var contentLen int

	if isShort {
		contentLen = int(f[lenPos])
		contentStart = 3 // after 7878 + len
	} else {
		contentLen = int(binary.BigEndian.Uint16(f[lenPos : lenPos+2]))
		contentStart = 4 // after 7979 + len2
	}

	// content is: [protocol(1) ... serial(2)] then CRC(2) then 0D0A
	content := f[contentStart : contentStart+contentLen]
	if len(content) < 1+2 {
		log.Println("content too short")
		return
	}

	proto := content[0]
	serial := content[len(content)-2:] // last 2 bytes inside content
	recvCrc := binary.BigEndian.Uint16(f[contentStart+contentLen : contentStart+contentLen+2])

	// CRC is computed over: length field + content (protocol..serial)
	var crcData []byte
	if isShort {
		crcData = append([]byte{byte(contentLen)}, content...)
	} else {
		len2 := make([]byte, 2)
		binary.BigEndian.PutUint16(len2, uint16(contentLen))
		crcData = append(len2, content...)
	}
	calc := crc16x25(crcData)

	if calc != recvCrc {
		log.Printf("CRC FAIL proto=0x%02X serial=%x calc=%04X recv=%04X\n", proto, serial, calc, recvCrc)
		return
	}

	log.Printf("CRC OK proto=0x%02X serial=%x\n", proto, serial)

	// Send ACK for login (0x01): server reply is also a GT06 frame
	if proto == 0x01 {
		ack := buildAckShort(0x01, serial)
		_, _ = c.Write(ack)
		log.Printf("Sent LOGIN ACK: %s\n", hex.EncodeToString(ack))
	}
}

// Build short ACK: 78 78 05 [proto] [serialHi serialLo] [crcHi crcLo] 0D 0A
func buildAckShort(proto byte, serial []byte) []byte {
	contentLen := byte(0x05)
	// content: proto + serial(2)
	content := []byte{proto, serial[0], serial[1]}
	// CRC over: len + content
	crcData := append([]byte{contentLen}, content...)
	crc := crc16x25(crcData)

	out := make([]byte, 0, 2+1+int(contentLen)+2+2)
	out = append(out, 0x78, 0x78, contentLen)
	out = append(out, content...)
	out = append(out, byte(crc>>8), byte(crc))
	out = append(out, 0x0D, 0x0A)
	return out
}

// CRC-16/X25 (CRC-ITU) reflected form, poly 0x1021 => 0x8408 in reflected loop
func crc16x25(data []byte) uint16 {
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
