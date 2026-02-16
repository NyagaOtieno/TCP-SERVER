// main.go
package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	addr := env("LISTEN_ADDR", ":5027")

	// UDP
	go func() {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			log.Fatalf("UDP resolve error: %v", err)
		}
		pc, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			log.Fatalf("UDP listen error: %v", err)
		}
		defer pc.Close()

		log.Printf("UDP Listening on %s", addr)

		buf := make([]byte, 4096)
		for {
			n, raddr, err := pc.ReadFromUDP(buf)
			if err != nil {
				log.Printf("UDP read error: %v", err)
				continue
			}
			data := append([]byte(nil), buf[:n]...)
			log.Printf("UDP FROM %s RAW (%d): %s", raddr.String(), len(data), hex.EncodeToString(data))

			// Try parse GT06
			if ack, ok := handleGT06Datagram(data); ok {
				_, _ = pc.WriteToUDP(ack, raddr)
				log.Printf("UDP TO %s ACK: %s", raddr.String(), hex.EncodeToString(ack))
				continue
			}

			// Try parse P13/S168 (sometimes sent over UDP as ASCII)
			if resp, ok := handleP13MessageBytes(data); ok {
				_, _ = pc.WriteToUDP([]byte(resp), raddr)
				log.Printf("UDP TO %s P13 ACK: %q", raddr.String(), resp)
				continue
			}

			log.Printf("UDP FROM %s Unknown packet", raddr.String())
		}
	}()

	// TCP
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("TCP listen error: %v", err)
	}
	defer ln.Close()
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

// ---------------------------
// TCP handler (stream parser)
// ---------------------------
func handleTCPConn(c net.Conn) {
	defer c.Close()
	remote := c.RemoteAddr().String()
	log.Printf("TCP New connection: %s", remote)

	_ = c.SetReadDeadline(time.Now().Add(10 * time.Minute))

	var buf []byte
	tmp := make([]byte, 4096)

	for {
		n, err := c.Read(tmp)
		if err != nil {
			log.Printf("TCP %s closed: %v", remote, err)
			return
		}
		chunk := tmp[:n]
		log.Printf("TCP %s RAW (%d): %s", remote, n, hex.EncodeToString(chunk))
		buf = append(buf, chunk...)

		// Parse as many frames/messages as possible
		for {
			if len(buf) == 0 {
				break
			}

			// 1) P13 / UniGuard S168 ASCII ends with '$'
			if idx := bytes.IndexByte(buf, '$'); idx >= 0 {
				// If it looks like it contains S168 before '$', treat it as P13
				part := buf[:idx+1]
				if bytes.Contains(part, []byte("S168")) {
					resp, ok := handleP13MessageBytes(part)
					if ok {
						_, _ = c.Write([]byte(resp))
						log.Printf("TCP %s P13 ACK: %q", remote, resp)
					} else {
						log.Printf("TCP %s P13 parse failed: %q", remote, string(part))
					}
					buf = buf[idx+1:]
					continue
				}
			}

			// 2) GT06 binary frames (7878 or 7979)
			if ack, consumed, ok := parseAndAckGT06FromStream(buf); ok {
				if len(ack) > 0 {
					_, _ = c.Write(ack)
					log.Printf("TCP %s GT06 ACK: %s", remote, hex.EncodeToString(ack))
				}
				buf = buf[consumed:]
				continue
			}

			// 3) Teltonika IMEI handshake pattern (00 0F + ASCII digits) - optional fallback
			// Your logs show this for FMB920. If seen, ACK 0x01 and keep going.
			if imei, consumed, ok := parseTeltonikaIMEIHandshake(buf); ok {
				log.Printf("TCP %s Teltonika IMEI: %s", remote, imei)
				_, _ = c.Write([]byte{0x01})
				log.Printf("TCP %s Sent Teltonika IMEI ACK: 01", remote)
				buf = buf[consumed:]
				continue
			}

			// Otherwise, discard 1 byte to resync
			buf = buf[1:]
		}
	}
}

// ---------------------------
// UniGuard / Bluebird P13 (S168)
// ---------------------------
//
// Upstream example includes:
// S168 # IMEI # serial # length # LOCA: G; CELL: ...; GDATA: ...; ALERT: ...; STATUS: ...; WAY: 0 $ 4
// Downstream LOCA ACK:
// S168 # IMEI # serial # length # ACK ^ LOCA, parameter 5
//
// Heartbeat/SYNC:
// Upstream SYNC… 6
// Downstream ACK^SYNC,time 7
func handleP13MessageBytes(b []byte) (string, bool) {
	s := string(b)

	// keep only up to '$'
	if i := strings.IndexByte(s, '$'); i >= 0 {
		s = s[:i+1]
	} else {
		return "", false
	}

	// Normalize: remove spaces around separators
	s = strings.ReplaceAll(s, " ", "")
	if !strings.HasPrefix(s, "S168#") && !strings.HasPrefix(s, "S168") {
		return "", false
	}
	s = strings.TrimSuffix(s, "$")

	parts := strings.Split(s, "#")
	if len(parts) < 5 {
		return "", false
	}

	head := parts[0] // "S168" (sometimes could include "S168" only)
	imei := parts[1]
	serial := parts[2]
	// length := parts[3] // device-sent length (we don't strictly require it)
	content := strings.Join(parts[4:], "#")

	_ = head

	msgType := detectP13Type(content) // LOCA / SYNC / RET / other
	switch msgType {
	case "LOCA":
		// ACK^LOCA (parameter optional per doc) 8
		ackBody := "ACK^LOCA"
		return buildP13Downstream(imei, serial, ackBody), true

	case "SYNC":
		// ACK^SYNC,yyyymmddhhmmss 9
		nowUTC := time.Now().UTC().Format("20060102150405")
		ackBody := "ACK^SYNC," + nowUTC
		return buildP13Downstream(imei, serial, ackBody), true

	default:
		// Generic RET ack
		ackBody := "ACK^" + msgType
		return buildP13Downstream(imei, serial, ackBody), true
	}
}

func detectP13Type(content string) string {
	// Handles "LOCA:" / "SYNC:" and also "SYNC,BEAT:..." style 10
	up := strings.ToUpper(content)
	if strings.HasPrefix(up, "LOCA:") || strings.HasPrefix(up, "LOCA,") {
		return "LOCA"
	}
	if strings.HasPrefix(up, "SYNC:") || strings.HasPrefix(up, "SYNC,") {
		return "SYNC"
	}
	if strings.HasPrefix(up, "RET,") || strings.HasPrefix(up, "RET:") {
		return "RET"
	}
	// Fallback: take token up to ':' or ','
	for i, ch := range up {
		if ch == ':' || ch == ',' {
			if i == 0 {
				break
			}
			return up[:i]
		}
	}
	return "UNKNOWN"
}

func buildP13Downstream(imei, serial, ackBody string) string {
	// Downstream: S168#IMEI#serial#length#<content>$  (examples in doc) 11
	// length is hex (0000-ffff). We set it to the length of ackBody (safe + consistent).
	l := fmt.Sprintf("%04x", len(ackBody))
	return fmt.Sprintf("S168#%s#%s#%s#%s$", imei, serial, l, ackBody)
}

// ---------------------------
// GT06
// ---------------------------
//
// Login packet and response structure shown in PDF 12
// CRC-ITU: calculated from Packet Length through Information Serial Number 13
func parseAndAckGT06FromStream(buf []byte) (ack []byte, consumed int, ok bool) {
	// resync: find 7878 or 7979
	start := findGT06Start(buf)
	if start < 0 {
		return nil, 0, false
	}
	if start > 0 {
		// drop bytes before start
		return nil, start, true
	}

	// 0x78 0x78 format: [78 78] [len1] [proto] ... [serial2] [crc2] [0D 0A]
	if len(buf) >= 3 && buf[0] == 0x78 && buf[1] == 0x78 {
		if len(buf) < 5 {
			return nil, 0, false
		}
		l := int(buf[2])
		total := 2 + 1 + l + 2
		if len(buf) < total {
			return nil, 0, false
		}
		frame := buf[:total]
		if frame[total-2] != 0x0D || frame[total-1] != 0x0A {
			// not a full valid frame; resync by dropping 1 byte
			return nil, 1, true
		}

		// CRC check (optional but helpful)
		crcGot := uint16(frame[total-4])<<8 | uint16(frame[total-3])
		crcCalc := crcITU(frame[2 : total-4]) // from len to end of serial 14
		if crcGot != crcCalc {
			log.Printf("GT06 CRC mismatch got=%04x calc=%04x (continuing anyway)", crcGot, crcCalc)
		}

		proto := frame[3]
		serial := frame[total-6 : total-4] // 2 bytes before crc
		ack = buildGT06Ack78(proto, serial)
		logGT06(frame, proto)

		return ack, total, true
	}

	// 0x79 0x79 format (some devices): [79 79] [len2] [proto] ... [serial2] [crc2] [0D 0A]
	if len(buf) >= 4 && buf[0] == 0x79 && buf[1] == 0x79 {
		if len(buf) < 8 {
			return nil, 0, false
		}
		l := int(buf[2])<<8 | int(buf[3])
		total := 2 + 2 + l + 2
		if len(buf) < total {
			return nil, 0, false
		}
		frame := buf[:total]
		if frame[total-2] != 0x0D || frame[total-1] != 0x0A {
			return nil, 1, true
		}

		crcGot := uint16(frame[total-4])<<8 | uint16(frame[total-3])
		crcCalc := crcITU(frame[2 : total-4]) // from len(2 bytes) to serial
		if crcGot != crcCalc {
			log.Printf("GT06(7979) CRC mismatch got=%04x calc=%04x (continuing anyway)", crcGot, crcCalc)
		}

		proto := frame[4]
		serial := frame[total-6 : total-4]
		ack = buildGT06Ack79(proto, serial)
		logGT06(frame, proto)

		return ack, total, true
	}

	return nil, 0, false
}

func handleGT06Datagram(data []byte) ([]byte, bool) {
	ack, _, ok := parseAndAckGT06FromStream(data)
	return ack, ok && len(ack) > 0
}

func findGT06Start(b []byte) int {
	for i := 0; i+1 < len(b); i++ {
		if (b[i] == 0x78 && b[i+1] == 0x78) || (b[i] == 0x79 && b[i+1] == 0x79) {
			return i
		}
	}
	return -1
}

func buildGT06Ack78(proto byte, serial []byte) []byte {
	// Response example: 78 78 05 01 <serial2> <crc2> 0D 0A 15
	// len=0x05 => proto(1) + serial(2) + crc(2)
	out := []byte{0x78, 0x78, 0x05, proto, serial[0], serial[1], 0x00, 0x00, 0x0D, 0x0A}
	crc := crcITU(out[2 : len(out)-4])
	out[len(out)-4] = byte(crc >> 8)
	out[len(out)-3] = byte(crc)
	return out
}

func buildGT06Ack79(proto byte, serial []byte) []byte {
	// Similar but with 2-byte length.
	// len=0x0005
	out := []byte{0x79, 0x79, 0x00, 0x05, proto, serial[0], serial[1], 0x00, 0x00, 0x0D, 0x0A}
	crc := crcITU(out[2 : len(out)-4])
	out[len(out)-4] = byte(crc >> 8)
	out[len(out)-3] = byte(crc)
	return out
}

func logGT06(frame []byte, proto byte) {
	// If it's a login packet (0x01), terminal id is 8 bytes after proto 16
	if proto == 0x01 {
		// 7878: [0]=78 [1]=78 [2]=len [3]=proto [4..11]=terminal id
		if len(frame) >= 12 && (frame[0] == 0x78 && frame[1] == 0x78) {
			tid := frame[4:12]
			imei := bcdIMEI(tid)
			log.Printf("GT06 LOGIN IMEI: %s", imei)
		}
		// 7979: [0]=79 [1]=79 [2..3]=len [4]=proto [5..12]=terminal id
		if len(frame) >= 13 && (frame[0] == 0x79 && frame[1] == 0x79) {
			tid := frame[5:13]
			imei := bcdIMEI(tid)
			log.Printf("GT06(7979) LOGIN IMEI: %s", imei)
		}
	}
}

func bcdIMEI(b []byte) string {
	// Terminal ID is 8 bytes BCD representing IMEI digits 17
	var sb strings.Builder
	for _, x := range b {
		sb.WriteByte('0' + (x>>4)&0x0F)
		sb.WriteByte('0' + x&0x0F)
	}
	s := sb.String()
	s = strings.TrimLeft(s, "0")
	// many devices want 15 digits; if longer, keep last 15
	if len(s) > 15 {
		s = s[len(s)-15:]
	}
	return s
}

// CRC-ITU (CRC-16/CCITT-FALSE style used by GT06 docs) 18
func crcITU(data []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// ---------------------------
// Teltonika IMEI handshake (optional)
// ---------------------------
func parseTeltonikaIMEIHandshake(buf []byte) (imei string, consumed int, ok bool) {
	// Pattern: 00 0F + 15 ASCII digits (like your log "000f3335....")
	if len(buf) < 2 {
		return "", 0, false
	}
	if buf[0] != 0x00 || buf[1] != 0x0F {
		return "", 0, false
	}
	if len(buf) < 2+15 {
		return "", 0, false
	}
	raw := buf[2 : 2+15]
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return "", 0, false
		}
	}
	return string(raw), 2 + 15, true
}

// ---------------------------
// utils
// ---------------------------
func env(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func mustHexToInt(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	// tries hex then decimal
	if n, err := strconv.ParseInt(s, 16, 32); err == nil {
		return int(n)
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return 0
}