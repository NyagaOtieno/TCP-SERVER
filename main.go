package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
GT06 key facts from your PDF:
- Start: 0x78 0x78
- Stop:  0x0D 0x0A
- Length is 1 byte (counts: Protocol + Info + Serial(2) + CRC(2))
- Server ACK example:
  78 78 05 01 00 01 D9 DC 0D 0A  (login ack)   5
  78 78 05 13 00 11 F9 70 0D 0A  (status ack)  6
- Location packet (0x12) field layout example in doc. 7
*/

const (
	defaultPort = 5027

	// Teltonika IMEI "login" is: 0x00 0x0F + 15 ASCII digits (IMEI)
	teltonikaIMEIPrefixHi = 0x00
	teltonikaIMEIPrefixLo = 0x0F

	// GT06 framing
	gt06Start1 = 0x78
	gt06Start2 = 0x78
	gt06Stop1  = 0x0D
	gt06Stop2  = 0x0A

	// GT06 protocol numbers (common)
	gt06ProtoLogin    = 0x01
	gt06ProtoLocation = 0x12
	gt06ProtoStatus   = 0x13 // "P13" heartbeat/status packet in your logs/doc 8
)

type ConnState struct {
	Proto string // "teltonika" or "gt06" or ""
	IMEI  string
	Buf   []byte
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	port := envInt("PORT", defaultPort)
	addr := fmt.Sprintf("0.0.0.0:%d", port)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		startTCP(addr)
	}()
	go func() {
		defer wg.Done()
		startUDP(addr)
	}()

	wg.Wait()
}

// =======================
// TCP
// =======================

func startTCP(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("TCP listen failed: %v", err)
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

func handleTCPConn(c net.Conn) {
	defer c.Close()

	remote := c.RemoteAddr().String()
	log.Printf("TCP New connection: %s", remote)

	st := &ConnState{Buf: make([]byte, 0, 4096)}

	tmp := make([]byte, 4096)
	for {
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Minute))
		n, err := c.Read(tmp)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				log.Printf("TCP timeout: %s", remote)
			} else {
				log.Printf("TCP closed: %s (%v)", remote, err)
			}
			return
		}
		if n <= 0 {
			continue
		}

		chunk := tmp[:n]
		log.Printf("TCP RAW (%d bytes): %s", len(chunk), hex.EncodeToString(chunk))
		st.Buf = append(st.Buf, chunk...)

		// Decide protocol as soon as we can
		if st.Proto == "" {
			if looksLikeTeltonikaIMEI(st.Buf) {
				st.Proto = "teltonika"
			} else if bytes.Contains(st.Buf, []byte{gt06Start1, gt06Start2}) {
				st.Proto = "gt06"
			}
		}

		switch st.Proto {
		case "teltonika":
			if err := processTeltonikaTCP(c, st); err != nil {
				log.Printf("Teltonika error (%s): %v", remote, err)
				return
			}
		case "gt06":
			if err := processGT06StreamTCP(c, st); err != nil {
				log.Printf("GT06 error (%s): %v", remote, err)
				return
			}
		default:
			// Not sure yet; keep buffering until we can detect
			if len(st.Buf) > 8192 {
				log.Printf("Unknown protocol, dropping buffer (%s)", remote)
				st.Buf = st.Buf[:0]
			}
		}
	}
}

// =======================
// UDP
// =======================

func startUDP(addr string) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalf("UDP resolve failed: %v", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("UDP listen failed: %v", err)
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
		data := append([]byte(nil), buf[:n]...)
		log.Printf("UDP RAW (%d bytes) from %s: %s", n, raddr.String(), hex.EncodeToString(data))

		// Most UniGuard/GT06 devices send GT06 frames over UDP too
		if !bytes.HasPrefix(data, []byte{gt06Start1, gt06Start2}) || len(data) < 10 {
			log.Printf("UDP unsupported packet (not 7878...) from %s", raddr.String())
			continue
		}

		// Handle a single GT06 datagram (it may contain exactly 1 frame)
		frame, ok := extractOneGT06Frame(data)
		if !ok {
			log.Printf("UDP could not extract a full GT06 frame from %s", raddr.String())
			continue
		}

		info, err := parseGT06Frame(frame)
		if err != nil {
			log.Printf("UDP GT06 parse error from %s: %v", raddr.String(), err)
			continue
		}

		ack := buildGT06Ack(info.Protocol, info.Serial)
		_, _ = conn.WriteToUDP(ack, raddr)
		log.Printf("UDP Sent GT06 ACK to %s: %s", raddr.String(), hex.EncodeToString(ack))
	}
}

// =======================
// Teltonika (TCP)
// =======================

func looksLikeTeltonikaIMEI(b []byte) bool {
	// Expect at least 17 bytes: 00 0F + 15 ASCII digits
	if len(b) < 17 {
		return false
	}
	return b[0] == teltonikaIMEIPrefixHi && b[1] == teltonikaIMEIPrefixLo
}

func processTeltonikaTCP(c net.Conn, st *ConnState) error {
	// Step 1: read IMEI if not yet read
	if st.IMEI == "" {
		if len(st.Buf) < 17 {
			return nil // wait more
		}
		raw := st.Buf[:17]
		st.Buf = st.Buf[17:]

		imeiBytes := raw[2:] // after 00 0F
		imei := strings.TrimSpace(string(imeiBytes))
		imei = onlyDigits(imei)

		st.IMEI = imei
		log.Printf("TCP Teltonika IMEI: %s", st.IMEI)

		// Teltonika expects single byte 0x01 on IMEI accept
		_, _ = c.Write([]byte{0x01})
		log.Printf("TCP Sent Teltonika IMEI ACK: 01")
	}

	// Step 2: parse AVL frames: [00000000][dataLen:4][data:dataLen][crc:4]
	for {
		if len(st.Buf) < 8 {
			return nil
		}
		// Must start with 4 zero preamble
		if !bytes.Equal(st.Buf[:4], []byte{0, 0, 0, 0}) {
			// resync: drop 1 byte
			st.Buf = st.Buf[1:]
			continue
		}

		dataLen := int(binary.BigEndian.Uint32(st.Buf[4:8]))
		if dataLen <= 0 || dataLen > 1024*1024 {
			// invalid, resync
			st.Buf = st.Buf[1:]
			continue
		}

		total := 8 + dataLen + 4
		if len(st.Buf) < total {
			return nil // wait more
		}

		frame := st.Buf[:total]
		st.Buf = st.Buf[total:]

		// Try to read AVL record count (codec8/8E)
		count := teltonikaAVLCount(frame)
		log.Printf("TCP Teltonika AVL received: dataLen=%d records=%d imei=%s", dataLen, count, st.IMEI)

		// Correct Teltonika response is 4 bytes: number of records (NOT 5 bytes)
		ack := make([]byte, 4)
		binary.BigEndian.PutUint32(ack, uint32(count))
		_, _ = c.Write(ack)
		log.Printf("TCP Sent Teltonika AVL ACK: %s", hex.EncodeToString(ack))
	}
}

func teltonikaAVLCount(frame []byte) int {
	// frame: 4 preamble + 4 len + data + 4 crc
	if len(frame) < 8+2 {
		return 0
	}
	dataLen := int(binary.BigEndian.Uint32(frame[4:8]))
	if len(frame) < 8+dataLen {
		return 0
	}
	data := frame[8 : 8+dataLen]
	// Data layout: codecID (1), recordCount (1), records..., recordCount (1), crc16? etc depends.
	if len(data) < 2 {
		return 0
	}
	return int(data[1])
}

// =======================
// GT06 (TCP/UDP)
// =======================

type GT06Info struct {
	Protocol byte
	Serial   uint16
	IMEI     string // for login packets
	// For location packets (0x12)
	Lat   float64
	Lon   float64
	Speed int
	Time  time.Time
}

func processGT06StreamTCP(c net.Conn, st *ConnState) error {
	for {
		frame, ok := extractOneGT06Frame(st.Buf)
		if !ok {
			// no full frame yet
			if len(st.Buf) > 65535 {
				st.Buf = st.Buf[:0]
			}
			return nil
		}

		// consume
		st.Buf = st.Buf[len(frame):]

		info, err := parseGT06Frame(frame)
		if err != nil {
			log.Printf("TCP GT06 parse error: %v | frame=%s", err, hex.EncodeToString(frame))
			continue
		}

		// Log useful decoded fields
		switch info.Protocol {
		case gt06ProtoLogin:
			log.Printf("TCP GT06 LOGIN imei=%s serial=%d", info.IMEI, info.Serial)
		case gt06ProtoLocation:
			log.Printf("TCP GT06 LOCATION lat=%.6f lon=%.6f speed=%d time=%s serial=%d",
				info.Lat, info.Lon, info.Speed, info.Time.Format(time.RFC3339), info.Serial)
		case gt06ProtoStatus:
			log.Printf("TCP GT06 STATUS (P13/0x13) serial=%d", info.Serial) // 9
		default:
			log.Printf("TCP GT06 proto=0x%02X serial=%d", info.Protocol, info.Serial)
		}

		ack := buildGT06Ack(info.Protocol, info.Serial)
		_, _ = c.Write(ack)
		log.Printf("TCP Sent GT06 ACK: %s", hex.EncodeToString(ack))
	}
}

func extractOneGT06Frame(b []byte) ([]byte, bool) {
	// Find start 78 78
	i := bytes.Index(b, []byte{gt06Start1, gt06Start2})
	if i < 0 {
		return nil, false
	}
	if i > 0 {
		// drop leading junk by returning a "virtual" frame length 0;
		// caller will resync by consuming when we return nil/false, so we handle here:
		b = b[i:]
	}

	if len(b) < 5 {
		return nil, false
	}
	// length byte at b[2]
	L := int(b[2])
	// total = start(2) + len(1) + L + stop(2)
	total := 2 + 1 + L + 2
	if total <= 0 || total > 2048 {
		return nil, false
	}
	if len(b) < total {
		return nil, false
	}
	frame := b[:total]
	// validate stop bits
	if frame[total-2] != gt06Stop1 || frame[total-1] != gt06Stop2 {
		// not a real frame; try to resync by shifting one byte next loop
		return nil, false
	}
	return frame, true
}

func parseGT06Frame(frame []byte) (*GT06Info, error) {
	// frame: 78 78 [len] [proto] [info...] [serialHi serialLo] [crcHi crcLo] 0D 0A
	if len(frame) < 10 {
		return nil, fmt.Errorf("frame too short")
	}
	if frame[0] != gt06Start1 || frame[1] != gt06Start2 {
		return nil, fmt.Errorf("bad start")
	}
	if frame[len(frame)-2] != gt06Stop1 || frame[len(frame)-1] != gt06Stop2 {
		return nil, fmt.Errorf("bad stop")
	}

	L := int(frame[2])
	if 2+1+L+2 != len(frame) {
		return nil, fmt.Errorf("length mismatch: L=%d total=%d", L, len(frame))
	}

	// CRC in packet (last 4 bytes before stop: [crcHi crcLo 0D 0A])
	crcIn := binary.BigEndian.Uint16(frame[len(frame)-4 : len(frame)-2])

	// CRC calculated over: [length .. serial] (exclude CRC itself and stop)
	// That is bytes from frame[2] up to (but excluding) crc field.
	crcCalc := crc16ITU(frame[2 : len(frame)-4])
	if crcCalc != crcIn {
		return nil, fmt.Errorf("crc mismatch: got=0x%04X calc=0x%04X", crcIn, crcCalc)
	}

	proto := frame[3]

	// serial is always the 2 bytes right before CRC
	serial := binary.BigEndian.Uint16(frame[len(frame)-6 : len(frame)-4])

	infoStart := 4
	infoEnd := len(frame) - 6 // up to serial
	info := frame[infoStart:infoEnd]

	out := &GT06Info{Protocol: proto, Serial: serial}

	switch proto {
	case gt06ProtoLogin:
		// login info contains terminal ID (IMEI) in BCD (8 bytes) in example 10
		if len(info) < 8 {
			return out, nil
		}
		out.IMEI = bcdIMEI(info[:8])

	case gt06ProtoLocation:
		// See field layout in your doc: DateTime(6), GPS/Sats(1), Lat(4), Lon(4), Speed(1), Course/Status(2), ... 11
		if len(info) < 6+1+4+4+1+2 {
			return out, nil
		}
		out.Time = parseGT06Time(info[0:6])
		gpsSat := info[6]
		_ = gpsSat // available if you want later

		latRaw := binary.BigEndian.Uint32(info[7:11])
		lonRaw := binary.BigEndian.Uint32(info[11:15])

		// GT06 uses degree * 30000 * 60 (common). Convert:
		out.Lat = float64(latRaw) / 30000.0 / 60.0
		out.Lon = float64(lonRaw) / 30000.0 / 60.0

		out.Speed = int(info[15])

	case gt06ProtoStatus:
		// P13 status/heartbeat packet; we just ACK it correctly as shown in examples 12
		// You can parse terminal info / voltage / GSM / alarm/lang if you want later.
	}

	return out, nil
}

func buildGT06Ack(protocol byte, serial uint16) []byte {
	// ACK format shown in your doc examples:
	// 78 78 05 [protocol] [serialHi serialLo] [crcHi crcLo] 0D 0A  13 14
	ack := make([]byte, 10)
	ack[0] = gt06Start1
	ack[1] = gt06Start2
	ack[2] = 0x05
	ack[3] = protocol
	binary.BigEndian.PutUint16(ack[4:6], serial)

	crc := crc16ITU(ack[2:6]) // len + proto + serial
	binary.BigEndian.PutUint16(ack[6:8], crc)

	ack[8] = gt06Stop1
	ack[9] = gt06Stop2
	return ack
}

// CRC-ITU (CRC-16/IBM-SDLC / CRC-16/CCITT-FALSE variants differ; GT06 doc says CRC-ITU.
// Common implementation used by GT06 servers is poly 0x1021, init 0xFFFF, no xorout.
func crc16ITU(data []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if (crc & 0x8000) != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// =======================
// Helpers
// =======================

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func onlyDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func bcdIMEI(b []byte) string {
	// 8 bytes BCD → 16 digits, often last digit padded
	var out strings.Builder
	for _, x := range b {
		hi := (x >> 4) & 0x0F
		lo := x & 0x0F
		out.WriteByte('0' + hi)
		out.WriteByte('0' + lo)
	}
	imei := strings.TrimLeft(out.String(), "0")
	// many devices are 15 digits
	if len(imei) > 15 {
		imei = imei[len(imei)-15:]
	}
	return imei
}

func parseGT06Time(b []byte) time.Time {
	// YY MM DD HH mm ss (all in hex/decimal values)
	if len(b) != 6 {
		return time.Now().UTC()
	}
	yy := int(b[0]) + 2000
	mo := time.Month(b[1])
	dd := int(b[2])
	hh := int(b[3])
	mm := int(b[4])
	ss := int(b[5])
	// assume UTC
	return time.Date(yy, mo, dd, hh, mm, ss, 0, time.UTC)
}