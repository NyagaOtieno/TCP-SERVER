package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/joho/godotenv"
)

type AVLData struct {
	Timestamp  time.Time
	Latitude   float64
	Longitude  float64
	Altitude   int
	Angle      int
	Satellites int
	Speed      int
	IOData     map[uint8]interface{}
}

type Device struct {
	ID   int    `json:"id"`
	IMEI string `json:"imei"`
}

var (
	tcpServerHost      string
	backendTrackURL    string
	db                 *sql.DB
	httpClient         = &http.Client{Timeout: 10 * time.Second}
	wg                 sync.WaitGroup
	positionsHasIoData bool
	verbose            = true
)

func vLog(format string, a ...interface{}) {
	if verbose {
		log.Printf(format, a...)
	}
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	_ = godotenv.Load()

	tcpServerHost = getEnv("TCP_SERVER_HOST", "0.0.0.0:5027")
	backendTrackURL = getEnv("BACKEND_TRACK_URL", "https://mytrack-production.up.railway.app/api/track")

	pgURL := getEnv("DATABASE_URL", "")
	if pgURL == "" {
		log.Fatal("❌ DATABASE_URL not set")
	}

	var err error
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		log.Fatalf("❌ Failed to open PostgreSQL connection: %v", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatalf("❌ PostgreSQL ping failed: %v", err)
	}
	vLog("✅ PostgreSQL connected successfully")

	positionsHasIoData = checkPositionsHasIoData()
	if positionsHasIoData {
		vLog("ℹ️ positions.io_data column detected; will store IO JSON")
	} else {
		vLog("⚠️ positions.io_data column not detected; IO data will be omitted from DB inserts")
	}
}

func checkPositionsHasIoData() bool {
	var col string
	err := db.QueryRow(`
		SELECT column_name FROM information_schema.columns 
		WHERE table_name='positions' AND column_name='io_data' LIMIT 1
	`).Scan(&col)
	return err == nil && col == "io_data"
}

func main() {
	vLog("🚀 Starting Teltonika TCP server...")

	listener, err := net.Listen("tcp", tcpServerHost)
	if err != nil {
		log.Fatalf("❌ Failed to start TCP server: %v", err)
	}
	defer listener.Close()

	vLog("✅ TCP Server listening on %s", tcpServerHost)

	for {
		conn, err := listener.Accept()
		if err != nil {
			vLog("⚠️ Accept error: %v", err)
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer func() {
				if r := recover(); r != nil {
					vLog("🔥 Panic recovered: %v", r)
				}
			}()
			handleConnection(c)
		}(conn)
	}

	wg.Wait()
}

// =====================================================
//                 CONNECTION HANDLING
// =====================================================

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	vLog("🔗 New connection from %s", remote)

	// ✅ Read first bytes once, decide protocol (Teltonika vs GT06/P13)
	first, err := readFirstBytes(conn)
	if err != nil {
		vLog("❌ Failed initial read from %s: %v", remote, err)
		return
	}
	vLog("🧾 First bytes: %s", strings.ToUpper(hex.EncodeToString(first)))

	// ---------- GT06 / P13 branch ----------
	if looksLikeGT06(first) {
		imei, leftover, err := gt06LoginAndGetIMEI(conn, first)
		if err != nil {
			vLog("❌ GT06 login failed from %s: %v", remote, err)
			return
		}
		vLog("📡 GT06 Device connected: %s", imei)

		deviceID, err := ensureDevice(imei)
		if err != nil {
			vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
			return
		}

		handleGT06Loop(conn, imei, deviceID, leftover)
		return
	}

	// ---------- Teltonika branch (your original logic) ----------
	// We must extract IMEI using your existing readIMEI() logic,
	// BUT we already consumed "first". So we parse IMEI from "first" then continue normally.
	imei, err := readIMEIFromFirst(first, conn)
	if err != nil {
		vLog("❌ Failed IMEI read from %s: %v", remote, err)
		return
	}
	vLog("📡 Device connected: %s", imei)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
		return
	}

	// Now continue your existing Teltonika frame loop exactly as before
	residual := make([]byte, 0)
	tmp := make([]byte, 4096)

	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		n, err := conn.Read(tmp)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				vLog("⏱ Read timeout for %s, closing connection", imei)
			} else if err != io.EOF {
				vLog("🔌 Read error for %s: %v", imei, err)
			}
			return
		}

		if n > 0 {
			vLog("🟢 Raw TCP bytes: %s", hex.EncodeToString(tmp[:n]))
			residual = append(residual, tmp[:n]...)
			vLog("📥 Residual buffer length: %d", len(residual))
		}

		for len(residual) >= 4 {
			packetLen := int(binary.BigEndian.Uint32(residual[:4]))
			if packetLen <= 0 || packetLen > 5*1024*1024 {
				vLog("⚠️ Invalid packet length %d from %s", packetLen, imei)
				residual = residual[4:]
				continue
			}

			if len(residual) < 4+packetLen {
				break
			}

			frame := residual[4 : 4+packetLen]
			codecPayload, err := normalizeToCodec8(frame)
			if err != nil {
				vLog("❌ Codec normalization failed: %v", err)
				residual = residual[4+packetLen:]
				continue
			}

			records, err := parseCodec(codecPayload)
			if err != nil {
				vLog("❌ Frame parse error: %v", err)
				residual = residual[4+packetLen:]
				continue
			}

			valid := []*AVLData{}
			for _, r := range records {
				if r == nil {
					continue
				}
				if r.Latitude == 0 || r.Longitude == 0 {
					vLog("⚠️ Skipping zero coordinates: LAT=%.7f LNG=%.7f SAT=%d", r.Latitude, r.Longitude, r.Satellites)
					continue
				}
				if r.Satellites == 0 {
					vLog("⚠️ Skipping record with zero satellites: LAT=%.7f LNG=%.7f", r.Latitude, r.Longitude)
					continue
				}
				if r.Latitude < -90 || r.Latitude > 90 || r.Longitude < -180 || r.Longitude > 180 {
					vLog("⚠️ Skipping out-of-range coordinates: LAT=%.7f LNG=%.7f", r.Latitude, r.Longitude)
					continue
				}
				valid = append(valid, r)
			}

			vLog("🔎 Parsed %d valid AVL records", len(valid))

			if err := storePositionsBatch(deviceID, imei, valid); err != nil {
				vLog("❌ DB batch insert failed: %v", err)
			}

			payload := []map[string]interface{}{}
			for _, r := range valid {
				payload = append(payload, map[string]interface{}{
					"device_id":  deviceID,
					"imei":       imei,
					"timestamp":  r.Timestamp.UTC().Format(time.RFC3339),
					"latitude":   r.Latitude,
					"longitude":  r.Longitude,
					"speed":      r.Speed,
					"angle":      r.Angle,
					"altitude":   r.Altitude,
					"satellites": r.Satellites,
					"io_data":    r.IOData,
				})
			}

			_ = postPositionsToBackend(payload)
			sendACK(conn, len(valid))

			residual = residual[4+packetLen:]
		}
	}
}

// =====================================================
//                 IMEI / DEVICE HANDLING
// =====================================================

func readIMEI(conn net.Conn) (string, error) {
	buf := make([]byte, 64)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buf)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return "", err
	}

	raw := buf[:n]
	if len(raw) >= 2 && raw[0] == 0x00 && raw[1] == 0x0F {
		raw = raw[2:]
	}

	re := regexp.MustCompile(`\D`)
	imei := re.ReplaceAllString(string(raw), "")

	_, _ = conn.Write([]byte{0x01}) // ACK
	return imei, nil
}

func ensureDevice(imei string) (int, error) {
	var id int
	err := db.QueryRow("SELECT id FROM devices WHERE imei=$1", imei).Scan(&id)
	if err == nil {
		return id, nil
	}

	resp, err := httpClient.Get("https://mytrack-production.up.railway.app/api/devices/list")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var devices []Device
	if err := json.Unmarshal(body, &devices); err != nil {
		return 0, err
	}

	for _, d := range devices {
		if strings.TrimSpace(d.IMEI) == imei {
			_, _ = db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
			return d.ID, nil
		}
	}

	return 0, fmt.Errorf("device IMEI %s not found", imei)
}

// =====================================================
//                 TELTONIKA CODEC PARSING
// =====================================================

func normalizeToCodec8(frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return nil, fmt.Errorf("empty frame")
	}
	if frame[0] == 0x08 || frame[0] == 0x8E {
		return frame, nil
	}
	idx := bytes.IndexByte(frame, 0x08)
	if idx == -1 {
		idx = bytes.IndexByte(frame, 0x8E)
		if idx == -1 {
			return nil, fmt.Errorf("codec not found")
		}
	}
	return frame[idx:], nil
}

func parseCodec(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("frame too short")
	}
	reader := bytes.NewReader(data)

	var codecID byte
	_ = binary.Read(reader, binary.BigEndian, &codecID)

	var count byte
	_ = binary.Read(reader, binary.BigEndian, &count)

	records := make([]*AVLData, 0, count)
	for i := 0; i < int(count); i++ {
		rec, _ := parseSingleAVL(reader)
		if rec != nil {
			records = append(records, rec)
		}
	}
	return records, nil
}

func parseSingleAVL(r *bytes.Reader) (*AVLData, error) {
	var timestamp uint64
	_ = binary.Read(r, binary.BigEndian, &timestamp)

	nowMs := uint64(time.Now().UnixMilli())
	if timestamp == 0 || timestamp > nowMs+86400000 || timestamp < 946684800000 {
		timestamp = nowMs
	}

	var priority byte
	_ = binary.Read(r, binary.BigEndian, &priority)

	var lonRaw, latRaw int32
	_ = binary.Read(r, binary.BigEndian, &lonRaw)
	_ = binary.Read(r, binary.BigEndian, &latRaw)

	var altitude, angle uint16
	_ = binary.Read(r, binary.BigEndian, &altitude)
	_ = binary.Read(r, binary.BigEndian, &angle)

	var sats byte
	_ = binary.Read(r, binary.BigEndian, &sats)

	var speed uint16
	_ = binary.Read(r, binary.BigEndian, &speed)

	ioData, _ := parseIOElements(r)

	return &AVLData{
		Timestamp:  time.UnixMilli(int64(timestamp)),
		Latitude:   float64(latRaw) / 1e7,
		Longitude:  float64(lonRaw) / 1e7,
		Altitude:   int(altitude),
		Angle:      int(angle),
		Satellites: int(sats),
		Speed:      int(speed),
		IOData:     ioData,
	}, nil
}

func parseIOElements(r *bytes.Reader) (map[uint8]interface{}, error) {
	ioData := make(map[uint8]interface{})

	readByte := func() byte {
		var b byte
		_ = binary.Read(r, binary.BigEndian, &b)
		return b
	}
	readU16 := func() uint16 {
		var v uint16
		_ = binary.Read(r, binary.BigEndian, &v)
		return v
	}
	readU32 := func() uint32 {
		var v uint32
		_ = binary.Read(r, binary.BigEndian, &v)
		return v
	}
	readU64 := func() uint64 {
		var v uint64
		_ = binary.Read(r, binary.BigEndian, &v)
		return v
	}

	n1 := int(readByte())
	for i := 0; i < n1; i++ {
		id := readByte()
		val := readByte()
		ioData[id] = val
	}

	n2 := int(readByte())
	for i := 0; i < n2; i++ {
		id := readByte()
		val := readU16()
		ioData[id] = val
	}

	n4 := int(readByte())
	for i := 0; i < n4; i++ {
		id := readByte()
		val := readU32()
		ioData[id] = val
	}

	n8 := int(readByte())
	for i := 0; i < n8; i++ {
		id := readByte()
		val := readU64()
		ioData[id] = val
	}

	return ioData, nil
}

// =====================================================
//                 DATABASE + BACKEND
// =====================================================

func storePositionsBatch(deviceID int, imei string, recs []*AVLData) error {
	if len(recs) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var stmt *sql.Stmt
	if positionsHasIoData {
		stmt, err = tx.Prepare(`
			INSERT INTO positions 
			(device_id, lat, lng, speed, angle, altitude, satellites, timestamp, imei, io_data)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`)
	} else {
		stmt, err = tx.Prepare(`
			INSERT INTO positions 
			(device_id, lat, lng, speed, angle, altitude, satellites, timestamp, imei)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`)
	}
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range recs {
		ioJSON, _ := json.Marshal(r.IOData)

		if positionsHasIoData {
			_, err = stmt.Exec(
				deviceID, r.Latitude, r.Longitude, r.Speed, r.Angle,
				r.Altitude, r.Satellites, r.Timestamp.UTC(), imei, ioJSON,
			)
		} else {
			_, err = stmt.Exec(
				deviceID, r.Latitude, r.Longitude, r.Speed, r.Angle,
				r.Altitude, r.Satellites, r.Timestamp.UTC(), imei,
			)
		}

		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func postPositionsToBackend(positions []map[string]interface{}) error {
	if len(positions) == 0 {
		return nil
	}

	data, _ := json.Marshal(positions)

	req, _ := http.NewRequest("POST", backendTrackURL, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	vLog("📬 Backend response (%d): %.200s", resp.StatusCode, body)
	return nil
}

func sendACK(conn net.Conn, count int) {
	ack := make([]byte, 5)
	binary.BigEndian.PutUint32(ack, uint32(count))
	ack[4] = 0x01
	_, _ = conn.Write(ack)
}

// =====================================================
//                 UTILITY
// =====================================================

func getEnv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}

// =====================================================
//                 ADDED HELPERS (P13 / GT06) — NO REMOVALS
// =====================================================

func readFirstBytes(conn net.Conn) ([]byte, error) {
	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	n, err := conn.Read(buf)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func readIMEIFromFirst(first []byte, conn net.Conn) (string, error) {
	raw := first
	if len(raw) >= 2 && raw[0] == 0x00 && raw[1] == 0x0F {
		raw = raw[2:]
	}
	re := regexp.MustCompile(`\D`)
	imei := re.ReplaceAllString(string(raw), "")
	if len(imei) < 10 {
		return "", fmt.Errorf("could not extract Teltonika IMEI from first bytes: %s", strings.ToUpper(hex.EncodeToString(first)))
	}
	_, _ = conn.Write([]byte{0x01}) // Teltonika ACK
	return imei, nil
}

func looksLikeGT06(b []byte) bool {
	return len(b) >= 2 && ((b[0] == 0x78 && b[1] == 0x78) || (b[0] == 0x79 && b[1] == 0x79))
}

func handleGT06Loop(conn net.Conn, imei string, deviceID int, initial []byte) {
	residual := make([]byte, 0, 8192)
	tmp := make([]byte, 4096)

	if len(initial) > 0 {
		residual = append(residual, initial...)
	}

	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		n, err := conn.Read(tmp)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				vLog("⏱ Read timeout for %s, closing connection", imei)
			} else if err != io.EOF {
				vLog("🔌 Read error for %s: %v", imei, err)
			}
			return
		}

		if n > 0 {
			vLog("🟢 Raw TCP bytes (GT06): %s", strings.ToUpper(hex.EncodeToString(tmp[:n])))
			residual = append(residual, tmp[:n]...)
		}

		for {
			frame, rest, ok := extractGT06Frame(residual)
			if !ok {
				break
			}
			residual = rest

			proto := gt06Protocol(frame)
			serial := gt06Serial(frame)

			vLog("📦 GT06 frame proto=0x%02X len=%d data=%s", proto, len(frame), strings.ToUpper(hex.EncodeToString(frame)))

			switch proto {
			case 0x13: // P13 heartbeat
				ack := buildGT06Ack(proto, serial)
				_, _ = conn.Write(ack)
				vLog("✅ Sent P13 ACK: %s", strings.ToUpper(hex.EncodeToString(ack)))
			default:
				// keep device alive (many expect ack for other types too)
				if serial != nil {
					ack := buildGT06Ack(proto, serial)
					_, _ = conn.Write(ack)
					vLog("✅ Sent ACK proto=0x%02X: %s", proto, strings.ToUpper(hex.EncodeToString(ack)))
				}
			}

			// NOTE: We are only reading/ACKing P13 here.
			// If you paste a location frame proto (often 0x12 or 0x16),
			// I’ll add GPS decoding without touching Teltonika logic.
			_ = deviceID
		}
	}
}

func gt06LoginAndGetIMEI(conn net.Conn, first []byte) (string, []byte, error) {
	buf := append([]byte(nil), first...)
	deadline := time.Now().Add(10 * time.Second)

	for {
		frame, rest, ok := extractGT06Frame(buf)
		if ok {
			proto := gt06Protocol(frame)
			serial := gt06Serial(frame)

			if proto == 0x01 {
				imei := gt06IMEI(frame)
				if imei == "" {
					return "", nil, fmt.Errorf("GT06 login frame but IMEI not parsed: %s", strings.ToUpper(hex.EncodeToString(frame)))
				}
				ack := buildGT06Ack(0x01, serial)
				_, _ = conn.Write(ack)
				vLog("✅ Sent LOGIN ACK: %s", strings.ToUpper(hex.EncodeToString(ack)))
				return imei, rest, nil
			}

			// If first is heartbeat etc, ACK and keep waiting for login
			if serial != nil {
				ack := buildGT06Ack(proto, serial)
				_, _ = conn.Write(ack)
				vLog("✅ Sent pre-login ACK proto=0x%02X: %s", proto, strings.ToUpper(hex.EncodeToString(ack)))
			}

			buf = rest
			continue
		}

		if time.Now().After(deadline) {
			return "", nil, fmt.Errorf("timeout waiting for GT06 login; buffered=%s", strings.ToUpper(hex.EncodeToString(buf)))
		}

		tmp := make([]byte, 2048)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := conn.Read(tmp)
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return "", nil, err
		}
		buf = append(buf, tmp[:n]...)
	}
}

func extractGT06Frame(buf []byte) ([]byte, []byte, bool) {
	start := -1
	for i := 0; i+1 < len(buf); i++ {
		if (buf[i] == 0x78 && buf[i+1] == 0x78) || (buf[i] == 0x79 && buf[i+1] == 0x79) {
			start = i
			break
		}
	}
	if start == -1 {
		return nil, buf, false
	}
	if start > 0 {
		buf = buf[start:]
	}

	if len(buf) < 5 {
		return nil, buf, false
	}

	if buf[0] == 0x78 && buf[1] == 0x78 {
		l := int(buf[2])
		total := l + 5
		if l <= 0 || total > 16384 || len(buf) < total {
			return nil, buf, false
		}
		return buf[:total], buf[total:], true
	}

	if buf[0] == 0x79 && buf[1] == 0x79 {
		if len(buf) < 6 {
			return nil, buf, false
		}
		l := int(binary.BigEndian.Uint16(buf[2:4]))
		total := l + 7
		if l <= 0 || total > 16384 || len(buf) < total {
			return nil, buf, false
		}
		return buf[:total], buf[total:], true
	}

	return nil, buf, false
}

func gt06Protocol(frame []byte) byte {
	if len(frame) < 5 {
		return 0
	}
	if frame[0] == 0x78 && frame[1] == 0x78 {
		return frame[3]
	}
	if frame[0] == 0x79 && frame[1] == 0x79 {
		return frame[4]
	}
	return 0
}

func gt06Serial(frame []byte) []byte {
	if len(frame) < 10 {
		return nil
	}
	return []byte{frame[len(frame)-6], frame[len(frame)-5]}
}

func gt06IMEI(frame []byte) string {
	if gt06Protocol(frame) != 0x01 {
		return ""
	}
	if len(frame) >= 12 && frame[0] == 0x78 && frame[1] == 0x78 {
		return decodeBCDIMEI(frame[4:12])
	}
	if len(frame) >= 13 && frame[0] == 0x79 && frame[1] == 0x79 {
		return decodeBCDIMEI(frame[5:13])
	}
	return ""
}

func decodeBCDIMEI(bcd []byte) string {
	s := ""
	for _, v := range bcd {
		s += fmt.Sprintf("%02X", v)
	}
	return strings.TrimLeft(s, "0")
}

func buildGT06Ack(proto byte, serial []byte) []byte {
	if len(serial) != 2 {
		serial = []byte{0x00, 0x01}
	}
	ack := []byte{0x78, 0x78, 0x05, proto, serial[0], serial[1]}
	crc := crcITU(ack[2:])
	ack = append(ack, byte(crc>>8), byte(crc), 0x0D, 0x0A)
	return ack
}

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
