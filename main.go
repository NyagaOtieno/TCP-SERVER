// main.go
package main

import (
	"bufio"
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
	"strconv"
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
	IOData     map[uint16]interface{} // ✅ supports Codec8 + Codec8E
}

type Device struct {
	ID   int    `json:"id"`
	IMEI string `json:"imei"`
}

var (
	tcpServerHost      string
	backendTrackURL    string
	db                 *sql.DB
	httpClient         = &http.Client{Timeout: 12 * time.Second}
	wg                 sync.WaitGroup
	positionsHasIoData bool
	verbose            = true
)

// =========================
// Boot
// =========================

func vLog(format string, a ...interface{}) {
	if verbose {
		log.Printf(format, a...)
	}
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	_ = godotenv.Load()

	// Railway sets PORT; keep TCP_SERVER_HOST override if you use it
	port := getEnv("PORT", "5027")
	tcpServerHost = getEnv("TCP_SERVER_HOST", "0.0.0.0:"+port)

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
	vLog("🚀 Starting TCP tracker server (Teltonika FMB920 + UniGuard)...")

	ln, err := net.Listen("tcp", tcpServerHost)
	if err != nil {
		log.Fatalf("❌ Failed to start TCP server: %v", err)
	}
	defer ln.Close()

	vLog("✅ TCP Server listening on %s", tcpServerHost)

	for {
		conn, err := ln.Accept()
		if err != nil {
			vLog("⚠️ Accept error: %v", err)
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			defer func() {
				if r := recover(); r != nil {
					vLog("🔥 Panic recovered: %v", r)
				}
			}()
			handleConnection(c)
		}(conn)
	}
}

// =========================
// Connection Router
// =========================

func handleConnection(conn net.Conn) {
	remote := conn.RemoteAddr().String()
	vLog("🔗 New connection from %s", remote)

	br := bufio.NewReaderSize(conn, 128*1024)

	// Wait for first bytes (some devices are slow)
	conn.SetReadDeadline(time.Now().Add(25 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	var peek []byte
	for i := 0; i < 6; i++ {
		p, err := br.Peek(64)
		if err == nil && len(p) > 0 {
			peek = p
			break
		}
		time.Sleep(2 * time.Second)
	}

	if len(peek) == 0 {
		vLog("⚠️ No data received on connect from %s", remote)
		return
	}

	vLog("👀 First bytes HEX (%d): %s", min(len(peek), 64), hex.EncodeToString(peek[:min(len(peek), 64)]))
	vLog("👀 First bytes ASCII: %q", sanitizeASCII(peek[:min(len(peek), 64)]))

	if looksLikeUniGuard(peek) {
		vLog("🧭 Protocol guess from %s: UNIGUARD", remote)
		handleUniGuard(br, conn)
		return
	}

	// Default to Teltonika
	vLog("🧭 Protocol guess from %s: TELTONIKA", remote)
	handleTeltonika(br, conn)
}

func looksLikeUniGuard(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	// UniGuard messages are ASCII with '#' and end with '$'
	if (b[0] == 'S' || b[0] == 's') && bytes.Contains(b, []byte("#")) {
		return true
	}
	if bytes.Contains(b, []byte("#")) && bytes.Contains(b, []byte("$")) {
		return true
	}
	return false
}

func sanitizeASCII(b []byte) string {
	out := make([]byte, 0, len(b))
	for _, c := range b {
		if c >= 32 && c <= 126 {
			out = append(out, c)
		} else {
			out = append(out, '.')
		}
	}
	return string(out)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// =========================
// TELTONIKA (FMB920) handler
// =========================

func handleTeltonika(br *bufio.Reader, conn net.Conn) {
	remote := conn.RemoteAddr().String()

	imei, err := readTeltonikaIMEI(br, conn)
	if err != nil {
		vLog("❌ Failed IMEI read (Teltonika) from %s: %v", remote, err)
		return
	}
	vLog("📡 Teltonika device connected: %s", imei)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
		return
	}

	residual := make([]byte, 0, 8192)
	tmp := make([]byte, 4096)

	for {
		conn.SetReadDeadline(time.Now().Add(15 * time.Minute))
		n, err := br.Read(tmp)
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

		for {
			// Teltonika framing: 4B preamble(0) + 4B dataLen + data + 4B CRC
			if len(residual) < 12 {
				break
			}

			// resync to preamble 00000000
			if binary.BigEndian.Uint32(residual[:4]) != 0 {
				idx := bytes.Index(residual, []byte{0x00, 0x00, 0x00, 0x00})
				if idx == -1 {
					// keep last few bytes for overlap
					if len(residual) > 3 {
						residual = residual[len(residual)-3:]
					}
					break
				}
				residual = residual[idx:]
				if len(residual) < 12 {
					break
				}
			}

			dataLen := int(binary.BigEndian.Uint32(residual[4:8]))
			if dataLen <= 0 || dataLen > 5*1024*1024 {
				vLog("⚠️ Invalid Teltonika dataLen=%d from %s (resync)", dataLen, imei)
				residual = residual[1:]
				continue
			}

			total := 8 + dataLen + 4 // preamble+len + data + crc
			if len(residual) < total {
				break
			}

			data := residual[8 : 8+dataLen]

			codecPayload, err := normalizeToCodec(data)
			if err != nil {
				vLog("❌ Codec normalization failed: %v", err)
				residual = residual[total:]
				continue
			}

			records, err := parseCodec(codecPayload)
			if err != nil {
				vLog("❌ Frame parse error: %v", err)
				residual = residual[total:]
				continue
			}

			valid := filterValidPositions(records, imei)

			vLog("🔎 Parsed %d valid AVL records", len(valid))

			if err := storePositionsBatch(deviceID, imei, valid); err != nil {
				vLog("❌ DB batch insert failed: %v", err)
			}

			payload := make([]map[string]interface{}, 0, len(valid))
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

			// ✅ Teltonika ACK: 4 bytes number of records accepted
			sendTeltonikaACK(conn, len(valid))

			residual = residual[total:]
		}
	}
}

func readTeltonikaIMEI(br *bufio.Reader, conn net.Conn) (string, error) {
	conn.SetReadDeadline(time.Now().Add(12 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	p2, err := br.Peek(2)
	if err != nil {
		return "", err
	}

	// Standard: 00 0F + 15 digit IMEI ASCII
	if len(p2) >= 2 && p2[0] == 0x00 && p2[1] == 0x0F {
		_, _ = br.ReadByte()
		_, _ = br.ReadByte()

		imeiBytes := make([]byte, 15)
		if _, err := io.ReadFull(br, imeiBytes); err != nil {
			return "", err
		}
		imei := strings.TrimSpace(string(imeiBytes))
		if !regexp.MustCompile(`^\d{15}$`).MatchString(imei) {
			return "", fmt.Errorf("invalid IMEI: %q", imei)
		}
		_, _ = conn.Write([]byte{0x01})
		return imei, nil
	}

	// Fallback: plain 15 digits
	p, _ := br.Peek(32)
	if len(p) >= 15 && regexp.MustCompile(`^\d{15}$`).Match(p[:15]) {
		imei := string(p[:15])
		_, _ = br.Discard(15)
		_, _ = conn.Write([]byte{0x01})
		return imei, nil
	}

	return "", fmt.Errorf("not a Teltonika IMEI handshake")
}

func sendTeltonikaACK(conn net.Conn, accepted int) {
	ack := make([]byte, 4)
	binary.BigEndian.PutUint32(ack, uint32(accepted))
	_, _ = conn.Write(ack)
}

// =========================
// UNIGUARD handler
// =========================

func handleUniGuard(br *bufio.Reader, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	vLog("📡 UniGuard connection from %s", remote)

	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Minute))
		msg, err := br.ReadString('$') // UniGuard frame end
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				vLog("⏱ UniGuard timeout, closing %s", remote)
				return
			}
			if err != io.EOF {
				vLog("🔌 UniGuard read error: %v", err)
			}
			return
		}

		msg = strings.TrimSpace(msg)
		if msg == "" {
			continue
		}
		vLog("🟢 UniGuard raw: %s", msg)

		imei, serialHex, rec, ok := parseUniGuardMessage(msg)
		if !ok {
			vLog("⚠️ UniGuard parse failed (no usable GDATA)")
			continue
		}

		deviceID, err := ensureDevice(imei)
		if err != nil {
			vLog("❌ Device lookup failed for UniGuard IMEI %s: %v", imei, err)
			continue
		}

		valid := filterValidPositions([]*AVLData{rec}, imei)
		vLog("🔎 UniGuard parsed %d valid records", len(valid))

		if err := storePositionsBatch(deviceID, imei, valid); err != nil {
			vLog("❌ DB insert failed (UniGuard): %v", err)
		}

		payload := make([]map[string]interface{}, 0, len(valid))
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

		// UniGuard ACK format used by many UniGuard docs
		ack := buildUniGuardAck("S168", imei, serialHex, "LOCA")
		_, _ = conn.Write([]byte(ack))
	}
}

func parseUniGuardMessage(msg string) (imei string, serialHex string, rec *AVLData, ok bool) {
	msg = strings.TrimSpace(strings.TrimSuffix(msg, "$"))
	parts := strings.Split(msg, "#")
	if len(parts) < 5 {
		return "", "", nil, false
	}
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	imei = parts[1]
	serialHex = parts[2]
	content := strings.TrimSpace(strings.Join(parts[4:], "#"))
	if imei == "" || serialHex == "" || content == "" {
		return "", "", nil, false
	}

	// Look for "GDATA:" segment
	gdata := ""
	for _, s := range strings.Split(content, ";") {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(strings.ToUpper(s), "GDATA:") {
			gdata = strings.TrimSpace(s[len("GDATA:"):])
			break
		}
	}
	if gdata == "" {
		return "", "", nil, false
	}

	fields := splitCSVLoose(gdata)
	if len(fields) < 8 {
		return "", "", nil, false
	}

	sats, _ := strconv.Atoi(fields[1])
	tRaw := fields[2] // often YYMMDDhhmmss...
	lat, _ := strconv.ParseFloat(fields[3], 64)
	lon, _ := strconv.ParseFloat(fields[4], 64)
	speedF, _ := strconv.ParseFloat(fields[5], 64)
	headingF, _ := strconv.ParseFloat(fields[6], 64)
	altF, _ := strconv.ParseFloat(fields[7], 64)

	ts := time.Now().UTC()
	// try YYMMDDhhmmss
	if len(tRaw) >= 12 {
		if t, err := time.ParseInLocation("060102150405", tRaw[:12], time.UTC); err == nil {
			ts = t
		}
	}

	rec = &AVLData{
		Timestamp:  ts,
		Latitude:   lat,
		Longitude:  lon,
		Altitude:   int(altF),
		Angle:      int(headingF),
		Satellites: sats,
		Speed:      int(speedF + 0.5),
		IOData:     map[uint16]interface{}{},
	}
	return imei, serialHex, rec, true
}

func splitCSVLoose(s string) []string {
	raw := strings.Split(s, ",")
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		t := strings.TrimSpace(r)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func buildUniGuardAck(id, imei, serialHex, keyword string) string {
	content := "ACK^" + keyword
	lengthHex := fmt.Sprintf("%04x", len(content))
	return fmt.Sprintf("%s#%s#%s#%s#%s$", id, imei, serialHex, lengthHex, content)
}

// =========================
// Validations
// =========================

func filterValidPositions(records []*AVLData, imei string) []*AVLData {
	valid := make([]*AVLData, 0, len(records))
	for _, r := range records {
		if r == nil {
			continue
		}

		// Skip zero coords
		if r.Latitude == 0 || r.Longitude == 0 {
			vLog("⚠️ Skipping zero coordinates (imei=%s): LAT=%.7f LNG=%.7f SAT=%d", imei, r.Latitude, r.Longitude, r.Satellites)
			continue
		}

		// Skip records without satellites
		if r.Satellites == 0 {
			vLog("⚠️ Skipping record with zero satellites (imei=%s): LAT=%.7f LNG=%.7f", imei, r.Latitude, r.Longitude)
			continue
		}

		// Skip out-of-range
		if r.Latitude < -90 || r.Latitude > 90 || r.Longitude < -180 || r.Longitude > 180 {
			vLog("⚠️ Skipping out-of-range coordinates (imei=%s): LAT=%.7f LNG=%.7f", imei, r.Latitude, r.Longitude)
			continue
		}

		valid = append(valid, r)
	}
	return valid
}

// =========================
// Device lookup
// =========================

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

// =========================
// Teltonika Codec parsing (Codec8 + Codec8E)
// =========================

func normalizeToCodec(frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return nil, fmt.Errorf("empty frame")
	}
	// codec at start
	if frame[0] == 0x08 || frame[0] == 0x8E {
		return frame, nil
	}
	// try to find codec byte in data
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
	r := bytes.NewReader(data)

	var codecID byte
	_ = binary.Read(r, binary.BigEndian, &codecID)

	var count byte
	_ = binary.Read(r, binary.BigEndian, &count)

	records := make([]*AVLData, 0, count)
	for i := 0; i < int(count); i++ {
		rec, _ := parseSingleAVL(r, codecID)
		if rec != nil {
			records = append(records, rec)
		}
	}

	// second record count byte exists in Teltonika payload
	var count2 byte
	_ = binary.Read(r, binary.BigEndian, &count2)

	return records, nil
}

func parseSingleAVL(r *bytes.Reader, codecID byte) (*AVLData, error) {
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

	ioData, _ := parseIOElementsByCodec(r, codecID)

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

func parseIOElementsByCodec(r *bytes.Reader, codecID byte) (map[uint16]interface{}, error) {
	if codecID == 0x8E {
		return parseIOElementsCodec8E(r)
	}
	return parseIOElementsCodec8(r)
}

// Codec8: 1-byte IO IDs, 1-byte group counts
func parseIOElementsCodec8(r *bytes.Reader) (map[uint16]interface{}, error) {
	ioData := make(map[uint16]interface{})

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

	// event IO + total IO
	_ = readByte()
	_ = readByte()

	n1 := int(readByte())
	for i := 0; i < n1; i++ {
		id := uint16(readByte())
		val := readByte()
		ioData[id] = val
	}

	n2 := int(readByte())
	for i := 0; i < n2; i++ {
		id := uint16(readByte())
		val := readU16()
		ioData[id] = val
	}

	n4 := int(readByte())
	for i := 0; i < n4; i++ {
		id := uint16(readByte())
		val := readU32()
		ioData[id] = val
	}

	n8 := int(readByte())
	for i := 0; i < n8; i++ {
		id := uint16(readByte())
		val := readU64()
		ioData[id] = val
	}

	return ioData, nil
}

// Codec8E: 2-byte IO IDs, 2-byte group counts
func parseIOElementsCodec8E(r *bytes.Reader) (map[uint16]interface{}, error) {
	ioData := make(map[uint16]interface{})

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
	readBytes := func(n int) []byte {
		b := make([]byte, n)
		_, _ = io.ReadFull(r, b)
		return b
	}

	// event IO id (2) + total IO (2)
	_ = readU16()
	_ = readU16()

	n1 := int(readU16())
	for i := 0; i < n1; i++ {
		id := readU16()
		val := readBytes(1)[0]
		ioData[id] = val
	}

	n2 := int(readU16())
	for i := 0; i < n2; i++ {
		id := readU16()
		val := readU16()
		ioData[id] = val
	}

	n4 := int(readU16())
	for i := 0; i < n4; i++ {
		id := readU16()
		val := readU32()
		ioData[id] = val
	}

	n8 := int(readU16())
	for i := 0; i < n8; i++ {
		id := readU16()
		val := readU64()
		ioData[id] = val
	}

	return ioData, nil
}

// =========================
// DB + Backend
// =========================

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

// =========================
// Utility
// =========================

func getEnv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}
