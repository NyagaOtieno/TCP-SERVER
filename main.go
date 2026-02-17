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

/*
	✅ Supports:
	  1) Teltonika FMB920 (Codec8 / Codec8E) over TCP with proper framing + ACK
	  2) UniGuard ASCII protocol (Sxxx#IMEI#SERIAL#LEN#...$) with GDATA parsing + ACK

	Notes:
	  - Railway/Cloud: uses PORT env automatically (still honors TCP_SERVER_HOST if set)
	  - Teltonika ACK is 4 bytes record count (NOT 5 bytes)
	  - Teltonika framing is: 00000000 + dataLen(4) + data + CRC(4)
*/

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
	httpClient         = &http.Client{Timeout: 12 * time.Second}
	wg                 sync.WaitGroup
	positionsHasIoData bool
	verbose            = true
)

// =========================
// Logging
// =========================

func vLog(format string, a ...interface{}) {
	if verbose {
		log.Printf(format, a...)
	}
}

// =========================
// Init
// =========================

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	_ = godotenv.Load()

	// Railway often sets PORT. If TCP_SERVER_HOST is provided, it wins.
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

// =========================
// Main
// =========================

func main() {
	vLog("🚀 Starting TCP tracker server (Teltonika FMB920 + UniGuard)...")

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
}

// =========================
// Protocol Detection
// =========================

type ProtocolKind int

const (
	ProtoTeltonika ProtocolKind = iota
	ProtoUniGuard
)

func (p ProtocolKind) String() string {
	switch p {
	case ProtoUniGuard:
		return "UNIGUARD"
	default:
		return "TELTONIKA"
	}
}

func detectProtocolPeek(peek []byte) ProtocolKind {
	// UniGuard is ASCII and commonly begins with 'S' and contains '#'
	if len(peek) > 0 && (peek[0] == 'S' || peek[0] == 's') && bytes.Contains(peek, []byte("#")) {
		return ProtoUniGuard
	}
	// Fallback: if it contains '#', likely UniGuard
	if bytes.Contains(peek, []byte("#")) && bytes.Contains(peek, []byte("$")) {
		return ProtoUniGuard
	}
	return ProtoTeltonika
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

// =========================
// Connection Handling
// =========================

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	vLog("🔗 New connection from %s", remote)

	br := bufio.NewReaderSize(conn, 128*1024)

	// Peek first bytes for protocol selection
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	peek, _ := br.Peek(64)
	_ = conn.SetReadDeadline(time.Time{})

	if len(peek) > 0 {
		vLog("👀 First bytes HEX (%d): %s", len(peek), hex.EncodeToString(peek))
		vLog("👀 First bytes ASCII: %q", sanitizeASCII(peek))
	} else {
		vLog("⚠️ No data received on connect from %s", remote)
		// still continue; some devices are slow
	}

	kind := detectProtocolPeek(peek)
	vLog("🧭 Protocol guess from %s: %s", remote, kind.String())

	switch kind {
	case ProtoUniGuard:
		handleUniGuard(br, conn)
		return
	default:
		handleTeltonika(br, conn)
		return
	}
}

// =========================
// TELTONIKA (FMB920) HANDLER
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

	// Teltonika streaming parser
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
			// Teltonika framing needs at least 8 bytes:
			// preamble (4) + dataLen (4)
			if len(residual) < 8 {
				break
			}

			// Resync to preamble 00000000
			if binary.BigEndian.Uint32(residual[:4]) != 0 {
				idx := bytes.Index(residual, []byte{0x00, 0x00, 0x00, 0x00})
				if idx == -1 {
					// keep last few bytes to allow resync
					if len(residual) > 3 {
						residual = residual[len(residual)-3:]
					}
					break
				}
				residual = residual[idx:]
				if len(residual) < 8 {
					break
				}
			}

			dataLen := int(binary.BigEndian.Uint32(residual[4:8]))
			if dataLen <= 0 || dataLen > 5*1024*1024 {
				vLog("⚠️ Invalid Teltonika dataLen=%d from %s (resync)", dataLen, imei)
				residual = residual[1:]
				continue
			}

			totalLen := 8 + dataLen + 4 // + CRC
			if len(residual) < totalLen {
				break
			}

			data := residual[8 : 8+dataLen]
			// crc := residual[8+dataLen : totalLen] // available if you want to validate later

			codecPayload, err := normalizeToCodec8(data)
			if err != nil {
				vLog("❌ Codec normalization failed: %v", err)
				residual = residual[totalLen:]
				continue
			}

			records, err := parseCodec(codecPayload)
			if err != nil {
				vLog("❌ Frame parse error: %v", err)
				residual = residual[totalLen:]
				continue
			}

			valid := filterValidPositions(records, imei)
			vLog("🔎 Parsed %d valid AVL records", len(valid))

			if err := storePositionsBatch(deviceID, imei, valid); err != nil {
				vLog("❌ DB batch insert failed: %v", err)
			}

			// Post to backend
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

			// ✅ Correct Teltonika ACK: 4 bytes accepted record count
			sendTeltonikaACK(conn, len(valid))

			residual = residual[totalLen:]
		}
	}
}

func readTeltonikaIMEI(br *bufio.Reader, conn net.Conn) (string, error) {
	// Teltonika IMEI packet: 00 0F + 15 ASCII digits
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	p2, err := br.Peek(2)
	if err != nil {
		return "", err
	}
	if len(p2) < 2 || p2[0] != 0x00 || p2[1] != 0x0F {
		// Some devices may send plain digits; try that too (15 digits)
		p, _ := br.Peek(32)
		if len(p) >= 15 && regexp.MustCompile(`^\d{15}$`).Match(p[:15]) {
			imei := string(p[:15])
			_, _ = br.Discard(15)
			_, _ = conn.Write([]byte{0x01})
			return imei, nil
		}
		return "", fmt.Errorf("not a Teltonika IMEI handshake")
	}

	_, _ = br.ReadByte() // 00
	_, _ = br.ReadByte() // 0F

	imeiBytes := make([]byte, 15)
	if _, err := io.ReadFull(br, imeiBytes); err != nil {
		return "", err
	}

	imei := strings.TrimSpace(string(imeiBytes))
	if !regexp.MustCompile(`^\d{15}$`).MatchString(imei) {
		return "", fmt.Errorf("invalid IMEI: %q", imei)
	}

	// Teltonika ACK after IMEI
	_, _ = conn.Write([]byte{0x01})
	return imei, nil
}

func sendTeltonikaACK(conn net.Conn, accepted int) {
	ack := make([]byte, 4)
	binary.BigEndian.PutUint32(ack, uint32(accepted))
	_, _ = conn.Write(ack)
}

// =========================
// UNIGUARD HANDLER
// =========================

func handleUniGuard(br *bufio.Reader, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	vLog("📡 UniGuard connection from %s", remote)

	// UniGuard is line-like messages ending with '$'
	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Minute))

		msg, err := br.ReadString('$')
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
		if len(valid) == 0 {
			// still ACK so device doesn't keep retrying forever
			ack := buildUniGuardAck("S168", imei, serialHex, "LOCA")
			_, _ = conn.Write([]byte(ack))
			continue
		}

		if err := storePositionsBatch(deviceID, imei, valid); err != nil {
			vLog("❌ DB insert failed (UniGuard): %v", err)
		}

		payload := []map[string]interface{}{
			{
				"device_id":  deviceID,
				"imei":       imei,
				"timestamp":  valid[0].Timestamp.UTC().Format(time.RFC3339),
				"latitude":   valid[0].Latitude,
				"longitude":  valid[0].Longitude,
				"speed":      valid[0].Speed,
				"angle":      valid[0].Angle,
				"altitude":   valid[0].Altitude,
				"satellites": valid[0].Satellites,
				"io_data":    valid[0].IOData,
			},
		}
		_ = postPositionsToBackend(payload)

		// ACK
		ack := buildUniGuardAck("S168", imei, serialHex, "LOCA")
		_, _ = conn.Write([]byte(ack))
	}
}

func parseUniGuardMessage(msg string) (imei string, serialHex string, rec *AVLData, ok bool) {
	msg = strings.TrimSpace(msg)
	msg = strings.TrimSuffix(msg, "$")

	parts := strings.Split(msg, "#")
	if len(parts) < 5 {
		return "", "", nil, false
	}
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	// Sxxx#IMEI#SERIAL#LEN#CONTENT
	imei = parts[1]
	serialHex = parts[2]
	content := strings.TrimSpace(strings.Join(parts[4:], "#"))
	if imei == "" || serialHex == "" || content == "" {
		return "", "", nil, false
	}

	// Find "GDATA:" block (common in UniGuard protocol docs)
	// Example: ...;GDATA:A,12,160412154800,22.564025,113.242329,5.5,152,900;...
	gdata := ""
	segments := strings.Split(content, ";")
	for _, s := range segments {
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

	// fields: [A, sats, yymmddhhmmss, lat, lon, speed, heading, altitude]
	sats, _ := strconv.Atoi(fields[1])
	tRaw := fields[2]
	lat, _ := strconv.ParseFloat(fields[3], 64)
	lon, _ := strconv.ParseFloat(fields[4], 64)
	speedF, _ := strconv.ParseFloat(fields[5], 64)
	headingF, _ := strconv.ParseFloat(fields[6], 64)
	altF, _ := strconv.ParseFloat(fields[7], 64)

	ts := time.Now().UTC()
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
		IOData:     map[uint8]interface{}{},
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
	// Common ACK style: S168#<imei>#<serial>#<lenHex>#ACK^<keyword>$
	content := "ACK^" + keyword
	lengthHex := fmt.Sprintf("%04x", len(content))
	return fmt.Sprintf("%s#%s#%s#%s#%s$", id, imei, serialHex, lengthHex, content)
}

// =========================
// Shared Validations
// =========================

func filterValidPositions(records []*AVLData, imei string) []*AVLData {
	valid := make([]*AVLData, 0, len(records))
	for _, r := range records {
		if r == nil {
			continue
		}

		// Skip zero coordinates
		if r.Latitude == 0 || r.Longitude == 0 {
			vLog("⚠️ Skipping zero coordinates (imei=%s): LAT=%.7f LNG=%.7f SAT=%d", imei, r.Latitude, r.Longitude, r.Satellites)
			continue
		}

		// Skip records without satellites
		if r.Satellites == 0 {
			vLog("⚠️ Skipping record with zero satellites (imei=%s): LAT=%.7f LNG=%.7f", imei, r.Latitude, r.Longitude)
			continue
		}

		// Skip out-of-range coordinates
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
// Teltonika Codec parsing
// =========================

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

	// Teltonika IO: eventIO (1), totalIO (1), then groups
	_ = readByte()
	_ = readByte()

	// 1-byte values
	n1 := int(readByte())
	for i := 0; i < n1; i++ {
		id := readByte()
		val := readByte()
		ioData[id] = val
	}

	// 2-byte values
	n2 := int(readByte())
	for i := 0; i < n2; i++ {
		id := readByte()
		val := readU16()
		ioData[id] = val
	}

	// 4-byte values
	n4 := int(readByte())
	for i := 0; i < n4; i++ {
		id := readByte()
		val := readU32()
		ioData[id] = val
	}

	// 8-byte values
	n8 := int(readByte())
	for i := 0; i < n8; i++ {
		id := readByte()
		val := readU64()
		ioData[id] = val
	}

	return ioData, nil
}

// =========================
// Database + Backend
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
