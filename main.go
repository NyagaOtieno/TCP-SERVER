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
	vLog("🚀 Starting TCP tracker server (Teltonika + GT06 + UniGuard)...")

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

type ProtocolKind int

const (
	ProtoTeltonika ProtocolKind = iota
	ProtoGT06
	ProtoUniGuard
)

func detectProtocolPeek(br *bufio.Reader) ProtocolKind {
	peek, _ := br.Peek(8)
	if len(peek) >= 2 && peek[0] == 0x78 && peek[1] == 0x78 {
		return ProtoGT06
	}
	// UniGuard is ASCII starting with S168 (or Sxxx) and ends with $
	if len(peek) >= 1 && (peek[0] == 'S' || peek[0] == 's') {
		// likely UniGuard if we see a '#'
		if bytes.Contains(peek, []byte("#")) {
			return ProtoUniGuard
		}
	}
	// Default: Teltonika IMEI handshake (0x000F prefix or ASCII digits)
	return ProtoTeltonika
}

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	vLog("🔗 New connection from %s", remote)

	br := bufio.NewReaderSize(conn, 64*1024)

	kind := detectProtocolPeek(br)
	switch kind {
	case ProtoGT06:
		handleGT06(br, conn)
	case ProtoUniGuard:
		handleUniGuard(br, conn)
	default:
		handleTeltonika(br, conn)
	}
}

// =====================================================
//                 TELTONIKA (YOUR EXISTING FLOW)
// =====================================================

func handleTeltonika(br *bufio.Reader, conn net.Conn) {
	remote := conn.RemoteAddr().String()

	imei, err := readIMEIFromReader(br, conn)
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

	residual := make([]byte, 0)
	tmp := make([]byte, 4096)

	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
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

				// Skip zero coordinates
				if r.Latitude == 0 || r.Longitude == 0 {
					vLog("⚠️ Skipping zero coordinates: LAT=%.7f LNG=%.7f SAT=%d", r.Latitude, r.Longitude, r.Satellites)
					continue
				}

				// Skip records without satellites
				if r.Satellites == 0 {
					vLog("⚠️ Skipping record with zero satellites: LAT=%.7f LNG=%.7f", r.Latitude, r.Longitude)
					continue
				}

				// Skip out-of-range coordinates
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

			// Post to backend
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
//                 GT06 HANDLER (0x78 0x78 ... 0x0D0A)
// =====================================================

func handleGT06(br *bufio.Reader, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	vLog("📡 GT06 connection from %s", remote)

	// First packet should be login (protocol 0x01) containing Terminal ID (IMEI in BCD-like bytes).
	imei, serial, err := readGT06Login(br)
	if err != nil {
		vLog("❌ GT06 login read failed: %v", err)
		return
	}
	vLog("✅ GT06 device login IMEI=%s serial=0x%04X", imei, serial)

	// respond login ACK
	if err := writeGT06Ack(conn, 0x01, serial); err != nil {
		vLog("⚠️ GT06 login ACK write failed: %v", err)
	}

	deviceID, err := ensureDevice(imei)
	if err != nil {
		vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
		return
	}

	for {
		conn.SetReadDeadline(time.Now().Add(10 * time.Minute))
		pkt, proto, serial, err := readGT06Packet(br)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				vLog("⏱ GT06 timeout for %s, closing", imei)
				return
			}
			if err != io.EOF {
				vLog("🔌 GT06 read error for %s: %v", imei, err)
			}
			return
		}

		switch proto {
		case 0x12: // location
			rec, ok := parseGT06Location(pkt)
			if ok {
				processOnePosition(deviceID, imei, rec)
			}
			_ = writeGT06Ack(conn, proto, serial)

		case 0x16: // alarm packet (contains GPS fields same order, plus extra)
			rec, ok := parseGT06Location(pkt)
			if ok {
				processOnePosition(deviceID, imei, rec)
			}
			_ = writeGT06Ack(conn, proto, serial)

		case 0x13: // heartbeat/status
			// ACK heartbeat to keep link stable
			_ = writeGT06Ack(conn, proto, serial)

		case 0x15, 0x1A:
			// string / query packets - ACK to be safe
			_ = writeGT06Ack(conn, proto, serial)

		default:
			// Unknown protocol - still ACK with same proto
			vLog("ℹ️ GT06 proto 0x%02X len=%d", proto, len(pkt))
			_ = writeGT06Ack(conn, proto, serial)
		}
	}
}

// readGT06Login reads the first GT06 login packet and returns IMEI + serial.
func readGT06Login(br *bufio.Reader) (string, uint16, error) {
	pkt, proto, serial, err := readGT06Packet(br)
	if err != nil {
		return "", 0, err
	}
	if proto != 0x01 {
		return "", 0, fmt.Errorf("expected GT06 login proto 0x01, got 0x%02X", proto)
	}
	// payload: start(2) + len(1) + proto(1) + terminalID(8) + serial(2) + crc(2) + stop(2)
	// readGT06Packet returns the full raw packet bytes.
	if len(pkt) < 2+1+1+8+2+2+2 {
		return "", 0, fmt.Errorf("login packet too short: %d", len(pkt))
	}
	terminalID := pkt[2+1+1 : 2+1+1+8] // 8 bytes
	imei := gt06TerminalIDToIMEI(terminalID)
	return imei, serial, nil
}

func readGT06Packet(br *bufio.Reader) ([]byte, byte, uint16, error) {
	// Scan until start 0x78 0x78
	for {
		b, err := br.ReadByte()
		if err != nil {
			return nil, 0, 0, err
		}
		if b != 0x78 {
			continue
		}
		b2, err := br.ReadByte()
		if err != nil {
			return nil, 0, 0, err
		}
		if b2 != 0x78 {
			continue
		}

		// length byte
		lenByte, err := br.ReadByte()
		if err != nil {
			return nil, 0, 0, err
		}
		l := int(lenByte) // bytes from protocol to CRC inclusive (proto+content+serial+crc)
		if l < 5 || l > 255 {
			// invalid, continue scanning
			continue
		}

		body := make([]byte, l)
		if _, err := io.ReadFull(br, body); err != nil {
			return nil, 0, 0, err
		}

		// stop bytes
		stop := make([]byte, 2)
		if _, err := io.ReadFull(br, stop); err != nil {
			return nil, 0, 0, err
		}
		if stop[0] != 0x0D || stop[1] != 0x0A {
			// If framing is wrong, keep scanning.
			continue
		}

		raw := append([]byte{0x78, 0x78, lenByte}, body...)
		raw = append(raw, stop...)

		proto := body[0]
		serial := binary.BigEndian.Uint16(body[l-4 : l-2]) // serial is before crc
		return raw, proto, serial, nil
	}
}

func writeGT06Ack(w io.Writer, proto byte, serial uint16) error {
	// Response packet format: 0x78 0x78 0x05 <proto> <serial(2)> <crc(2)> 0x0D 0x0A
	// (same protocol number as request) :contentReference[oaicite:7]{index=7}
	pkt := make([]byte, 0, 10)
	pkt = append(pkt, 0x78, 0x78)
	pkt = append(pkt, 0x05)      // length
	pkt = append(pkt, proto)     // protocol
	pkt = append(pkt, 0x00, 0x00) // serial placeholder
	binary.BigEndian.PutUint16(pkt[4:6], serial)

	// CRC is calculated from length to serial (inclusive): [len][proto][serialHi][serialLo]
	crcInput := []byte{0x05, proto, byte(serial >> 8), byte(serial & 0xFF)}
	crc := crcITU(crcInput)
	pkt = append(pkt, byte(crc>>8), byte(crc&0xFF))
	pkt = append(pkt, 0x0D, 0x0A)

	_, err := w.Write(pkt)
	return err
}

func parseGT06Location(raw []byte) (*AVLData, bool) {
	// raw: start2 + len1 + body(len) + stop2
	if len(raw) < 2+1+5+2 {
		return nil, false
	}
	length := int(raw[2])
	body := raw[3 : 3+length] // proto..crc
	if len(body) < 1+6+1+4+4+1+2+2+2 {
		// proto + datetime + gpslen/sat + lat + lon + speed + course/status + serial + crc
		return nil, false
	}
	proto := body[0]
	if proto != 0x12 && proto != 0x16 {
		return nil, false
	}

	// Offsets per spec for location packet :contentReference[oaicite:8]{index=8}
	i := 1
	dt := body[i : i+6]
	i += 6

	gpsLenSat := body[i]
	i++

	// satellites in low nibble (per doc: first nibble is GPS info length, second nibble is sat count) :contentReference[oaicite:9]{index=9}
	sats := int(gpsLenSat & 0x0F)

	latRaw := binary.BigEndian.Uint32(body[i : i+4])
	i += 4
	lonRaw := binary.BigEndian.Uint32(body[i : i+4])
	i += 4

	speed := int(body[i])
	i++

	cs := binary.BigEndian.Uint16(body[i : i+2])
	i += 2

	// Course is lower 10 bits
	angle := int(cs & 0x03FF)

	// Status bits for sign: bit3 lon east/west, bit2 lat south/north :contentReference[oaicite:10]{index=10}
	// (BYTE_1 is high byte of cs)
	status := byte(cs >> 8)
	lonWest := (status & (1 << 3)) != 0
	latNorth := (status & (1 << 2)) != 0

	// Conversion: (deg*60+minutes)*30000 => raw, so degrees = raw/30000/60 :contentReference[oaicite:11]{index=11}
	lat := float64(latRaw) / 30000.0 / 60.0
	lon := float64(lonRaw) / 30000.0 / 60.0

	if !latNorth {
		lat = -lat
	}
	if lonWest {
		lon = -lon
	}

	ts, ok := parseGT06DateTime(dt)
	if !ok {
		ts = time.Now().UTC()
	}

	return &AVLData{
		Timestamp:  ts,
		Latitude:   lat,
		Longitude:  lon,
		Altitude:   0,
		Angle:      angle,
		Satellites: sats,
		Speed:      speed,
		IOData:     map[uint8]interface{}{}, // GT06 doesn't map to Teltonika IO; keep empty
	}, true
}

func parseGT06DateTime(b []byte) (time.Time, bool) {
	// 6 bytes: YY MM DD HH MM SS (hex bytes)
	if len(b) != 6 {
		return time.Time{}, false
	}
	yy := int(b[0])
	mm := time.Month(int(b[1]))
	dd := int(b[2])
	hh := int(b[3])
	mi := int(b[4])
	ss := int(b[5])

	year := 2000 + yy
	if mm < 1 || mm > 12 || dd < 1 || dd > 31 || hh > 23 || mi > 59 || ss > 59 {
		return time.Time{}, false
	}
	return time.Date(year, mm, dd, hh, mi, ss, 0, time.UTC), true
}

// Terminal ID in login is IMEI 15 digits encoded as 8 bytes like 0x01 0x23 ... :contentReference[oaicite:12]{index=12}
func gt06TerminalIDToIMEI(b []byte) string {
	// Convert each nibble to decimal digit.
	var sb strings.Builder
	for _, x := range b {
		hi := (x >> 4) & 0x0F
		lo := x & 0x0F
		sb.WriteByte('0' + hi)
		sb.WriteByte('0' + lo)
	}
	s := sb.String()
	// often includes a leading 0 to make 16 digits, but IMEI is 15 digits
	s = strings.TrimLeft(s, "0")
	if len(s) > 15 {
		s = s[len(s)-15:]
	}
	if len(s) == 0 {
		return "000000000000000"
	}
	return s
}

// CRC-ITU (CRC-16/CCITT-FALSE style)
func crcITU(data []byte) uint16 {
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

// =====================================================
//                 UNIGUARD HANDLER (S168#...$)
// =====================================================

func handleUniGuard(br *bufio.Reader, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	vLog("📡 UniGuard connection from %s", remote)

	for {
		conn.SetReadDeadline(time.Now().Add(20 * time.Minute))
		line, err := br.ReadString('$') // packets end with '$'
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

		msg := strings.TrimSpace(line)
		if msg == "" {
			continue
		}
		vLog("🟢 UniGuard raw: %s", msg)

		imei, serialHex, rec, ok := parseUniGuard(msg)
		if !ok {
			continue
		}

		deviceID, err := ensureDevice(imei)
		if err != nil {
			vLog("❌ Device lookup failed for UniGuard IMEI %s: %v", imei, err)
			continue
		}

		processOnePosition(deviceID, imei, rec)

		// send ACK (minimal)
		ack := buildUniGuardAck("S168", imei, serialHex, "LOCA")
		_, _ = conn.Write([]byte(ack))
	}
}

func parseUniGuard(msg string) (imei string, serialHex string, rec *AVLData, ok bool) {
	// Format: ID#IMEI#serial#length#content$ :contentReference[oaicite:13]{index=13}
	msg = strings.TrimSuffix(msg, "$")
	parts := strings.Split(msg, "#")
	if len(parts) < 5 {
		return "", "", nil, false
	}

	// Normalize
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	imei = strings.TrimSpace(parts[1])
	serialHex = strings.TrimSpace(parts[2])

	content := strings.TrimSpace(strings.Join(parts[4:], "#")) // in case content has '#'
	if content == "" {
		return imei, serialHex, nil, false
	}

	// Find GDATA: ... ;  :contentReference[oaicite:14]{index=14}
	sections := strings.Split(content, ";")
	var gdata string
	for _, s := range sections {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(strings.ToUpper(s), "GDATA:") {
			gdata = strings.TrimSpace(s[len("GDATA:"):])
			break
		}
	}
	if gdata == "" {
		return imei, serialHex, nil, false
	}

	// Example: GDATA: A, 12,160412154800,22.564025,113.242329,5.5,152,900 :contentReference[oaicite:15]{index=15}
	fields := splitCSVLoose(gdata)
	if len(fields) < 8 {
		return imei, serialHex, nil, false
	}

	// fields[0] = A/V
	sats, _ := strconv.Atoi(strings.TrimSpace(fields[1]))
	tRaw := strings.TrimSpace(fields[2])
	lat, _ := strconv.ParseFloat(strings.TrimSpace(fields[3]), 64)
	lon, _ := strconv.ParseFloat(strings.TrimSpace(fields[4]), 64)
	speedF, _ := strconv.ParseFloat(strings.TrimSpace(fields[5]), 64)
	headingF, _ := strconv.ParseFloat(strings.TrimSpace(fields[6]), 64)
	altF, _ := strconv.ParseFloat(strings.TrimSpace(fields[7]), 64)

	ts := time.Now().UTC()
	// time format: yymmddhhmmss (year 2 digits) per doc description/example :contentReference[oaicite:16]{index=16}
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
	// split on commas, trim spaces, and remove empty tokens
	raw := strings.Split(s, ",")
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		t := strings.TrimSpace(r)
		if t == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

// Downstream ACK format: S168 # IMEI # serial # length # ACK ^ LOCA, parameter :contentReference[oaicite:17]{index=17}
func buildUniGuardAck(id, imei, serialHex, keyword string) string {
	content := "ACK^" + keyword
	// length is hex length of content portion (characters count), excluding '#' and '$' :contentReference[oaicite:18]{index=18}
	lengthHex := fmt.Sprintf("%04x", len(content))
	// Keep same header style (with spaces around # is tolerated; but devices often accept no spaces)
	return fmt.Sprintf("%s#%s#%s#%s#%s$", id, imei, serialHex, lengthHex, content)
}

// =====================================================
//                 SHARED PROCESSING
// =====================================================

func processOnePosition(deviceID int, imei string, rec *AVLData) {
	if rec == nil {
		return
	}

	// Common validation (same rules you use)
	if rec.Latitude == 0 || rec.Longitude == 0 {
		vLog("⚠️ Skipping zero coordinates (imei=%s): LAT=%.7f LNG=%.7f", imei, rec.Latitude, rec.Longitude)
		return
	}
	if rec.Satellites == 0 {
		vLog("⚠️ Skipping record with zero satellites (imei=%s)", imei)
		return
	}
	if rec.Latitude < -90 || rec.Latitude > 90 || rec.Longitude < -180 || rec.Longitude > 180 {
		vLog("⚠️ Skipping out-of-range coordinates (imei=%s): LAT=%.7f LNG=%.7f", imei, rec.Latitude, rec.Longitude)
		return
	}

	if err := storePositionsBatch(deviceID, imei, []*AVLData{rec}); err != nil {
		vLog("❌ DB insert failed: %v", err)
	}

	payload := []map[string]interface{}{
		{
			"device_id":  deviceID,
			"imei":       imei,
			"timestamp":  rec.Timestamp.UTC().Format(time.RFC3339),
			"latitude":   rec.Latitude,
			"longitude":  rec.Longitude,
			"speed":      rec.Speed,
			"angle":      rec.Angle,
			"altitude":   rec.Altitude,
			"satellites": rec.Satellites,
			"io_data":    rec.IOData,
		},
	}
	_ = postPositionsToBackend(payload)
}

// =====================================================
//                 IMEI / DEVICE HANDLING
// =====================================================

// Keep your original exported behavior, but implement via reader-safe helper.
func readIMEI(conn net.Conn) (string, error) {
	br := bufio.NewReaderSize(conn, 64*1024)
	return readIMEIFromReader(br, conn)
}

// Teltonika IMEI handshake: read first bytes and parse digits, then ACK 0x01
func readIMEIFromReader(br *bufio.Reader, conn net.Conn) (string, error) {
	buf := make([]byte, 64)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := br.Read(buf)
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

// (Optional helper if you ever need strict hex parsing for UniGuard serial/len)
func parseHexU16(s string) uint16 {
	s = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(s), "0x"))
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseUint(s, 16, 16)
	return uint16(v)
}
