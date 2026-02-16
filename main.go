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
	httpClient         = &http.Client{Timeout: 12 * time.Second}
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
}

// =====================================================
//              PROTOCOL DETECTION + ROUTING
// =====================================================

type ProtocolKind int

const (
	ProtoTeltonika ProtocolKind = iota
	ProtoGT06
	ProtoUniGuard
)

func (p ProtocolKind) String() string {
	switch p {
	case ProtoGT06:
		return "GT06"
	case ProtoUniGuard:
		return "UNIGUARD"
	default:
		return "TELTONIKA/UNKNOWN"
	}
}

func detectProtocolPeek(br *bufio.Reader) ProtocolKind {
	peek, _ := br.Peek(32)

	// GT06 binary starts 0x78 0x78
	if len(peek) >= 2 && peek[0] == 0x78 && peek[1] == 0x78 {
		return ProtoGT06
	}

	// UniGuard is ASCII starts with 'S' and contains '#'
	if len(peek) >= 1 && (peek[0] == 'S' || peek[0] == 's') {
		if bytes.Contains(peek, []byte("#")) {
			return ProtoUniGuard
		}
	}

	// Teltonika IMEI handshake 0x00 0x0F + 15 ASCII digits
	if len(peek) >= 2 && peek[0] == 0x00 && peek[1] == 0x0F {
		return ProtoTeltonika
	}

	// Unknown -> treat as teltonika/unknown, but we will do safe fallbacks
	return ProtoTeltonika
}

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	vLog("🔗 New connection from %s", remote)

	br := bufio.NewReaderSize(conn, 128*1024)

	if p, _ := br.Peek(32); len(p) > 0 {
		vLog("👀 First bytes: %s", hex.EncodeToString(p))
	}

	kind := detectProtocolPeek(br)
	vLog("🧭 Protocol guess from %s: %s", remote, kind.String())

	switch kind {
	case ProtoGT06:
		handleGT06(br, conn)
		return
	case ProtoUniGuard:
		handleUniGuard(br, conn)
		return
	default:
		// Try Teltonika IMEI handshake ONLY if header matches 00 0F.
		imei, err := readTeltonikaIMEIHandshake(br, conn)
		if err == nil {
			handleTeltonikaAfterIMEI(br, conn, imei)
			return
		}

		// Fallbacks (do not consume bytes)
		if p, _ := br.Peek(2); len(p) == 2 && p[0] == 0x78 && p[1] == 0x78 {
			vLog("🔁 Fallback detected GT06 after Teltonika IMEI fail")
			handleGT06(br, conn)
			return
		}
		if p, _ := br.Peek(1); len(p) == 1 && (p[0] == 'S' || p[0] == 's') {
			vLog("🔁 Fallback detected UniGuard after Teltonika IMEI fail")
			handleUniGuard(br, conn)
			return
		}

		vLog("⚠️ Unknown protocol from %s (no Teltonika IMEI / no GT06 / no UniGuard). Closing.", remote)
		return
	}
}

// =====================================================
//                    TELTONIKA (FMB/FMC)
// =====================================================

// Reads ONLY Teltonika IMEI handshake if it matches 00 0F.
// If it isn't Teltonika, it returns error WITHOUT consuming bytes.
func readTeltonikaIMEIHandshake(br *bufio.Reader, conn net.Conn) (string, error) {
	conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	p2, err := br.Peek(2)
	if err != nil {
		return "", err
	}
	if len(p2) < 2 || p2[0] != 0x00 || p2[1] != 0x0F {
		return "", fmt.Errorf("not a teltonika IMEI header")
	}

	// consume 2 header bytes
	_, _ = br.ReadByte()
	_, _ = br.ReadByte()

	imeiBytes := make([]byte, 15)
	if _, err := io.ReadFull(br, imeiBytes); err != nil {
		return "", err
	}

	imei := strings.TrimSpace(string(imeiBytes))
	if !regexp.MustCompile(`^\d{15}$`).MatchString(imei) {
		return "", fmt.Errorf("invalid teltonika imei: %q", imei)
	}

	// Teltonika requires 0x01 ACK after IMEI
	_, _ = conn.Write([]byte{0x01})
	return imei, nil
}

func handleTeltonikaAfterIMEI(br *bufio.Reader, conn net.Conn, imei string) {
	vLog("📡 Teltonika device connected: %s", imei)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
		return
	}

	residual := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)

	for {
		conn.SetReadDeadline(time.Now().Add(10 * time.Minute))
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

		// Teltonika frame:
		// 4 bytes preamble (00000000)
		// 4 bytes data length (N)
		// N bytes data (codec8/8e...)
		// 4 bytes CRC
		for {
			if len(residual) < 8 {
				break
			}

			// resync to preamble 00000000
			if binary.BigEndian.Uint32(residual[:4]) != 0 {
				idx := bytes.Index(residual, []byte{0x00, 0x00, 0x00, 0x00})
				if idx == -1 {
					// keep last few bytes in case preamble is split
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
				vLog("⚠️ Invalid dataLen %d from %s, resync...", dataLen, imei)
				residual = residual[1:]
				continue
			}

			total := 8 + dataLen + 4
			if len(residual) < total {
				break
			}

			data := residual[8 : 8+dataLen]
			// crc := residual[8+dataLen : total] // available if you later want to validate
			_ = residual[8+dataLen : total]

			codecPayload, err := normalizeToCodec8(data)
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

			valid := make([]*AVLData, 0, len(records))
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

			// ✅ Correct Teltonika ACK: 4 bytes accepted record count
			sendTeltonikaACK(conn, len(valid))

			// consume frame
			residual = residual[total:]
		}
	}
}

func sendTeltonikaACK(conn net.Conn, count int) {
	ack := make([]byte, 4)
	binary.BigEndian.PutUint32(ack, uint32(count))
	_, _ = conn.Write(ack)
}

// =====================================================
//                    GT06
// =====================================================

func handleGT06(br *bufio.Reader, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	vLog("📡 GT06 connection from %s", remote)

	imei, serial, err := readGT06Login(br)
	if err != nil {
		vLog("❌ GT06 login failed: %v", err)
		return
	}
	vLog("✅ GT06 login IMEI=%s serial=0x%04X", imei, serial)

	// ACK login
	_ = writeGT06Ack(conn, 0x01, serial)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
		return
	}

	for {
		conn.SetReadDeadline(time.Now().Add(20 * time.Minute))
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
		case 0x12, 0x16: // location / alarm location
			rec, ok := parseGT06Location(pkt)
			if ok {
				processOnePosition(deviceID, imei, rec)
			}
			_ = writeGT06Ack(conn, proto, serial)

		case 0x13: // heartbeat
			_ = writeGT06Ack(conn, proto, serial)

		default:
			vLog("ℹ️ GT06 proto 0x%02X len=%d", proto, len(pkt))
			_ = writeGT06Ack(conn, proto, serial)
		}
	}
}

func readGT06Login(br *bufio.Reader) (string, uint16, error) {
	pkt, proto, serial, err := readGT06Packet(br)
	if err != nil {
		return "", 0, err
	}
	if proto != 0x01 {
		return "", 0, fmt.Errorf("expected login proto 0x01, got 0x%02X", proto)
	}
	// login has 8-byte terminal id after proto
	if len(pkt) < 2+1+1+8+2+2+2 {
		return "", 0, fmt.Errorf("login packet too short: %d", len(pkt))
	}
	terminalID := pkt[2+1+1 : 2+1+1+8]
	imei := gt06TerminalIDToIMEI(terminalID)
	return imei, serial, nil
}

func readGT06Packet(br *bufio.Reader) ([]byte, byte, uint16, error) {
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

		lenByte, err := br.ReadByte()
		if err != nil {
			return nil, 0, 0, err
		}
		l := int(lenByte)
		if l < 5 || l > 255 {
			continue
		}

		body := make([]byte, l)
		if _, err := io.ReadFull(br, body); err != nil {
			return nil, 0, 0, err
		}

		stop := make([]byte, 2)
		if _, err := io.ReadFull(br, stop); err != nil {
			return nil, 0, 0, err
		}
		if stop[0] != 0x0D || stop[1] != 0x0A {
			continue
		}

		raw := append([]byte{0x78, 0x78, lenByte}, body...)
		raw = append(raw, stop...)

		proto := body[0]
		serial := binary.BigEndian.Uint16(body[l-4 : l-2]) // serial before crc
		return raw, proto, serial, nil
	}
}

func writeGT06Ack(w io.Writer, proto byte, serial uint16) error {
	// 78 78 05 <proto> <serial> <crc> 0D 0A
	pkt := make([]byte, 0, 10)
	pkt = append(pkt, 0x78, 0x78, 0x05, proto)
	pkt = append(pkt, 0x00, 0x00)
	binary.BigEndian.PutUint16(pkt[4:6], serial)

	crcInput := []byte{0x05, proto, byte(serial >> 8), byte(serial & 0xFF)}
	crc := crcITU(crcInput)
	pkt = append(pkt, byte(crc>>8), byte(crc&0xFF))
	pkt = append(pkt, 0x0D, 0x0A)

	_, err := w.Write(pkt)
	return err
}

func parseGT06Location(raw []byte) (*AVLData, bool) {
	// raw: 78 78 <len> <body(len bytes)> 0D 0A
	if len(raw) < 5 {
		return nil, false
	}
	length := int(raw[2])
	if len(raw) < 3+length+2 {
		return nil, false
	}
	body := raw[3 : 3+length] // proto..crc

	if len(body) < 1+6+1+4+4+1+2 {
		return nil, false
	}

	proto := body[0]
	if proto != 0x12 && proto != 0x16 {
		return nil, false
	}

	i := 1
	dt := body[i : i+6]
	i += 6

	gpsLenSat := body[i]
	i++
	sats := int(gpsLenSat & 0x0F)

	latRaw := binary.BigEndian.Uint32(body[i : i+4])
	i += 4
	lonRaw := binary.BigEndian.Uint32(body[i : i+4])
	i += 4

	speed := int(body[i])
	i++

	cs := binary.BigEndian.Uint16(body[i : i+2]) // course/status
	angle := int(cs & 0x03FF)

	status := byte(cs >> 8)
	lonWest := (status & (1 << 3)) != 0
	latNorth := (status & (1 << 2)) != 0

	// Convert: raw = (deg*60 + minutes) * 30000
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
		IOData:     map[uint8]interface{}{},
	}, true
}

func parseGT06DateTime(b []byte) (time.Time, bool) {
	if len(b) != 6 {
		return time.Time{}, false
	}
	yy := int(b[0])
	year := 2000 + yy
	month := time.Month(int(b[1]))
	day := int(b[2])
	h := int(b[3])
	m := int(b[4])
	s := int(b[5])

	if month < 1 || month > 12 || day < 1 || day > 31 || h > 23 || m > 59 || s > 59 {
		return time.Time{}, false
	}
	return time.Date(year, month, day, h, m, s, 0, time.UTC), true
}

func gt06TerminalIDToIMEI(b []byte) string {
	// terminal id is 8 bytes (BCD-like). Often yields 16 digits with leading 0.
	var sb strings.Builder
	for _, x := range b {
		hi := (x >> 4) & 0x0F
		lo := x & 0x0F
		sb.WriteByte('0' + hi)
		sb.WriteByte('0' + lo)
	}
	s := sb.String()
	if len(s) >= 15 {
		return s[len(s)-15:]
	}
	return fmt.Sprintf("%015s", s)
}

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
//                    UNIGUARD
// =====================================================

func handleUniGuard(br *bufio.Reader, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	vLog("📡 UniGuard connection from %s", remote)

	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Minute))
		line, err := br.ReadString('$')
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
			vLog("⚠️ UniGuard parse failed (no/short GDATA?)")
			continue
		}
		vLog("✅ UniGuard IMEI=%s serial=%s", imei, serialHex)

		deviceID, err := ensureDevice(imei)
		if err != nil {
			vLog("❌ Device lookup failed for UniGuard IMEI %s: %v", imei, err)
			continue
		}

		processOnePosition(deviceID, imei, rec)

		ack := buildUniGuardAck("S168", imei, serialHex, "LOCA")
		_, _ = conn.Write([]byte(ack))
	}
}

func parseUniGuard(msg string) (imei string, serialHex string, rec *AVLData, ok bool) {
	msg = strings.TrimSuffix(msg, "$")
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
	if content == "" {
		return imei, serialHex, nil, false
	}

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

	fields := splitCSVLoose(gdata)
	if len(fields) < 8 {
		return imei, serialHex, nil, false
	}

	// Example: A,12,160412154800,22.564025,113.242329,5.5,152,900
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
		if t == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

func buildUniGuardAck(id, imei, serialHex, keyword string) string {
	// ACK^LOCA (commonly accepted by UniGuard style devices)
	content := "ACK^" + keyword
	lengthHex := fmt.Sprintf("%04x", len(content))
	return fmt.Sprintf("%s#%s#%s#%s#%s$", id, imei, serialHex, lengthHex, content)
}

// =====================================================
//           SHARED: POSITION PROCESSING + VALIDATION
// =====================================================

func processOnePosition(deviceID int, imei string, rec *AVLData) {
	if rec == nil {
		return
	}

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
//                 DEVICE HANDLING
// =====================================================

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

	// codec8/8e IO:
	// event IO id (1 byte)
	// total IO count (1 byte)
	_ = readByte()
	_ = readByte()

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

func parseHexU16(s string) uint16 {
	s = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(s), "0x"))
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseUint(s, 16, 16)
	return uint16(v)
}
