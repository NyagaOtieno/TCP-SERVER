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
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/joho/godotenv"
)

/*
SUPPORTED DEVICES (auto-detect by first bytes)
1) Teltonika FMB920  -> IMEI: 00 0F + ASCII IMEI, AVL: 00000000 <len> Codec 08/8E ... <crc>
2) UniGuard GT06R    -> GT06/Concox frames: 78 78 / 79 79
3) UniGuard P13      -> GT06/Concox family (78 78 / 79 79)
*/

type AVLData struct {
	Timestamp  time.Time
	Latitude   float64
	Longitude  float64
	Altitude   int
	Angle      int
	Satellites int
	Speed      int
	IOData     map[uint16]interface{} // ✅ use uint16 to support Codec8E IO IDs too
}

type Device struct {
	ID   int    `json:"id"`
	IMEI string `json:"imei"`
}

var (
	tcpServerHost      string
	backendTrackURL    string
	db                 *sql.DB
	httpClient         = &http.Client{Timeout: 15 * time.Second}
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
	vLog("🚀 Starting multi-protocol TCP server (Teltonika + GT06 family)...")

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
//                 CONNECTION HANDLING
// =====================================================

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	vLog("🔗 New connection from %s", remote)

	// Helps with cellular/NAT links
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	// Use a buffered reader so we can peek without losing bytes
	br := bufio.NewReaderSize(conn, 8192)

	// Detect protocol by first bytes
	first, err := peekWithTimeout(br, 2, 45*time.Second)
	if err != nil {
		vLog("❌ Failed initial peek from %s: %v", remote, err)
		return
	}

	// Teltonika IMEI login begins with 00 0F
	if len(first) >= 2 && first[0] == 0x00 && first[1] == 0x0F {
		imei, err := readIMEITeltonika(br, conn)
		if err != nil {
			vLog("❌ Failed Teltonika IMEI read from %s: %v", remote, err)
			return
		}
		vLog("📡 Teltonika device connected: %s", imei)

		deviceID, err := ensureDevice(imei)
		if err != nil {
			vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
			return
		}

		runTeltonikaSession(br, conn, deviceID, imei)
		return
	}

	// GT06 family frames begin with 78 78 or 79 79
	if len(first) >= 2 && ((first[0] == 0x78 && first[1] == 0x78) || (first[0] == 0x79 && first[1] == 0x79)) {
		imei, err := readIMEIGT06(br, conn)
		if err != nil {
			vLog("❌ Failed GT06 IMEI read from %s: %v", remote, err)
			return
		}
		vLog("📡 GT06-family device connected: %s", imei)

		deviceID, err := ensureDevice(imei)
		if err != nil {
			vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
			return
		}

		runGT06Session(br, conn, deviceID, imei)
		return
	}

	// Unknown start bytes: read a small chunk and log it
	chunk, _ := peekWithTimeout(br, 32, 10*time.Second)
	vLog("⚠️ Unknown protocol start from %s: %s", remote, strings.ToUpper(hex.EncodeToString(chunk)))
}

// =====================================================
//                 TELTONIKA SESSION
// =====================================================

func readIMEITeltonika(br *bufio.Reader, conn net.Conn) (string, error) {
	// Teltonika IMEI login: 2 bytes length (00 0F) then 15 ASCII digits
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	raw := make([]byte, 17)
	if _, err := io.ReadFull(br, raw); err != nil {
		return "", err
	}

	vLog("🧾 IMEI-read chunk (%d): %s", len(raw), strings.ToUpper(hex.EncodeToString(raw)))

	// raw[0:2] should be 00 0F
	if raw[0] != 0x00 || raw[1] != 0x0F {
		return "", fmt.Errorf("not a Teltonika IMEI frame: %s", strings.ToUpper(hex.EncodeToString(raw)))
	}

	imeiAscii := string(raw[2:])
	re := regexp.MustCompile(`\D`)
	imei := re.ReplaceAllString(imeiAscii, "")
	if len(imei) < 10 {
		return "", fmt.Errorf("invalid IMEI parsed: %q from %s", imei, imeiAscii)
	}

	// Teltonika expects 0x01 ACK after IMEI
	_, _ = conn.Write([]byte{0x01})
	vLog("✅ Teltonika IMEI parsed: %s", imei)

	return imei, nil
}

func runTeltonikaSession(br *bufio.Reader, conn net.Conn, deviceID int, imei string) {
	residual := make([]byte, 0, 8192)
	tmp := make([]byte, 4096)

	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		n, err := br.Read(tmp)
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				vLog("⏱ Teltonika read timeout for %s, closing", imei)
			} else if err != io.EOF {
				vLog("🔌 Teltonika read error for %s: %v", imei, err)
			}
			return
		}

		if n > 0 {
			vLog("🟢 Raw TCP bytes: %s", hex.EncodeToString(tmp[:n]))
			residual = append(residual, tmp[:n]...)
			vLog("📥 Residual buffer length: %d", len(residual))
		}

		// Teltonika packet framing:
		// 00000000 <dataLen:4> <data:dataLen> <crc:4>
		for len(residual) >= 12 {
			if !bytes.Equal(residual[:4], []byte{0x00, 0x00, 0x00, 0x00}) {
				vLog("⚠️ Missing Teltonika preamble, shifting buffer by 1")
				residual = residual[1:]
				continue
			}

			dataLen := int(binary.BigEndian.Uint32(residual[4:8]))
			if dataLen <= 0 || dataLen > 5*1024*1024 {
				vLog("⚠️ Invalid Teltonika dataLen %d from %s", dataLen, imei)
				residual = residual[4:] // skip preamble
				continue
			}

			totalLen := 8 + dataLen + 4
			if len(residual) < totalLen {
				break
			}

			dataField := residual[8 : 8+dataLen]
			// crcField := residual[8+dataLen : totalLen] // optional validate
			records, err := parseTeltonikaCodec(dataField)
			if err != nil {
				vLog("❌ Teltonika frame parse error: %v", err)
				residual = residual[totalLen:]
				continue
			}

			valid := filterValid(records)

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

			// Teltonika ACK is 4 bytes = number of records accepted
			sendACKTeltonika(conn, len(records))

			residual = residual[totalLen:]
		}
	}
}

// =====================================================
//                 GT06 SESSION
// =====================================================

func readIMEIGT06(br *bufio.Reader, conn net.Conn) (string, error) {
	conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
	defer conn.SetReadDeadline(time.Time{})

	// Read first full GT06 frame
	frame, err := readGT06Frame(br)
	if err != nil {
		return "", err
	}
	vLog("🧾 GT06 first frame: %s", strings.ToUpper(hex.EncodeToString(frame)))

	proto := gt06Protocol(frame)

	// Many devices start with login (0x01). If not, we still ACK and keep reading a bit.
	if proto != 0x01 {
		serial := gt06ExtractSerial(frame)
		if serial != nil {
			ack := buildGT06Ack(proto, serial)
			_, _ = conn.Write(ack)
		}

		// Try reading next frames until login arrives
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			f, e := readGT06Frame(br)
			if e != nil {
				return "", e
			}
			vLog("🧾 GT06 frame: %s", strings.ToUpper(hex.EncodeToString(f)))
			p := gt06Protocol(f)
			serial := gt06ExtractSerial(f)
			if serial != nil {
				ack := buildGT06Ack(p, serial)
				_, _ = conn.Write(ack)
			}
			if p == 0x01 {
				frame = f
				proto = p
				break
			}
		}
	}

	if proto != 0x01 {
		return "", fmt.Errorf("GT06 login (0x01) not received")
	}

	imei := gt06ExtractIMEI(frame)
	if imei == "" {
		return "", fmt.Errorf("GT06 login received but IMEI not parsed: %s", strings.ToUpper(hex.EncodeToString(frame)))
	}

	serial := gt06ExtractSerial(frame)
	ack := buildGT06Ack(0x01, serial)
	_, _ = conn.Write(ack)

	return imei, nil
}

func runGT06Session(br *bufio.Reader, conn net.Conn, deviceID int, imei string) {
	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		frame, err := readGT06Frame(br)
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				vLog("⏱ GT06 read timeout for %s, closing", imei)
			} else if err != io.EOF {
				vLog("🔌 GT06 read error for %s: %v", imei, err)
			}
			return
		}

		vLog("🟢 GT06 raw frame: %s", strings.ToUpper(hex.EncodeToString(frame)))

		proto := gt06Protocol(frame)
		serial := gt06ExtractSerial(frame)

		// Always ACK heartbeat / status packets so device keeps sending
		if serial != nil {
			ack := buildGT06Ack(proto, serial)
			_, _ = conn.Write(ack)
		}

		// Parse GPS-like frames if possible (common: 0x12 / 0x22)
		records := parseGT06MaybeGPS(frame)
		valid := filterValid(records)

		if len(valid) > 0 {
			vLog("🔎 GT06 parsed %d valid records", len(valid))

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
		}
	}
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

	// Try fetch from backend list (optional)
	resp, err := httpClient.Get("https://mytrack-production.up.railway.app/api/devices/list")
	if err == nil {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		var devices []Device
		if json.Unmarshal(body, &devices) == nil {
			for _, d := range devices {
				if strings.TrimSpace(d.IMEI) == imei {
					_, _ = db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
					return d.ID, nil
				}
			}
		}
	}

	// Fallback: create local row if your schema allows it
	_, insErr := db.Exec("INSERT INTO devices(imei) VALUES($1) ON CONFLICT DO NOTHING", imei)
	if insErr != nil {
		return 0, fmt.Errorf("device IMEI %s not found and failed to insert: %v", imei, insErr)
	}

	err = db.QueryRow("SELECT id FROM devices WHERE imei=$1", imei).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("device IMEI %s inserted but id fetch failed: %v", imei, err)
	}
	return id, nil
}

// =====================================================
//                 TELTONIKA CODEC 8 / 8E PARSING
// =====================================================

func parseTeltonikaCodec(data []byte) ([]*AVLData, error) {
	// data starts with codec (08 or 8E)
	if len(data) < 2 {
		return nil, fmt.Errorf("frame too short")
	}

	reader := bytes.NewReader(data)

	var codecID byte
	_ = binary.Read(reader, binary.BigEndian, &codecID)

	var count byte
	_ = binary.Read(reader, binary.BigEndian, &count)

	records := make([]*AVLData, 0, int(count))
	for i := 0; i < int(count); i++ {
		rec, err := parseTeltonikaAVLRecord(reader, codecID)
		if err == nil && rec != nil {
			records = append(records, rec)
		}
	}

	// There is a trailing "count2" byte at end in Teltonika packets.
	// We'll consume it if present but not require.
	if reader.Len() >= 1 {
		_, _ = reader.ReadByte()
	}

	return records, nil
}

func parseTeltonikaAVLRecord(r *bytes.Reader, codecID byte) (*AVLData, error) {
	// Timestamp (8)
	var timestamp uint64
	if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		return nil, err
	}

	nowMs := uint64(time.Now().UnixMilli())
	if timestamp == 0 || timestamp > nowMs+86400000 || timestamp < 946684800000 {
		timestamp = nowMs
	}

	// Priority (1)
	var priority byte
	_ = binary.Read(r, binary.BigEndian, &priority)

	// GPS: lon, lat (4,4), alt (2), angle (2), sats (1), speed (2)
	var lonRaw, latRaw int32
	if err := binary.Read(r, binary.BigEndian, &lonRaw); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &latRaw); err != nil {
		return nil, err
	}

	var altitude, angle uint16
	_ = binary.Read(r, binary.BigEndian, &altitude)
	_ = binary.Read(r, binary.BigEndian, &angle)

	var sats byte
	_ = binary.Read(r, binary.BigEndian, &sats)

	var speed uint16
	_ = binary.Read(r, binary.BigEndian, &speed)

	ioData, _ := parseTeltonikaIO(r, codecID)

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

func parseTeltonikaIO(r *bytes.Reader, codecID byte) (map[uint16]interface{}, error) {
	ioData := make(map[uint16]interface{})

	readByte := func() (byte, error) {
		b, err := r.ReadByte()
		return b, err
	}
	readU16 := func() (uint16, error) {
		var v uint16
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	}
	readU32 := func() (uint32, error) {
		var v uint32
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	}
	readU64 := func() (uint64, error) {
		var v uint64
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	}

	if codecID == 0x08 { // Codec 8
		event, err := readByte()
		if err != nil {
			return ioData, err
		}
		total, err := readByte()
		if err != nil {
			return ioData, err
		}
		ioData[250] = uint64(event)
		ioData[251] = uint64(total)

		n1b, err := readByte()
		if err != nil {
			return ioData, err
		}
		for i := 0; i < int(n1b); i++ {
			idb, _ := readByte()
			valb, _ := readByte()
			ioData[uint16(idb)] = uint64(valb)
		}

		n2b, err := readByte()
		if err != nil {
			return ioData, err
		}
		for i := 0; i < int(n2b); i++ {
			idb, _ := readByte()
			val, _ := readU16()
			ioData[uint16(idb)] = uint64(val)
		}

		n4b, err := readByte()
		if err != nil {
			return ioData, err
		}
		for i := 0; i < int(n4b); i++ {
			idb, _ := readByte()
			val, _ := readU32()
			ioData[uint16(idb)] = uint64(val)
		}

		n8b, err := readByte()
		if err != nil {
			return ioData, err
		}
		for i := 0; i < int(n8b); i++ {
			idb, _ := readByte()
			val, _ := readU64()
			ioData[uint16(idb)] = val
		}

		return ioData, nil
	}

	if codecID == 0x8E { // Codec 8E
		event, err := readU16()
		if err != nil {
			return ioData, err
		}
		total, err := readU16()
		if err != nil {
			return ioData, err
		}
		ioData[250] = uint64(event)
		ioData[251] = uint64(total)

		n1, err := readU16()
		if err != nil {
			return ioData, err
		}
		for i := 0; i < int(n1); i++ {
			id, _ := readU16()
			v, _ := readByte()
			ioData[id] = uint64(v)
		}

		n2, err := readU16()
		if err != nil {
			return ioData, err
		}
		for i := 0; i < int(n2); i++ {
			id, _ := readU16()
			v, _ := readU16()
			ioData[id] = uint64(v)
		}

		n4, err := readU16()
		if err != nil {
			return ioData, err
		}
		for i := 0; i < int(n4); i++ {
			id, _ := readU16()
			v, _ := readU32()
			ioData[id] = uint64(v)
		}

		n8, err := readU16()
		if err != nil {
			return ioData, err
		}
		for i := 0; i < int(n8); i++ {
			id, _ := readU16()
			v, _ := readU64()
			ioData[id] = v
		}

		// 16-byte values exist in Codec8E
		n16, err := readU16()
		if err == nil {
			for i := 0; i < int(n16); i++ {
				id, _ := readU16()
				val16 := make([]byte, 16)
				_, _ = io.ReadFull(r, val16)
				ioData[id] = strings.ToUpper(hex.EncodeToString(val16))
			}
		}

		return ioData, nil
	}

	// Unknown codec, best-effort return
	return ioData, nil
}

// =====================================================
//                 GT06 FRAME + GPS BEST-EFFORT
// =====================================================

func readGT06Frame(br *bufio.Reader) ([]byte, error) {
	// read header (2 bytes)
	h := make([]byte, 2)
	if _, err := io.ReadFull(br, h); err != nil {
		return nil, err
	}

	if !((h[0] == 0x78 && h[1] == 0x78) || (h[0] == 0x79 && h[1] == 0x79)) {
		return nil, fmt.Errorf("invalid GT06 header: %s", strings.ToUpper(hex.EncodeToString(h)))
	}

	if h[0] == 0x78 && h[1] == 0x78 {
		// next: length (1 byte)
		lb, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		l := int(lb)
		// total = header2 + len1 + payload(l bytes) + crc2 + tail2
		total := 2 + 1 + l + 2 + 2
		frame := make([]byte, total)
		frame[0], frame[1] = h[0], h[1]
		frame[2] = lb
		if _, err := io.ReadFull(br, frame[3:]); err != nil {
			return nil, err
		}
		return frame, nil
	}

	// 79 79: length is 2 bytes
	len2 := make([]byte, 2)
	if _, err := io.ReadFull(br, len2); err != nil {
		return nil, err
	}
	l := int(binary.BigEndian.Uint16(len2))
	// total = header2 + len2 + payload(l bytes) + crc2 + tail2
	total := 2 + 2 + l + 2 + 2
	frame := make([]byte, total)
	frame[0], frame[1] = h[0], h[1]
	frame[2], frame[3] = len2[0], len2[1]
	if _, err := io.ReadFull(br, frame[4:]); err != nil {
		return nil, err
	}
	return frame, nil
}

func gt06Protocol(frame []byte) byte {
	if len(frame) < 5 {
		return 0
	}
	if frame[0] == 0x78 && frame[1] == 0x78 {
		// 78 78 len proto ...
		return frame[3]
	}
	if frame[0] == 0x79 && frame[1] == 0x79 {
		// 79 79 len2 proto ...
		return frame[4]
	}
	return 0
}

func gt06ExtractSerial(frame []byte) []byte {
	if len(frame) < 10 {
		return nil
	}
	// tail: serial(2) crc(2) 0D 0A
	return []byte{frame[len(frame)-6], frame[len(frame)-5]}
}

func gt06ExtractIMEI(frame []byte) string {
	// Login proto usually 0x01, and IMEI is 8 bytes BCD after proto
	if gt06Protocol(frame) != 0x01 {
		return ""
	}
	if frame[0] == 0x78 && frame[1] == 0x78 && len(frame) >= 12 {
		return decodeBCDIMEI(frame[4:12])
	}
	if frame[0] == 0x79 && frame[1] == 0x79 && len(frame) >= 13 {
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

// Best-effort GPS parse (firmwares vary). If it can’t confidently parse, returns empty.
func parseGT06MaybeGPS(frame []byte) []*AVLData {
	proto := gt06Protocol(frame)
	if proto != 0x12 && proto != 0x22 {
		return nil
	}

	// We can’t safely guess all GT06 variants without seeing your raw frames.
	// This function is intentionally conservative to avoid wrong coordinates.
	// Once you capture a 7878 GPS frame, we’ll map it exactly.
	return nil
}

// =====================================================
//                 RECORD FILTER
// =====================================================

func filterValid(records []*AVLData) []*AVLData {
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
	return valid
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
		// io_data JSON
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

func sendACKTeltonika(conn net.Conn, accepted int) {
	ack := make([]byte, 4)
	binary.BigEndian.PutUint32(ack, uint32(accepted))
	_, _ = conn.Write(ack)
}

// =====================================================
//                 HELPERS
// =====================================================

func getEnv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}

func peekWithTimeout(br *bufio.Reader, n int, timeout time.Duration) ([]byte, error) {
	// We can’t set deadline on Reader; caller sets it on conn before calls in practice.
	// Here we just Peek.
	if n <= 0 {
		return nil, fmt.Errorf("peek n must be > 0")
	}
	return br.Peek(n)
}
