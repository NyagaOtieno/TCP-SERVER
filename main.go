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
					vLog("🔥 Panic recovered in connection goroutine: %v", r)
				}
			}()
			handleConnection(c)
		}(conn)
	}

	wg.Wait()
}

// ---------------- Connection Handling ----------------

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	vLog("🔗 New connection from %s", remote)

	imei, err := readIMEI(conn)
	if err != nil {
		vLog("❌ Failed IMEI read from %s: %v", remote, err)
		return
	}
	vLog("📡 Device connected: %s (from %s)", imei, remote)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
		return
	}

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
			vLog("🟢 Raw TCP bytes (%d) from %s: %s", n, imei, hex.EncodeToString(tmp[:n]))
			residual = append(residual, tmp[:n]...)
			vLog("📥 Residual buffer length: %d", len(residual))
		}

		for len(residual) >= 4 {
			packetLen := int(binary.BigEndian.Uint32(residual[:4]))
			vLog("📦 Packet length from %s: %d", imei, packetLen)
			if packetLen <= 0 || packetLen > 5*1024*1024 {
				vLog("⚠️ Invalid packet length %d from %s", packetLen, imei)
				residual = residual[4:]
				continue
			}

			if len(residual) < 4+packetLen {
				break
			}

			frame := residual[4 : 4+packetLen]
			vLog("🔹 Frame before normalization: %s", hex.EncodeToString(frame))

			codecPayload, err := normalizeToCodec8(frame)
			if err != nil {
				vLog("❌ Frame normalization failed for %s: %v", imei, err)
				residual = residual[4+packetLen:]
				continue
			}
			vLog("🔹 Normalized Codec8 frame (%d bytes): %s", len(codecPayload), hex.EncodeToString(codecPayload))

			records, err := parseCodec(codecPayload)
			if err != nil {
				vLog("❌ Frame parse error for %s: %v", imei, err)
				residual = residual[4+packetLen:]
				continue
			}

			validRecords := make([]*AVLData, 0, len(records))
			for _, r := range records {
				if r != nil && r.Latitude != 0 && r.Longitude != 0 {
					validRecords = append(validRecords, r)
				}
			}

			vLog("🔎 Parsed %d valid AVL record(s) for %s", len(validRecords), imei)
			for _, avl := range validRecords {
				vLog("📍 AVL Record: TS=%s LAT=%.7f LNG=%.7f SPD=%d ALT=%d ANG=%d SAT=%d IO=%v",
					avl.Timestamp.UTC().Format(time.RFC3339),
					avl.Latitude, avl.Longitude,
					avl.Speed, avl.Altitude, avl.Angle, avl.Satellites, avl.IOData)
			}

			if err := storePositionsBatch(deviceID, imei, validRecords); err != nil {
				vLog("❌ DB batch insert failed for %s: %v", imei, err)
			}

			payload := make([]map[string]interface{}, 0, len(validRecords))
			for _, avl := range validRecords {
				payload = append(payload, map[string]interface{}{
					"device_id":  deviceID,
					"imei":       imei,
					"timestamp":  avl.Timestamp.UTC().Format(time.RFC3339),
					"latitude":   avl.Latitude,
					"longitude":  avl.Longitude,
					"speed":      avl.Speed,
					"angle":      avl.Angle,
					"altitude":   avl.Altitude,
					"satellites": avl.Satellites,
					"io_data":    avl.IOData,
				})
			}

			if err := postPositionsToBackend(payload); err != nil {
				vLog("❌ Failed backend post for %s: %v", imei, err)
			}

			sendACK(conn, len(validRecords))
			residual = residual[4+packetLen:]
			vLog("📤 Residual buffer length after processing: %d", len(residual))
		}
	}
}

// ---------------- Helper Functions ----------------

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
			return nil, fmt.Errorf("codec not found in frame")
		}
	}
	return frame[idx:], nil
}

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

	_, _ = conn.Write([]byte{0x01})
	vLog("🔢 Raw IMEI read: %q, Cleaned IMEI: %s", string(raw), imei)
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
		return 0, fmt.Errorf("failed GET devices list: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var devices []Device
	if err := json.Unmarshal(body, &devices); err != nil {
		return 0, fmt.Errorf("parse devices list failed: %v", err)
	}

	for _, d := range devices {
		if strings.TrimSpace(d.IMEI) == imei {
			_, _ = db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
			return d.ID, nil
		}
	}
	return 0, fmt.Errorf("device IMEI %s not found", imei)
}

// ---------------- Codec Parsing ----------------

func parseCodec(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("frame too short")
	}
	reader := bytes.NewReader(data)

	var codecID byte
	if err := binary.Read(reader, binary.BigEndian, &codecID); err != nil {
		return nil, err
	}

	if codecID != 0x08 && codecID != 0x8E {
		return nil, fmt.Errorf("unsupported codec ID: %02X", codecID)
	}

	var recordCount byte
	if err := binary.Read(reader, binary.BigEndian, &recordCount); err != nil {
		return nil, err
	}

	records := make([]*AVLData, 0, int(recordCount))
	for i := 0; i < int(recordCount); i++ {
		avl, err := parseSingleAVL(reader)
		if err != nil {
			vLog("⚠️ IO parse warning for record %d: %v", i, err)
			continue
		}
		if avl != nil {
			records = append(records, avl)
		}
	}

	return records, nil
}

func parseSingleAVL(r *bytes.Reader) (*AVLData, error) {
	const minHeader = 8 + 1 + 4 + 4 + 2 + 2 + 1 + 2
	if r.Len() < minHeader {
		return nil, fmt.Errorf("single AVL too short")
	}

	var timestamp uint64
	_ = binary.Read(r, binary.BigEndian, &timestamp)

	nowMs := uint64(time.Now().UnixMilli())
	if timestamp == 0 || timestamp > nowMs+24*3600*1000 || timestamp < 946684800000 {
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

	var satellites byte
	_ = binary.Read(r, binary.BigEndian, &satellites)
	var speed uint16
	_ = binary.Read(r, binary.BigEndian, &speed)

	ioData, _ := parseIOElements(r)

	return &AVLData{
		Timestamp:  time.UnixMilli(int64(timestamp)),
		Latitude:   float64(latRaw) / 1e7,
		Longitude:  float64(lonRaw) / 1e7,
		Altitude:   int(altitude),
		Angle:      int(angle),
		Satellites: int(satellites),
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

	readUint16 := func() uint16 {
		var v uint16
		_ = binary.Read(r, binary.BigEndian, &v)
		return v
	}

	readUint32 := func() uint32 {
		var v uint32
		_ = binary.Read(r, binary.BigEndian, &v)
		return v
	}

	readUint64 := func() uint64 {
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
		val := readUint16()
		ioData[id] = val
	}

	n4 := int(readByte())
	for i := 0; i < n4; i++ {
		id := readByte()
		val := readUint32()
		ioData[id] = val
	}

	n8 := int(readByte())
	for i := 0; i < n8; i++ {
		id := readByte()
		val := readUint64()
		ioData[id] = val
	}

	return ioData, nil
}

// ---------------- DB / Backend ----------------

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
		vLog("💾 Inserting position into DB: DeviceID=%d IMEI=%s LAT=%.7f LNG=%.7f",
			deviceID, imei, r.Latitude, r.Longitude)
		if positionsHasIoData {
			_, err = stmt.Exec(deviceID, r.Latitude, r.Longitude, r.Speed, r.Angle,
				r.Altitude, r.Satellites, r.Timestamp.UTC(), imei, ioJSON)
		} else {
			_, err = stmt.Exec(deviceID, r.Latitude, r.Longitude, r.Speed, r.Angle,
				r.Altitude, r.Satellites, r.Timestamp.UTC(), imei)
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
	vLog("📬 Backend response (%d): %.200s", resp.StatusCode, string(body))
	return nil
}

func sendACK(conn net.Conn, count int) {
	ack := make([]byte, 5)
	binary.BigEndian.PutUint32(ack, uint32(count))
	ack[4] = 0x01
	_, _ = conn.Write(ack)
	vLog("✅ ACK sent to %s for %d record(s)", conn.RemoteAddr(), count)
}

// ---------------- Utility ----------------

func getEnv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}
