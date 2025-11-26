package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
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

// =======================
//        STRUCTS
// =======================

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

// =======================
//     GLOBAL CONFIG
// =======================

var (
	tcpServerHost   string
	backendTrackURL string
	db              *sql.DB
	httpClient      = &http.Client{Timeout: 10 * time.Second}
	wg              sync.WaitGroup

	// feature flags
	positionsHasIoData bool
)

// =======================
//        INIT
// =======================

func init() {
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
		log.Fatalf("❌ Failed to connect to PostgreSQL: %v", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatalf("❌ PostgreSQL ping failed: %v", err)
	}
	log.Println("✅ PostgreSQL connected successfully")

	// detect whether positions.io_data exists (avoid runtime pq error)
	positionsHasIoData = checkPositionsHasIoData()
	if positionsHasIoData {
		log.Println("ℹ️ positions.io_data column detected; will store IO JSON")
	} else {
		log.Println("⚠️ positions.io_data column not detected; IO data will be omitted from DB inserts")
	}
}

func checkPositionsHasIoData() bool {
	var col string
	err := db.QueryRow(
		`SELECT column_name FROM information_schema.columns
		 WHERE table_name='positions' AND column_name='io_data' LIMIT 1`).Scan(&col)
	if err != nil {
		// if no rows, err is sql.ErrNoRows -> treat as missing
		return false
	}
	return col == "io_data"
}

// =======================
//        MAIN
// =======================

func main() {
	listener, err := net.Listen("tcp", tcpServerHost)
	if err != nil {
		log.Fatalf("❌ Failed to start TCP server: %v", err)
	}
	defer listener.Close()
	log.Println("✅ TCP Server listening on", tcpServerHost)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("⚠️ Accept error:", err)
			continue
		}
		wg.Add(1)
		go handleConnection(conn)
	}

	wg.Wait()
}

// =======================
//   CONNECTION HANDLER
// =======================

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	imei, err := readIMEI(conn)
	if err != nil {
		log.Println("❌ Failed IMEI:", err)
		return
	}
	log.Printf("📡 Device connected: %s", imei)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		log.Printf("❌ Device lookup failed: %v", err)
		// still continue — device may be unknown; you can decide to return here if needed
		return
	}

	residual := make([]byte, 0)
	tmp := make([]byte, 4096)

	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				log.Printf("🔌 Read error for %s: %v", imei, err)
			}
			return
		}
		if n > 0 {
			residual = append(residual, tmp[:n]...)
			log.Printf("🟢 Raw TCP bytes: %x", tmp[:n])
		}

		// parse all complete frames in residual
		for len(residual) >= 4 {
			// typical Teltonika frame: 4 bytes length, then payload (length bytes)
			packetLen := int(binary.BigEndian.Uint32(residual[:4]))
			// guard against invalid lengths
			if packetLen <= 0 || packetLen > 10*1024*1024 {
				// weird length, skip these 4 bytes and continue
				log.Printf("⚠️ invalid packet length %d, skipping 4 bytes", packetLen)
				residual = residual[4:]
				continue
			}

			if len(residual) < 4+packetLen {
				// incomplete frame; wait for more data
				break
			}

			frame := residual[4 : 4+packetLen]

			// Normalize frame: ensure we feed parseCodec8 with codec payload. Some devices include extra header/trailer.
			codecPayload, err := normalizeToCodec8(frame)
			if err != nil {
				log.Printf("❌ Frame normalization failed: %v, frame hex: %x", err, frame)
				// skip this frame so we don't loop forever
				residual = residual[4+packetLen:]
				continue
			}

			records, err := parseCodec8(codecPayload)
			if err != nil {
				log.Printf("❌ Frame parse error: %v, frame hex: %x", err, codecPayload)
				residual = residual[4+packetLen:] // skip problematic frame
				continue
			}

			log.Printf("🔎 Parsed %d AVL record(s) for %s", len(records), imei)

			if err := storePositionsBatch(deviceID, imei, records); err != nil {
				log.Printf("❌ DB batch insert failed: %v", err)
			}

			payload := make([]map[string]interface{}, 0, len(records))
			for _, avl := range records {
				if avl.Latitude == 0 || avl.Longitude == 0 {
					continue
				}
				payload = append(payload, map[string]interface{}{
					"device_id":  deviceID,
					"imei":       imei,
					"timestamp":  avl.Timestamp.Format(time.RFC3339),
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
				log.Printf("❌ Failed backend post: %v", err)
			}

			sendACK(conn, len(records))
			residual = residual[4+packetLen:]
		}
	}
}

// normalizeToCodec8 attempts to find the codec8 payload inside a raw frame
// It returns a slice starting at codec byte (0x08) and ending before CRC/final bytes if present.
// This makes parsing resilient to small format differences.
func normalizeToCodec8(frame []byte) ([]byte, error) {
	// If frame already starts with codec 0x08, return it
	if len(frame) > 0 && frame[0] == 0x08 {
		return frame, nil
	}
	// Otherwise search for first occurrence of 0x08
	idx := bytes.IndexByte(frame, 0x08)
	if idx == -1 {
		return nil, fmt.Errorf("codec 0x08 not found in frame")
	}
	return frame[idx:], nil
}

// ===============================
//       IMEI READER
// ===============================

func readIMEI(conn net.Conn) (string, error) {
	buf := make([]byte, 64)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buf)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return "", err
	}
	raw := string(buf[:n])
	re := regexp.MustCompile("\\D")
	imei := re.ReplaceAllString(raw, "")
	_, _ = conn.Write([]byte{0x01})
	log.Printf("🔢 Raw IMEI: %q, Cleaned IMEI: %s", raw, imei)
	return imei, nil
}

// ===============================
//       DEVICE LOOKUP
// ===============================

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

// ===============================
//  CODEC8 PARSER
// ===============================

// parseCodec8 expects payload starting with 0x08 (codec) and conforms to Teltonika Codec8 format.
// It's defensive and checks bounds before reads.
func parseCodec8(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("frame too short")
	}
	reader := bytes.NewReader(data)

	var codecID byte
	if err := binary.Read(reader, binary.BigEndian, &codecID); err != nil {
		return nil, err
	}
	if codecID != 0x08 {
		return nil, fmt.Errorf("unexpected codec ID: %d", codecID)
	}

	var recordCount byte
	if err := binary.Read(reader, binary.BigEndian, &recordCount); err != nil {
		return nil, err
	}

	records := make([]*AVLData, 0, int(recordCount))
	for i := 0; i < int(recordCount); i++ {
		avl, err := parseSingleAVL(reader)
		if err != nil {
			// return available records with detailed error
			return records, fmt.Errorf("error parsing AVL record %d: %v", i, err)
		}
		records = append(records, avl)
	}

	// Some implementations include an ending "number of records" byte - read it if present
	// but do not require it.
	// If enough bytes remain, attempt to read the final recordCount2
	if reader.Len() >= 1 {
		var recordCount2 byte
		_ = binary.Read(reader, binary.BigEndian, &recordCount2)
		// ignore value; it's just consistency check in protocol
		_ = recordCount2
	}

	return records, nil
}

func parseSingleAVL(r *bytes.Reader) (*AVLData, error) {
	// We need at least:
	// timestamp(8) + priority(1) + lon(4) + lat(4) + altitude(2) + angle(2) + satellites(1) + speed(2)
	const minHeader = 8 + 1 + 4 + 4 + 2 + 2 + 1 + 2
	if r.Len() < minHeader {
		return nil, fmt.Errorf("single AVL too short (need %d bytes, have %d)", minHeader, r.Len())
	}

	var timestamp uint64
	if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		return nil, err
	}
	var priority byte
	if err := binary.Read(r, binary.BigEndian, &priority); err != nil {
		return nil, err
	}

	var lonRaw, latRaw int32
	if err := binary.Read(r, binary.BigEndian, &lonRaw); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &latRaw); err != nil {
		return nil, err
	}

	var altitude uint16
	if err := binary.Read(r, binary.BigEndian, &altitude); err != nil {
		return nil, err
	}
	var angle uint16
	if err := binary.Read(r, binary.BigEndian, &angle); err != nil {
		return nil, err
	}
	var satellites byte
	if err := binary.Read(r, binary.BigEndian, &satellites); err != nil {
		return nil, err
	}
	var speed uint16
	if err := binary.Read(r, binary.BigEndian, &speed); err != nil {
		return nil, err
	}

	// parse IO elements defensively
	ioData, err := parseIOElements(r)
	if err != nil {
		// return parsed fields even if IO parsing is truncated
		return &AVLData{
			Timestamp:  time.UnixMilli(int64(timestamp)),
			Latitude:   float64(latRaw) / 1e7,
			Longitude:  float64(lonRaw) / 1e7,
			Altitude:   int(altitude),
			Angle:      int(angle),
			Satellites: int(satellites),
			Speed:      int(speed),
			IOData:     ioData,
		}, fmt.Errorf("io parse warning: %v", err)
	}

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

// parseIOElements reads IO structure but is careful with bounds and returns partial ioData + error if truncated.
func parseIOElements(r *bytes.Reader) (map[uint8]interface{}, error) {
	ioData := make(map[uint8]interface{})

	// Check we have at least 1 byte for "n1" count (or return empty)
	if r.Len() < 1 {
		return ioData, fmt.Errorf("io: missing n1")
	}
	var n1 byte
	if err := binary.Read(r, binary.BigEndian, &n1); err != nil {
		return ioData, err
	}
	// n1 entries: id(1) + value(1)
	for i := 0; i < int(n1); i++ {
		if r.Len() < 2 {
			return ioData, fmt.Errorf("io: truncated n1 element")
		}
		var id, val byte
		_ = binary.Read(r, binary.BigEndian, &id)
		_ = binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	// n2
	if r.Len() < 1 {
		return ioData, fmt.Errorf("io: missing n2")
	}
	var n2 byte
	_ = binary.Read(r, binary.BigEndian, &n2)
	for i := 0; i < int(n2); i++ {
		if r.Len() < 3 {
			return ioData, fmt.Errorf("io: truncated n2 element")
		}
		var id byte
		var val uint16
		_ = binary.Read(r, binary.BigEndian, &id)
		_ = binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	// n4
	if r.Len() < 1 {
		return ioData, fmt.Errorf("io: missing n4")
	}
	var n4 byte
	_ = binary.Read(r, binary.BigEndian, &n4)
	for i := 0; i < int(n4); i++ {
		if r.Len() < 5 {
			return ioData, fmt.Errorf("io: truncated n4 element")
		}
		var id byte
		var val uint32
		_ = binary.Read(r, binary.BigEndian, &id)
		_ = binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	// n8
	if r.Len() < 1 {
		return ioData, nil // it's ok to have no n8; return nil error
	}
	var n8 byte
	_ = binary.Read(r, binary.BigEndian, &n8)
	for i := 0; i < int(n8); i++ {
		if r.Len() < 9 {
			return ioData, fmt.Errorf("io: truncated n8 element")
		}
		var id byte
		var val uint64
		_ = binary.Read(r, binary.BigEndian, &id)
		_ = binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	return ioData, nil
}

// ===============================
//     DB BATCH INSERT
// ===============================

func storePositionsBatch(deviceID int, imei string, recs []*AVLData) error {
	if len(recs) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Choose statement depending on whether io_data exists
	var stmt *sql.Stmt
	if positionsHasIoData {
		stmt, err = tx.Prepare(`
			INSERT INTO positions (device_id, lat, lng, speed, angle, altitude, satellites, timestamp, imei, io_data)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`)
	} else {
		stmt, err = tx.Prepare(`
			INSERT INTO positions (device_id, lat, lng, speed, angle, altitude, satellites, timestamp, imei)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`)
	}
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range recs {
		if r.Latitude == 0 || r.Longitude == 0 {
			continue
		}
		if positionsHasIoData {
			// marshal io to JSON for storage
			ioJSON, _ := json.Marshal(r.IOData)
			_, _ = stmt.Exec(
				deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp, imei, ioJSON,
			)
		} else {
			_, _ = stmt.Exec(
				deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp, imei,
			)
		}
	}

	return tx.Commit()
}

// ===============================
//     BACKEND FORWARDER
// ===============================

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
	log.Printf("📬 Backend response (%d): %s", resp.StatusCode, string(body))
	return nil
}

// ===============================
//       ACK SENDER
// ===============================

func sendACK(conn net.Conn, count int) {
	ack := make([]byte, 5)
	binary.BigEndian.PutUint32(ack, uint32(count))
	ack[4] = 0x01
	_, _ = conn.Write(ack)
}

// ===============================
//         HELPERS
// ===============================

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
