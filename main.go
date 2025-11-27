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
	tcpServerHost     string
	backendTrackURL   string
	db                *sql.DB
	httpClient        = &http.Client{Timeout: 10 * time.Second}
	wg                sync.WaitGroup
	positionsHasIoData bool
)

func init() {
	// Ensure logs always go to stdout with timestamps (helps when run as a service)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	_ = godotenv.Load()

	log.Println("🔧 init: loading configuration")

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
	log.Println("✅ PostgreSQL connected successfully")

	positionsHasIoData = checkPositionsHasIoData()
	if positionsHasIoData {
		log.Println("ℹ️ positions.io_data column detected; will store IO JSON")
	} else {
		log.Println("⚠️ positions.io_data column not detected; IO data will be omitted from DB inserts")
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
	log.Println("🚀 starting teltonika server...")

	listener, err := net.Listen("tcp", tcpServerHost)
	if err != nil {
		log.Fatalf("❌ Failed to start TCP server: %v", err)
	}
	defer listener.Close()

	log.Printf("✅ TCP Server listening on %s", tcpServerHost)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("⚠️ Accept error:", err)
			continue
		}
		wg.Add(1)
		// wrap goroutine to recover from panics and ensure wg.Done()
		go func(c net.Conn) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("🔥 panic recovered in connection goroutine: %v", r)
				}
			}()
			handleConnection(c)
		}(conn)
	}

	// Should never reach here because server loop is infinite, but keep for completeness
	wg.Wait()
}

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	log.Printf("🔗 new connection from %s", remote)

	imei, err := readIMEI(conn)
	if err != nil {
		log.Printf("❌ Failed IMEI read from %s: %v", remote, err)
		return
	}
	log.Printf("📡 Device connected: %s (from %s)", imei, remote)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		log.Printf("❌ Device lookup failed for IMEI %s: %v", imei, err)
		return
	}

	residual := make([]byte, 0)
	tmp := make([]byte, 4096)

	for {
		// Optional: set a read deadline to detect dead peers
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		n, err := conn.Read(tmp)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				log.Printf("⏱ read timeout for %s, closing connection", imei)
			} else if err != io.EOF {
				log.Printf("🔌 Read error for %s: %v", imei, err)
			}
			return
		}

		if n > 0 {
			residual = append(residual, tmp[:n]...)
			log.Printf("🟢 Raw TCP bytes (%d) from %s: %x", n, imei, tmp[:n])
		}

		// framing loop: need at least 4 bytes for length
		for len(residual) >= 4 {
			packetLen := int(binary.BigEndian.Uint32(residual[:4]))
			if packetLen <= 0 || packetLen > 5*1024*1024 {
				log.Printf("⚠️ invalid packet length %d from %s, skipping 4 bytes", packetLen, imei)
				residual = residual[4:]
				continue
			}

			if len(residual) < 4+packetLen {
				// wait for more bytes
				break
			}

			frame := residual[4 : 4+packetLen]
			codecPayload, err := normalizeToCodec8(frame)
			if err != nil {
				log.Printf("❌ Frame normalization failed for %s: %v, frame hex: %.100x", imei, err, frame)
				// discard this whole frame to avoid infinite loop
				residual = residual[4+packetLen:]
				continue
			}

			records, err := parseCodec(codecPayload)
			if err != nil {
				log.Printf("❌ Frame parse error for %s: %v, payload prefix: %.200x", imei, err, codecPayload)
				residual = residual[4+packetLen:]
				continue
			}

			validRecords := make([]*AVLData, 0, len(records))
			for _, r := range records {
				if r != nil && r.Latitude != 0 && r.Longitude != 0 {
					validRecords = append(validRecords, r)
				}
			}

			log.Printf("🔎 Parsed %d valid AVL record(s) for %s", len(validRecords), imei)
			if err := storePositionsBatch(deviceID, imei, validRecords); err != nil {
				log.Printf("❌ DB batch insert failed for %s: %v", imei, err)
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
				log.Printf("❌ Failed backend post for %s: %v", imei, err)
			}

			sendACK(conn, len(validRecords))
			residual = residual[4+packetLen:]
		}
	}
}

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
	// many Teltonika devices send 0x00 0x0F prefix before IMEI
	if len(raw) >= 2 && raw[0] == 0x00 && raw[1] == 0x0F {
		raw = raw[2:]
	}

	re := regexp.MustCompile(`\D`)
	imei := re.ReplaceAllString(string(raw), "")

	_, _ = conn.Write([]byte{0x01})
	log.Printf("🔢 Raw IMEI read: %q, Cleaned IMEI: %s", string(raw), imei)
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
			log.Printf("⚠️ IO parse warning for record %d: %v", i, err)
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
		return nil, fmt.Errorf("single AVL too short (need %d bytes, have %d)", minHeader, r.Len())
	}

	var timestamp uint64
	if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		return nil, err
	}
	// guard unreasonable timestamps (fix negative/garbage)
	nowMs := uint64(time.Now().UnixMilli())
	if timestamp == 0 || timestamp > nowMs+24*3600*1000 || timestamp < 946684800000 { // before 2000-01-01
		log.Printf("⚠️ suspicious timestamp %d, replacing with now", timestamp)
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

	ioData, err := parseIOElements(r)
	if err != nil {
		log.Printf("⚠️ IO parsing warning: %v", err)
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

func parseIOElements(r *bytes.Reader) (map[uint8]interface{}, error) {
	ioData := make(map[uint8]interface{})

	if r.Len() < 1 {
		return ioData, fmt.Errorf("io: missing n1")
	}
	var n1 byte
	_ = binary.Read(r, binary.BigEndian, &n1)
	for i := 0; i < int(n1); i++ {
		if r.Len() < 2 {
			return ioData, fmt.Errorf("io: truncated n1 element")
		}
		var id, val byte
		_ = binary.Read(r, binary.BigEndian, &id)
		_ = binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

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

	if r.Len() < 1 {
		return ioData, nil
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
				deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp.UTC(),
				imei, ioJSON,
			)
		} else {
			_, err = stmt.Exec(
				deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp.UTC(),
				imei,
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
	log.Printf("📬 Backend response (%d): %.200s", resp.StatusCode, string(body))
	return nil
}

func sendACK(conn net.Conn, count int) {
	ack := make([]byte, 5)
	binary.BigEndian.PutUint32(ack, uint32(count))
	ack[4] = 0x01
	_, _ = conn.Write(ack)
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
