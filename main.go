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

// AVLData represents a single GPS/IO record
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

// Device represents a registered device
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
	log.Println("🚀 Starting Teltonika TCP Server...")

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
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("🔥 Panic recovered: %v", r)
				}
			}()
			handleConnection(c)
		}(conn)
	}

	wg.Wait()
}

func handleConnection(conn net.Conn) {
	remote := conn.RemoteAddr().String()
	log.Printf("🔗 New connection from %s", remote)

	imei, err := readIMEI(conn)
	if err != nil {
		log.Printf("❌ Failed to read IMEI: %v", err)
		return
	}
	log.Printf("📡 Device connected: %s", imei)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		log.Printf("❌ Device lookup failed: %v", err)
		return
	}

	residual := make([]byte, 0)
	tmp := make([]byte, 4096)

	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		n, err := conn.Read(tmp)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				log.Printf("⏱ Read timeout for %s, closing connection", imei)
			} else if err != io.EOF {
				log.Printf("🔌 Read error for %s: %v", imei, err)
			}
			return
		}

		if n > 0 {
			residual = append(residual, tmp[:n]...)
		}

		// TCP framing: 4-byte length prefix
		for len(residual) >= 4 {
			packetLen := int(binary.BigEndian.Uint32(residual[:4]))
			if packetLen <= 0 || packetLen > 5*1024*1024 {
				log.Printf("⚠️ Invalid packet length %d", packetLen)
				residual = residual[4:]
				continue
			}

			if len(residual) < 4+packetLen {
				break
			}

			frame := residual[4 : 4+packetLen]
			codecPayload, err := normalizeToCodec8(frame)
			if err != nil {
				log.Printf("❌ Codec normalization failed: %v", err)
				residual = residual[4+packetLen:]
				continue
			}

			records, err := parseCodec(codecPayload)
			if err != nil {
				log.Printf("❌ Codec parse failed: %v", err)
				residual = residual[4+packetLen:]
				continue
			}

			validRecords := filterValidRecords(records)
			log.Printf("🔎 Parsed %d valid AVL record(s) for %s", len(validRecords), imei)

			if err := storePositionsBatch(deviceID, imei, validRecords); err != nil {
				log.Printf("❌ DB batch insert failed: %v", err)
			}

			if err := postPositionsToBackend(validRecords, deviceID, imei); err != nil {
				log.Printf("❌ Backend post failed: %v", err)
			}

			sendACK(conn, len(validRecords))
			residual = residual[4+packetLen:]
		}
	}
}

// normalizeToCodec8 returns slice starting at Codec8 ID
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
			return nil, fmt.Errorf("codec8 not found")
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
	log.Printf("🔢 Raw IMEI: %q, Cleaned IMEI: %s", string(raw), imei)
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
		return 0, fmt.Errorf("GET devices list failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var devices []Device
	if err := json.Unmarshal(body, &devices); err != nil {
		return 0, fmt.Errorf("unmarshal devices failed: %v", err)
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
		return nil, fmt.Errorf("codec frame too short")
	}
	reader := bytes.NewReader(data)

	var codecID byte
	if err := binary.Read(reader, binary.BigEndian, &codecID); err != nil {
		return nil, err
	}
	if codecID != 0x08 && codecID != 0x8E {
		return nil, fmt.Errorf("unsupported codec ID %02X", codecID)
	}

	var recordCount byte
	if err := binary.Read(reader, binary.BigEndian, &recordCount); err != nil {
		return nil, err
	}

	records := make([]*AVLData, 0, int(recordCount))
	for i := 0; i < int(recordCount); i++ {
		avl, err := parseSingleAVL(reader)
		if err != nil {
			log.Printf("⚠️ Parse warning record %d: %v", i, err)
			continue
		}
		if avl != nil {
			records = append(records, avl)
		}
	}

	return records, nil
}

func parseSingleAVL(r *bytes.Reader) (*AVLData, error) {
	if r.Len() < 28 {
		return nil, fmt.Errorf("AVL too short")
	}

	var timestamp uint64
	if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		return nil, err
	}
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

	readCount := func() (uint16, error) {
		var cnt uint16
		if err := binary.Read(r, binary.BigEndian, &cnt); err != nil {
			return 0, err
		}
		return cnt, nil
	}

	// N1 - 1 byte values
	n1, _ := readCount()
	for i := 0; i < int(n1); i++ {
		var id, val byte
		_ = binary.Read(r, binary.BigEndian, &id)
		_ = binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	// N2 - 2 byte values
	n2, _ := readCount()
	for i := 0; i < int(n2); i++ {
		var id byte
		var val uint16
		_ = binary.Read(r, binary.BigEndian, &id)
		_ = binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	// N4 - 4 byte values
	n4, _ := readCount()
	for i := 0; i < int(n4); i++ {
		var id byte
		var val uint32
		_ = binary.Read(r, binary.BigEndian, &id)
		_ = binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	// N8 - 8 byte values
	n8, _ := readCount()
	for i := 0; i < int(n8); i++ {
		var id byte
		var val uint64
		_ = binary.Read(r, binary.BigEndian, &id)
		_ = binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	return ioData, nil
}

func filterValidRecords(records []*AVLData) []*AVLData {
	valid := make([]*AVLData, 0, len(records))
	for _, r := range records {
		if r != nil && r.Latitude != 0 && r.Longitude != 0 {
			valid = append(valid, r)
		}
	}
	return valid
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
			_, err = stmt.Exec(deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp.UTC(),
				imei, ioJSON)
		} else {
			_, err = stmt.Exec(deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp.UTC(), imei)
		}
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func postPositionsToBackend(records []*AVLData, deviceID int, imei string) error {
	if len(records) == 0 {
		return nil
	}

	payload := make([]map[string]interface{}, 0, len(records))
	for _, r := range records {
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

	data, _ := json.Marshal(payload)
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
