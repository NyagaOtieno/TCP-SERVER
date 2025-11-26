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

//
// ===========================
//      STRUCT DEFINITIONS
// ===========================
//

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

//
// ===========================
//      GLOBAL VARIABLES
// ===========================
//

var (
	tcpServerHost   string
	backendTrackURL string
	db              *sql.DB
	httpClient      = &http.Client{Timeout: 10 * time.Second}
	wg              sync.WaitGroup

	positionsHasIoData bool
)

//
// ===========================
//        INIT
// ===========================
//

func init() {
	_ = godotenv.Load()

	tcpServerHost = getEnv("TCP_SERVER_HOST", "0.0.0.0:5027")
	backendTrackURL = getEnv("BACKEND_TRACK_URL", "https://mytrack-production.up.railway.app/api/track")

	pgURL := getEnv("DATABASE_URL", "")
	if pgURL == "" {
		log.Fatal("❌ DATABASE_URL missing")
	}

	var err error
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		log.Fatalf("❌ Failed to open Postgres: %v", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatalf("❌ PostgreSQL ping failed: %v", err)
	}

	positionsHasIoData = checkPositionsHasIoData()
}

func getEnv(key, def string) string {
	if os.Getenv(key) == "" {
		return def
	}
	return os.Getenv(key)
}

func checkPositionsHasIoData() bool {
	var col string
	err := db.QueryRow(`
		SELECT column_name FROM information_schema.columns 
		WHERE table_name='positions' AND column_name='io_data'
	`).Scan(&col)

	return err == nil && col == "io_data"
}

//
// ===========================
//        MAIN SERVER
// ===========================
//

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

//
// ===========================
//      CONNECTION HANDLER
// ===========================
//

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	imei, err := readIMEI(conn)
	if err != nil {
		log.Println("❌ IMEI read error:", err)
		return
	}

	log.Printf("📡 Device connected: %s", imei)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		log.Printf("⚠️ Device lookup failed: %v", err)
	}

	buffer := make([]byte, 0)
	temp := make([]byte, 4096)

	for {

		n, err := conn.Read(temp)
		if err != nil {
			if err != io.EOF {
				log.Printf("🔌 Read error for %s: %v", imei, err)
			}
			return
		}

		if n > 0 {
			buffer = append(buffer, temp[:n]...)
			log.Printf("🟢 Raw TCP bytes: %x", temp[:n])
		}

		for len(buffer) >= 4 {

			packetLen := int(binary.BigEndian.Uint32(buffer[:4]))

			if packetLen <= 0 || packetLen > 10*1024*1024 {
				log.Printf("⚠️ Invalid packet length %d, skipping header", packetLen)
				buffer = buffer[4:]
				continue
			}

			if len(buffer) < 4+packetLen {
				break
			}

			frame := buffer[4 : 4+packetLen]
			codecPayload, err := normalizeToCodec8(frame)

			if err != nil {
				log.Printf("❌ Normalize error: %v", err)
				buffer = buffer[4+packetLen:]
				continue
			}

			records, err := parseCodec8(codecPayload)
			if err != nil {
				log.Printf("❌ Parse error: %v", err)
				buffer = buffer[4+packetLen:]
				continue
			}

			log.Printf("🔎 Parsed %d AVL records for %s", len(records), imei)

			_ = storePositionsBatch(deviceID, imei, records)
			_ = postPositionsToBackend(records, deviceID, imei)

			sendACK(conn, len(records))

			buffer = buffer[4+packetLen:]
		}
	}
}

//
// ===========================
//       IMEI READER
// ===========================
//

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

	conn.Write([]byte{0x01})

	return imei, nil
}

//
// ===========================
//      DEVICE LOOKUP
// ===========================
//

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
	_ = json.Unmarshal(body, &devices)

	for _, d := range devices {
		if strings.TrimSpace(d.IMEI) == imei {
			db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
			return d.ID, nil
		}
	}

	return 0, fmt.Errorf("device not found")
}

//
// ===========================
//       CODEC8 PARSER
// ===========================
//

func normalizeToCodec8(frame []byte) ([]byte, error) {
	if len(frame) > 0 && frame[0] == 0x08 {
		return frame, nil
	}
	idx := bytes.IndexByte(frame, 0x08)
	if idx == -1 {
		return nil, fmt.Errorf("codec8 not found")
	}
	return frame[idx:], nil
}

func parseCodec8(data []byte) ([]*AVLData, error) {

	reader := bytes.NewReader(data)

	var codec byte
	binary.Read(reader, binary.BigEndian, &codec)
	if codec != 0x08 {
		return nil, fmt.Errorf("invalid codec: %d", codec)
	}

	var count byte
	binary.Read(reader, binary.BigEndian, &count)

	records := make([]*AVLData, 0, count)

	for i := 0; i < int(count); i++ {
		avl, err := parseSingleAVL(reader)
		if err != nil {
			return records, err
		}
		records = append(records, avl)
	}

	return records, nil
}

func parseSingleAVL(r *bytes.Reader) (*AVLData, error) {

	var (
		tsRaw      uint64
		priority   byte
		lonRaw     int32
		latRaw     int32
		alt        uint16
		angle      uint16
		sats       byte
		speed      uint16
	)

	binary.Read(r, binary.BigEndian, &tsRaw)
	binary.Read(r, binary.BigEndian, &priority)
	binary.Read(r, binary.BigEndian, &lonRaw)
	binary.Read(r, binary.BigEndian, &latRaw)
	binary.Read(r, binary.BigEndian, &alt)
	binary.Read(r, binary.BigEndian, &angle)
	binary.Read(r, binary.BigEndian, &sats)
	binary.Read(r, binary.BigEndian, &speed)

	ioData, _ := parseIOElements(r)

	return &AVLData{
		Timestamp:  time.UnixMilli(int64(tsRaw)),
		Latitude:   float64(latRaw) / 1e7,
		Longitude:  float64(lonRaw) / 1e7,
		Altitude:   int(alt),
		Angle:      int(angle),
		Satellites: int(sats),
		Speed:      int(speed),
		IOData:     ioData,
	}, nil
}

func parseIOElements(r *bytes.Reader) (map[uint8]interface{}, error) {

	ioData := make(map[uint8]interface{})

	var n1 byte
	if r.Len() < 1 {
		return ioData, nil
	}
	binary.Read(r, binary.BigEndian, &n1)
	for i := 0; i < int(n1); i++ {
		var id, val byte
		binary.Read(r, binary.BigEndian, &id)
		binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	var n2 byte
	if r.Len() < 1 {
		return ioData, nil
	}
	binary.Read(r, binary.BigEndian, &n2)
	for i := 0; i < int(n2); i++ {
		var id byte
		var val uint16
		binary.Read(r, binary.BigEndian, &id)
		binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	var n4 byte
	if r.Len() < 1 {
		return ioData, nil
	}
	binary.Read(r, binary.BigEndian, &n4)
	for i := 0; i < int(n4); i++ {
		var id byte
		var val uint32
		binary.Read(r, binary.BigEndian, &id)
		binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	var n8 byte
	if r.Len() < 1 {
		return ioData, nil
	}
	binary.Read(r, binary.BigEndian, &n8)
	for i := 0; i < int(n8); i++ {
		var id byte
		var val uint64
		binary.Read(r, binary.BigEndian, &id)
		binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	return ioData, nil
}

//
// ===========================
//     DB BATCH INSERT
// ===========================
//

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
		if r.Latitude == 0 || r.Longitude == 0 {
			continue
		}

		if positionsHasIoData {
			j, _ := json.Marshal(r.IOData)
			_, _ = stmt.Exec(deviceID, r.Latitude, r.Longitude, r.Speed, r.Angle,
				r.Altitude, r.Satellites, r.Timestamp, imei, j)
		} else {
			_, _ = stmt.Exec(deviceID, r.Latitude, r.Longitude, r.Speed, r.Angle,
				r.Altitude, r.Satellites, r.Timestamp, imei)
		}
	}

	return tx.Commit()
}

//
// ===========================
//  BACKEND HTTP FORWARDER
// ===========================
//

func postPositionsToBackend(records []*AVLData, deviceID int, imei string) error {

	payload := []map[string]interface{}{}

	for _, r := range records {
		if r.Latitude == 0 || r.Longitude == 0 {
			continue
		}

		payload = append(payload, map[string]interface{}{
			"device_id":  deviceID,
			"imei":       imei,
			"timestamp":  r.Timestamp.Format(time.RFC3339),
			"latitude":   r.Latitude,
			"longitude":  r.Longitude,
			"speed":      r.Speed,
			"angle":      r.Angle,
			"altitude":   r.Altitude,
			"satellites": r.Satellites,
			"io_data":    r.IOData,
		})
	}

	if len(payload) == 0 {
		return nil
	}

	data, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", backendTrackURL, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

//
// ===========================
//         ACK SENDER
// ===========================
//

func sendACK(conn net.Conn, count int) {
	if count <= 0 {
		conn.Write([]byte{0x00})
		return
	}
	conn.Write([]byte{byte(count)})
}
