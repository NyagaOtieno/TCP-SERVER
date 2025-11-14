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
	"time"

	_ "github.com/lib/pq"
	"github.com/joho/godotenv"
)

// Device represents a registered device
type Device struct {
	ID   int    `json:"id"`
	IMEI string `json:"imei"`
}

// AVLData represents a parsed FMB920 AVL packet
type AVLData struct {
	Timestamp  time.Time
	Latitude   float64
	Longitude  float64
	Altitude   int
	Angle      int
	Satellites int
	Speed      int
}

// --- Global config ---
var (
	tcpServerHost   string
	backendTrackURL string
	db              *sql.DB
	httpClient      = &http.Client{Timeout: 10 * time.Second}
)

func init() {
	_ = godotenv.Load()

	tcpServerHost = getEnv("TCP_SERVER_HOST", "0.0.0.0:5027")
	backendTrackURL = getEnv("BACKEND_TRACK_URL", "https://mytrack-production.up.railway.app/api/track")

	pgURL := getEnv("POSTGRES_URL", "postgresql://postgres:password@localhost:5432/tracker?sslmode=disable")
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

	log.Println("✅ Configuration loaded, PostgreSQL connected")
}

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
			log.Println("⚠️ Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

// dumpHex prints raw packet bytes in hex for debugging
func dumpHex(label string, data []byte) {
	log.Printf("%s [%d bytes]: %X", label, len(data), data)
}

// --- Handle incoming device connection ---
func handleConnection(conn net.Conn) {
	defer conn.Close()

	imei, err := readIMEI(conn)
	if err != nil {
		log.Println("❌ Failed to read IMEI:", err)
		return
	}
	log.Printf("📡 Device connected: %s", imei)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		log.Printf("❌ Device lookup failed: %v", err)
		return
	}

	for {
		// Step 1: Read 4-byte length header
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			log.Println("⚠️ Failed to read packet length:", err)
			return
		}
		packetLength := int(binary.BigEndian.Uint32(lenBuf))

		// Step 2: Read the full packet
		packet := make([]byte, packetLength)
		if _, err := io.ReadFull(conn, packet); err != nil {
			log.Println("⚠️ Failed to read full packet:", err)
			return
		}

		dumpHex("📦 RAW PACKET RECEIVED", packet)

		// Step 3: Parse Teltonika Codec8/Codec8 Extended
		avlRecords, err := parseTeltonikaDataField(packet)
		if err != nil {
			log.Printf("❌ Failed to parse Teltonika frame: %v", err)
			continue
		}

		if len(avlRecords) > 0 {
			log.Printf("🔎 Parsed %d AVL record(s) for %s", len(avlRecords), imei)

			if err := storePositionsBatch(deviceID, imei, avlRecords); err != nil {
				log.Printf("❌ Failed to store batch positions: %v", err)
			}

			// Forward all records to backend
			var backendPayload []map[string]interface{}
			for _, avl := range avlRecords {
				backendPayload = append(backendPayload, map[string]interface{}{
					"device_id":  deviceID,
					"imei":       imei,
					"timestamp":  avl.Timestamp.Format(time.RFC3339),
					"latitude":   avl.Latitude,
					"longitude":  avl.Longitude,
					"speed":      avl.Speed,
					"angle":      avl.Angle,
					"altitude":   avl.Altitude,
					"satellites": avl.Satellites,
				})
			}
			if err := postPositionsToBackend(backendPayload); err != nil {
				log.Printf("❌ Failed to forward to backend: %v", err)
			} else {
				log.Printf("📤 %d positions forwarded to backend for %s", len(avlRecords), imei)
			}
		}

		// Step 4: Send ACK
		conn.Write([]byte{0x01})
	}
}

// --- Read IMEI ---
func readIMEI(conn net.Conn) (string, error) {
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}

	imeiRaw := string(buf[:n])
	re := regexp.MustCompile(`\D`)
	imei := re.ReplaceAllString(imeiRaw, "")

	conn.Write([]byte{0x01})

	log.Printf("🔢 Raw IMEI read: %q, Cleaned IMEI: %s", imeiRaw, imei)
	return imei, nil
}

// --- Ensure device exists via backend list ---
func ensureDevice(imei string) (int, error) {
	imei = strings.TrimSpace(imei)

	var id int
	err := db.QueryRow("SELECT id FROM devices WHERE imei=$1", imei).Scan(&id)
	if err == nil {
		return id, nil
	}

	resp, err := httpClient.Get("https://mytrack-production.up.railway.app/api/devices/list")
	if err != nil {
		return 0, fmt.Errorf("failed to GET devices list: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var devices []struct {
		ID   int    `json:"id"`
		IMEI string `json:"imei"`
	}
	if err := json.Unmarshal(body, &devices); err != nil {
		return 0, fmt.Errorf("failed to parse devices list: %v\n%s", err, string(body))
	}

	for _, d := range devices {
		if strings.TrimSpace(d.IMEI) == imei {
			_, _ = db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
			return d.ID, nil
		}
	}

	return 0, fmt.Errorf("device IMEI %s not found on backend", imei)
}

// --- Parse Teltonika Codec8/Codec8 Extended data field ---
func parseTeltonikaDataField(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data field too short")
	}
	reader := bytes.NewReader(data)
	var codecID byte
	if err := binary.Read(reader, binary.BigEndian, &codecID); err != nil {
		return nil, err
	}
	var recordsCount byte
	if err := binary.Read(reader, binary.BigEndian, &recordsCount); err != nil {
		return nil, err
	}

	records := make([]*AVLData, 0, recordsCount)

	for i := 0; i < int(recordsCount); i++ {
		var timestamp uint64
		if err := binary.Read(reader, binary.BigEndian, &timestamp); err != nil {
			return nil, fmt.Errorf("failed to read timestamp: %v", err)
		}
		var priority byte
		if err := binary.Read(reader, binary.BigEndian, &priority); err != nil {
			return nil, fmt.Errorf("failed to read priority: %v", err)
		}

		var lonRaw int32
		if err := binary.Read(reader, binary.BigEndian, &lonRaw); err != nil {
			return nil, fmt.Errorf("failed to read lon: %v", err)
		}
		var latRaw int32
		if err := binary.Read(reader, binary.BigEndian, &latRaw); err != nil {
			return nil, fmt.Errorf("failed to read lat: %v", err)
		}
		var altitude uint16
		if err := binary.Read(reader, binary.BigEndian, &altitude); err != nil {
			return nil, fmt.Errorf("failed to read alt: %v", err)
		}
		var angle uint16
		if err := binary.Read(reader, binary.BigEndian, &angle); err != nil {
			return nil, fmt.Errorf("failed to read angle: %v", err)
		}
		var satellites byte
		if err := binary.Read(reader, binary.BigEndian, &satellites); err != nil {
			return nil, fmt.Errorf("failed to read satellites: %v", err)
		}
		var speed uint16
		if err := binary.Read(reader, binary.BigEndian, &speed); err != nil {
			return nil, fmt.Errorf("failed to read speed: %v", err)
		}

		var n1, n2, n4, n8 byte
		binary.Read(reader, binary.BigEndian, &n1)
		for j := 0; j < int(n1); j++ {
			var id, val byte
			binary.Read(reader, binary.BigEndian, &id)
			binary.Read(reader, binary.BigEndian, &val)
		}
		binary.Read(reader, binary.BigEndian, &n2)
		for j := 0; j < int(n2); j++ {
			var id byte
			var val uint16
			binary.Read(reader, binary.BigEndian, &id)
			binary.Read(reader, binary.BigEndian, &val)
		}
		binary.Read(reader, binary.BigEndian, &n4)
		for j := 0; j < int(n4); j++ {
			var id byte
			var val uint32
			binary.Read(reader, binary.BigEndian, &id)
			binary.Read(reader, binary.BigEndian, &val)
		}
		binary.Read(reader, binary.BigEndian, &n8)
		for j := 0; j < int(n8); j++ {
			var id byte
			var val uint64
			binary.Read(reader, binary.BigEndian, &id)
			binary.Read(reader, binary.BigEndian, &val)
		}

		ts := time.UnixMilli(int64(timestamp))
		lat := float64(latRaw) / 1e7
		lon := float64(lonRaw) / 1e7
		if lat < -90 || lat > 90 {
			lat = 0
		}
		if lon < -180 || lon > 180 {
			lon = 0
		}

		records = append(records, &AVLData{
			Timestamp:  ts,
			Latitude:   lat,
			Longitude:  lon,
			Altitude:   int(altitude),
			Angle:      int(angle),
			Satellites: int(satellites),
			Speed:      int(speed),
		})
	}

	// Skip numberOfData2
	var numberOfData2 byte
	binary.Read(reader, binary.BigEndian, &numberOfData2)

	return records, nil
}

// --- Batch insert positions into PostgreSQL ---
func storePositionsBatch(deviceID int, imei string, records []*AVLData) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(`
		INSERT INTO positions (device_id, imei, timestamp, latitude, longitude, speed, angle, altitude, satellites)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, avl := range records {
		_, err := stmt.Exec(deviceID, imei, avl.Timestamp, avl.Latitude, avl.Longitude, avl.Speed, avl.Angle, avl.Altitude, avl.Satellites)
		if err != nil {
			log.Println("⚠️ Failed insert:", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// --- Forward batch to backend ---
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

// --- Helpers ---
func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
