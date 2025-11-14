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

// Default fallback coordinates (Bermuda Triangle midpoint)
const (
	defaultLat = 25.0000
	defaultLon = -71.0000
)

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

	// Set sensible connection pool limits to avoid "too many clients" errors
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatalf("❌ PostgreSQL ping failed: %v", err)
	}

	log.Println("✅ Configuration loaded, PostgreSQL connected")
}

// --- Main ---
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
		data := make([]byte, 4096)
		n, err := conn.Read(data)
		if err != nil {
			if err != io.EOF {
				log.Printf("🔌 Read error for %s: %v", imei, err)
			}
			return
		}

		// Debug: show raw packet bytes
		dumpHex("📦 RAW PACKET RECEIVED", data[:n])

		avlRecords, err := parseAVLRecords(data[:n])
		if err != nil {
			log.Printf("❌ Failed to parse AVL for %s: %v", imei, err)
			continue
		}

		if len(avlRecords) == 0 {
			continue
		}

		log.Printf("🔎 Parsed %d AVL record(s) for %s", len(avlRecords), imei)

		if err := storePositionsBatch(deviceID, imei, avlRecords); err != nil {
			log.Printf("❌ Failed to store batch positions: %v", err)
		} else {
			log.Printf("📍 %d positions saved for %s", len(avlRecords), imei)
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

		// ACK
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
	imei := string(buf[1:n])
	conn.Write([]byte{0x01}) // ACK

	// Clean IMEI: remove whitespace, control chars, keep only digits
	imei = strings.TrimSpace(imei)
	imei = strings.Trim(imei, "\x00\x0F")
	re := regexp.MustCompile(`\D`)
	imei = re.ReplaceAllString(imei, "")

	// Log raw and cleaned IMEI for debugging
	log.Printf("🔢 Raw IMEI read: %q, Cleaned IMEI: %s", string(buf[1:n]), imei)

	return imei, nil
}

// --- Ensure device exists via backend list ---
func ensureDevice(imei string) (int, error) {
	// Clean IMEI just in case
	imei = strings.TrimSpace(imei)
	imei = strings.Trim(imei, "\x00\x0F")
	re := regexp.MustCompile(`\D`)
	imei = re.ReplaceAllString(imei, "")

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
		// Clean backend IMEI for safe comparison
		cleanIMEI := strings.TrimSpace(d.IMEI)
		cleanIMEI = strings.Trim(cleanIMEI, "\x00\x0F")
		cleanIMEI = re.ReplaceAllString(cleanIMEI, "")
		if cleanIMEI == imei {
			_, _ = db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
			return d.ID, nil
		}
	}

	return 0, fmt.Errorf("device IMEI %s not found on backend", imei)
}

// --- Parse multiple AVL records ---
func parseAVLRecords(data []byte) ([]*AVLData, error) {
	var records []*AVLData
	reader := bytes.NewReader(data)

	for reader.Len() >= 25 {
		packet := make([]byte, 25)
		if _, err := reader.Read(packet); err != nil {
			return nil, err
		}

		// Log the per-packet bytes too (optional)
		dumpHex("📦 AVL PACKET", packet)

		avl, err := parseAVLPacket(packet)
		if err != nil {
			continue
		}

		// Apply fallback if zero coordinates
		if avl.Latitude == 0 || avl.Longitude == 0 {
			log.Printf("⚠️ Device sending zero coordinates, using fallback Bermuda Triangle location")
			avl.Latitude = defaultLat
			avl.Longitude = defaultLon
		}

		records = append(records, avl)
	}

	return records, nil
}

// --- Parse a single AVL packet ---
func parseAVLPacket(data []byte) (*AVLData, error) {
	if len(data) < 25 {
		return nil, fmt.Errorf("packet too short")
	}

	timestampMs := binary.BigEndian.Uint64(data[0:8])
	timestamp := time.UnixMilli(int64(timestampMs))

	lon := int32(binary.BigEndian.Uint32(data[8:12])) // corrected offset
	lat := int32(binary.BigEndian.Uint32(data[12:16]))
	alt := int(binary.BigEndian.Uint16(data[16:18]))
	angle := int(binary.BigEndian.Uint16(data[18:20]))
	sat := int(data[20])
	speed := int(binary.BigEndian.Uint16(data[21:23]))

	// Log parsed values for debugging
	log.Printf("📍 Parsed AVL: ts=%s lat=%f lon=%f speed=%d sat=%d angle=%d alt=%d",
		timestamp.Format(time.RFC3339),
		float64(lat)/1e7,
		float64(lon)/1e7,
		speed,
		sat,
		angle,
		alt,
	)

	return &AVLData{
		Timestamp:  timestamp,
		Latitude:   float64(lat) / 1e7,
		Longitude:  float64(lon) / 1e7,
		Altitude:   alt,
		Angle:      angle,
		Satellites: sat,
		Speed:      speed,
	}, nil
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

	// Ensure rollback on error
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
		if _, err := stmt.Exec(deviceID, imei, avl.Timestamp, avl.Latitude, avl.Longitude, avl.Speed, avl.Angle, avl.Altitude, avl.Satellites); err != nil {
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
	log.Printf("📬 Backend response: %s", string(body))
	return nil
}

// --- Helpers ---
func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
