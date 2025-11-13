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
	devicesPostURL  string
	backendTrackURL string
	db              *sql.DB
	httpClient      = &http.Client{Timeout: 10 * time.Second}
)

func init() {
	_ = godotenv.Load()

	tcpServerHost = getEnv("TCP_SERVER_HOST", "0.0.0.0:5027")
	devicesPostURL = getEnv("DEVICES_POST_URL", "")
	backendTrackURL = getEnv("BACKEND_TRACK_URL", "https://mytrack-production.up.railway.app/api/track")

	pgURL := getEnv("POSTGRES_URL", "postgres://user:pass@localhost:5432/tracker?sslmode=disable")
	var err error
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		log.Fatalf("❌ Failed to connect to PostgreSQL: %v", err)
	}

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
		log.Printf("❌ Device registration failed: %v", err)
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

		avlRecords, err := parseAVLRecords(data[:n])
		if err != nil {
			log.Printf("❌ Failed to parse AVL for %s: %v", imei, err)
			continue
		}

		if len(avlRecords) == 0 {
			continue
		}

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
	return imei, nil
}

// --- Ensure device exists ---
func ensureDevice(imei string) (int, error) {
	var id int
	err := db.QueryRow("SELECT id FROM devices WHERE imei=$1", imei).Scan(&id)
	if err == nil {
		return id, nil
	}

	data, _ := json.Marshal(map[string]string{"imei": imei})
	req, _ := http.NewRequest("POST", devicesPostURL, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var newDevice Device
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &newDevice); err != nil {
		return 0, err
	}

	_, err = db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", newDevice.ID, imei)
	if err != nil {
		return 0, err
	}

	return newDevice.ID, nil
}

// --- Parse multiple AVL records ---
func parseAVLRecords(data []byte) ([]*AVLData, error) {
	var records []*AVLData
	reader := bytes.NewReader(data)

	for reader.Len() >= 25 { // Minimum packet size
		packet := make([]byte, 25)
		if _, err := reader.Read(packet); err != nil {
			return nil, err
		}

		avl, err := parseAVLPacket(packet)
		if err != nil {
			continue
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

	lon := int32(binary.BigEndian.Uint32(data[9:13]))
	lat := int32(binary.BigEndian.Uint32(data[13:17]))
	alt := int(binary.BigEndian.Uint16(data[17:19]))
	angle := int(binary.BigEndian.Uint16(data[19:21]))
	sat := int(data[21])
	speed := int(binary.BigEndian.Uint16(data[22:24]))

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

	return tx.Commit()
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
