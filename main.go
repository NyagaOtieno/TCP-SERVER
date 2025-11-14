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

// Global configuration
var (
	tcpServerHost   string
	backendTrackURL string
	db              *sql.DB
	httpClient      = &http.Client{Timeout: 10 * time.Second}
)

func init() {
	_ = godotenv.Load()

	tcpServerHost = getEnv("TCP_PORT", "5027")
	backendTrackURL = getEnv("BACKEND_TRACK_URL", "https://mytrack-production.up.railway.app/api/track")

	pgURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=require",
		getEnv("DB_USER", "postgres"),
		getEnv("DB_PASSWORD", ""),
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_PORT", "5432"),
		getEnv("DB_NAME", "railway"),
	)

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
	listener, err := net.Listen("tcp", ":"+tcpServerHost)
	if err != nil {
		log.Fatalf("❌ Failed to start TCP server: %v", err)
	}
	defer listener.Close()
	log.Println("✅ TCP Server listening on port", tcpServerHost)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("⚠️ Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

// Handle incoming device connection
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

	buf := make([]byte, 4096)
	frameBuffer := make([]byte, 0)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("🔌 Read error for %s: %v", imei, err)
			}
			return
		}
		if n == 0 {
			continue
		}

		frameBuffer = append(frameBuffer, buf[:n]...)

		for {
			if len(frameBuffer) < 12 {
				break // not enough for header
			}

			// Check preamble (4 bytes zeros)
			if frameBuffer[0] != 0 || frameBuffer[1] != 0 || frameBuffer[2] != 0 || frameBuffer[3] != 0 {
				frameBuffer = frameBuffer[1:] // shift until preamble found
				continue
			}

			dataLen := int(binary.BigEndian.Uint32(frameBuffer[4:8]))
			frameEnd := 8 + dataLen + 4
			if len(frameBuffer) < frameEnd {
				break // incomplete frame
			}

			dataField := frameBuffer[8 : 8+dataLen]
			avlRecords, err := parseTeltonikaDataField(dataField)
			if err != nil {
				log.Printf("❌ Failed to parse frame: %v", err)
			} else if len(avlRecords) > 0 {
				log.Printf("🔎 Parsed %d AVL record(s) for %s", len(avlRecords), imei)

				if err := storePositionsBatch(deviceID, imei, avlRecords); err != nil {
					log.Printf("❌ Failed to store positions: %v", err)
				}

				backendPayload := make([]map[string]interface{}, 0, len(avlRecords))
				for _, avl := range avlRecords {
					backendPayload = append(backendPayload, map[string]interface{}{
						"device_id":  deviceID,
						"imei":       imei,
						"timestamp":  avl.Timestamp.Format(time.RFC3339),
						"lat":        avl.Latitude,
						"lng":        avl.Longitude,
						"speed":      avl.Speed,
						"angle":      avl.Angle,
						"altitude":   avl.Altitude,
						"satellites": avl.Satellites,
					})
				}
				if err := postPositionsToBackend(backendPayload); err != nil {
					log.Printf("❌ Failed to forward to backend: %v", err)
				}
			}

			conn.Write([]byte{0x01})
			frameBuffer = frameBuffer[frameEnd:]
		}
	}
}

// Read IMEI
func readIMEI(conn net.Conn) (string, error) {
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}
	re := regexp.MustCompile(`\D`)
	imei := re.ReplaceAllString(string(buf[:n]), "")
	conn.Write([]byte{0x01})
	log.Printf("🔢 Raw IMEI read: %q, Cleaned IMEI: %s", string(buf[:n]), imei)
	return imei, nil
}

// Ensure device exists
func ensureDevice(imei string) (int, error) {
	var id int
	err := db.QueryRow("SELECT id FROM devices WHERE imei=$1", imei).Scan(&id)
	if err == nil {
		return id, nil
	}

	resp, err := httpClient.Get(backendTrackURL + "/devices/list")
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

// Parse Teltonika Codec8 / Codec8 Extended
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
			return nil, err
		}
		var lonRaw, latRaw int32
		binary.Read(reader, binary.BigEndian, &lonRaw)
		binary.Read(reader, binary.BigEndian, &latRaw)
		var altitude, angle uint16
		binary.Read(reader, binary.BigEndian, &altitude)
		binary.Read(reader, binary.BigEndian, &angle)
		var satellites byte
		binary.Read(reader, binary.BigEndian, &satellites)
		var speed uint16
		binary.Read(reader, binary.BigEndian, &speed)

		// Read IO counts (N1/N2/N4/N8)
		var n1 byte
		if err := binary.Read(reader, binary.BigEndian, &n1); err != nil {
			return nil, fmt.Errorf("failed to read IO count: %v", err)
		}
		for j := 0; j < int(n1); j++ {
			var id, val byte
			binary.Read(reader, binary.BigEndian, &id)
			binary.Read(reader, binary.BigEndian, &val)
		}
		var n2, n4, n8 byte
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

		records = append(records, &AVLData{
			Timestamp:  time.UnixMilli(int64(timestamp)),
			Latitude:   float64(latRaw) / 1e7,
			Longitude:  float64(lonRaw) / 1e7,
			Altitude:   int(altitude),
			Angle:      int(angle),
			Satellites: int(satellites),
			Speed:      int(speed),
		})
	}
	var numberOfData2 byte
	binary.Read(reader, binary.BigEndian, &numberOfData2)

	return records, nil
}

// Insert positions into PostgreSQL
func storePositionsBatch(deviceID int, imei string, records []*AVLData) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO positions (device_id, imei, timestamp, lat, lng, speed, angle, altitude, satellites)
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

// Forward positions to backend
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

// Helper
func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
