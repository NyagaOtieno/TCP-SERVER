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

// AVLData represents a parsed FMB920 AVL packet with latitude & longitude
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

	buf := make([]byte, 4096)
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

		data := buf[:n]
		offset := 0
		for offset < len(data) {
			if len(data[offset:]) < 11 {
				break
			}

			if !(data[offset] == 0 && data[offset+1] == 0 && data[offset+2] == 0 && data[offset+3] == 0) {
				offset++
				continue
			}

			if offset+8 > len(data) {
				break
			}
			dataLen := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
			frameEnd := offset + 8 + dataLen + 4
			if frameEnd > len(data) {
				break
			}

			dataField := data[offset+8 : offset+8+dataLen]
			avlRecords, err := parseTeltonikaDataField(dataField)
			if err != nil {
				log.Printf("❌ Failed to parse Teltonika frame: %v", err)
			} else if len(avlRecords) > 0 {
				log.Printf("🔎 Parsed %d AVL record(s) for %s", len(avlRecords), imei)

				// Store in DB
				if err := storePositionsBatch(deviceID, imei, avlRecords); err != nil {
					log.Printf("❌ Failed to store batch positions: %v", err)
				}

				// Forward to backend
				var backendPayload []map[string]interface{}
				for _, avl := range avlRecords {
					if avl.Latitude == 0 || avl.Longitude == 0 {
						log.Printf("⚠️ Skipping zero lat/lng for backend: %+v", avl)
						continue
					}
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
				}
			}

			// ACK
			conn.Write([]byte{0x01})
			offset = frameEnd
		}
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

// --- Ensure device exists ---
func ensureDevice(imei string) (int, error) {
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

// --- Parse Teltonika Codec8 / Codec8 Extended (with longitude) ---
func parseTeltonikaDataField(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data field too short")
	}
	reader := bytes.NewReader(data)
	var codecID, recordsCount byte
	binary.Read(reader, binary.BigEndian, &codecID)
	binary.Read(reader, binary.BigEndian, &recordsCount)

	records := make([]*AVLData, 0, recordsCount)

	for i := 0; i < int(recordsCount); i++ {
		var timestamp uint64
		var priority byte
		var latRaw, lngRaw int32
		var alt, angle, speed uint16
		var satellites byte

		binary.Read(reader, binary.BigEndian, &timestamp)
		binary.Read(reader, binary.BigEndian, &priority)
		binary.Read(reader, binary.BigEndian, &latRaw)
		binary.Read(reader, binary.BigEndian, &lngRaw)
		binary.Read(reader, binary.BigEndian, &alt)
		binary.Read(reader, binary.BigEndian, &angle)
		binary.Read(reader, binary.BigEndian, &satellites)
		binary.Read(reader, binary.BigEndian, &speed)

		// skip IO elements
		var n1, n2, n4, n8 byte
		binary.Read(reader, binary.BigEndian, &n1)
		for j := 0; j < int(n1); j++ {
			reader.Seek(2, io.SeekCurrent)
		}
		binary.Read(reader, binary.BigEndian, &n2)
		for j := 0; j < int(n2); j++ {
			reader.Seek(3, io.SeekCurrent)
		}
		binary.Read(reader, binary.BigEndian, &n4)
		for j := 0; j < int(n4); j++ {
			reader.Seek(5, io.SeekCurrent)
		}
		binary.Read(reader, binary.BigEndian, &n8)
		for j := 0; j < int(n8); j++ {
			reader.Seek(9, io.SeekCurrent)
		}

		records = append(records, &AVLData{
			Timestamp:  time.UnixMilli(int64(timestamp)),
			Latitude:   float64(latRaw) / 1e7,
			Longitude:  float64(lngRaw) / 1e7,
			Altitude:   int(alt),
			Angle:      int(angle),
			Satellites: int(satellites),
			Speed:      int(speed),
		})
	}

	return records, nil
}

// --- Batch insert into PostgreSQL ---
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
		INSERT INTO positions (device_id, lat, lng, speed, angle, altitude, satellites, timestamp, imei)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, avl := range records {
		if avl.Latitude == 0 || avl.Longitude == 0 {
			log.Printf("⚠️ Skipping zero lat/lng: %+v", avl)
			continue
		}
		if _, err := stmt.Exec(deviceID, avl.Latitude, avl.Longitude, avl.Speed, avl.Angle, avl.Altitude, avl.Satellites, avl.Timestamp, imei); err != nil {
			log.Println("⚠️ Failed insert:", err)
		}
	}

	return tx.Commit()
}

// --- Forward positions to backend ---
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
