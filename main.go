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

	tcpPort := getEnv("TCP_PORT", "5027")
	tcpServerHost = ":" + tcpPort
	backendTrackURL = getEnv("BACKEND_TRACK_URL", "https://mytrack-production.up.railway.app/api/track")

	pgURL := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=require",
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

// --- Handle incoming connection ---
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

	// Robust buffer handling
	buf := make([]byte, 0)
	tmp := make([]byte, 4096)
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				log.Printf("🔌 Read error for %s: %v", imei, err)
			}
			return
		}
		if n == 0 {
			continue
		}
		buf = append(buf, tmp[:n]...)

		for {
			if len(buf) < 12 {
				break
			}
			// check preamble
			if !(buf[0] == 0 && buf[1] == 0 && buf[2] == 0 && buf[3] == 0) {
				buf = buf[1:]
				continue
			}

			dataLen := int(binary.BigEndian.Uint32(buf[4:8]))
			frameEnd := 8 + dataLen + 4
			if len(buf) < frameEnd {
				break
			}

			frame := buf[8 : 8+dataLen]
			avlRecords, err := parseTeltonikaDataField(frame)
			if err != nil {
				log.Printf("❌ Failed to parse frame: %v", err)
			} else {
				if len(avlRecords) > 0 {
					log.Printf("🔎 Parsed %d AVL record(s) for %s", len(avlRecords), imei)
					if err := storePositionsBatch(deviceID, imei, avlRecords); err != nil {
						log.Printf("❌ Failed to store positions: %v", err)
					}
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
					}
				}
			}

			conn.Write([]byte{0x01})
			buf = buf[frameEnd:]
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
	raw := string(buf[:n])
	re := regexp.MustCompile(`\D`)
	imei := re.ReplaceAllString(raw, "")
	conn.Write([]byte{0x01})
	log.Printf("🔢 Raw IMEI read: %q, Cleaned IMEI: %s", raw, imei)
	return imei, nil
}

// --- Ensure device exists ---
func ensureDevice(imei string) (int, error) {
	imei = strings.TrimSpace(imei)
	var id int
	err := db.QueryRow("SELECT id FROM devices WHERE imei=$1", imei).Scan(&id)
	if err == nil {
		return id, nil
	}
	// fallback: query backend
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
	return 0, fmt.Errorf("device IMEI %s not found", imei)
}

// --- Parse Teltonika data field (Codec8/Extended) ---
func parseTeltonikaDataField(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short")
	}
	r := bytes.NewReader(data)
	var codec byte
	if err := binary.Read(r, binary.BigEndian, &codec); err != nil {
		return nil, err
	}
	var recordsCount byte
	if err := binary.Read(r, binary.BigEndian, &recordsCount); err != nil {
		return nil, err
	}
	records := make([]*AVLData, 0, recordsCount)
	for i := 0; i < int(recordsCount); i++ {
		var ts uint64
		if err := binary.Read(r, binary.BigEndian, &ts); err != nil {
			return nil, fmt.Errorf("failed to read timestamp: %v", err)
		}
		var priority byte
		if err := binary.Read(r, binary.BigEndian, &priority); err != nil {
			return nil, err
		}
		var lonRaw int32
		var latRaw int32
		var altitude uint16
		var angle uint16
		var sats byte
		var speed uint16
		if err := binary.Read(r, binary.BigEndian, &lonRaw); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &latRaw); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &altitude); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &angle); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &sats); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &speed); err != nil {
			return nil, err
		}

		// Read N1, N2, N4, N8 counts
		var n1 byte
		if err := binary.Read(r, binary.BigEndian, &n1); err != nil {
			return nil, fmt.Errorf("failed to read IO count: %v", err)
		}
		for j := 0; j < int(n1); j++ {
			var id, val byte
			binary.Read(r, binary.BigEndian, &id)
			binary.Read(r, binary.BigEndian, &val)
		}
		var n2, n4, n8 byte
		binary.Read(r, binary.BigEndian, &n2)
		for j := 0; j < int(n2); j++ {
			var id byte
			var val uint16
			binary.Read(r, binary.BigEndian, &id)
			binary.Read(r, binary.BigEndian, &val)
		}
		binary.Read(r, binary.BigEndian, &n4)
		for j := 0; j < int(n4); j++ {
			var id byte
			var val uint32
			binary.Read(r, binary.BigEndian, &id)
			binary.Read(r, binary.BigEndian, &val)
		}
		binary.Read(r, binary.BigEndian, &n8)
		for j := 0; j < int(n8); j++ {
			var id byte
			var val uint64
			binary.Read(r, binary.BigEndian, &id)
			binary.Read(r, binary.BigEndian, &val)
		}

		lat := float64(latRaw) / 1e7
		lon := float64(lonRaw) / 1e7
		if lat < -90 || lat > 90 {
			lat = 0
		}
		if lon < -180 || lon > 180 {
			lon = 0
		}

		records = append(records, &AVLData{
			Timestamp:  time.UnixMilli(int64(ts)),
			Latitude:   lat,
			Longitude:  lon,
			Altitude:   int(altitude),
			Angle:      int(angle),
			Satellites: int(sats),
			Speed:      int(speed),
		})
	}
	var numberOfData2 byte
	binary.Read(r, binary.BigEndian, &numberOfData2)
	return records, nil
}

// --- Store positions ---
func storePositionsBatch(deviceID int, imei string, records []*AVLData) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if !tx.Stmt(nil) {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(`
		INSERT INTO positions (device_id, imei, timestamp, lat, lng, speed, angle, altitude, satellites)
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
	return tx.Commit()
}

// --- Forward to backend ---
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
