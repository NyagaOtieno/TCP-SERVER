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

type AVLData struct {
	Timestamp  time.Time
	Latitude   float64
	Longitude  float64
	Altitude   int
	Angle      int
	Satellites int
	Speed      int
}

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

// --- Handle incoming device connection with proper TCP buffering ---
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

	buffer := make([]byte, 0)
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

		buffer = append(buffer, tmp[:n]...)

		// Process all complete frames in buffer
		for {
			if len(buffer) < 8 {
				break // Not enough for header
			}

			// Look for start sequence
			start := bytes.Index(buffer, []byte{0, 0, 0, 0})
			if start < 0 {
				buffer = nil // discard garbage
				break
			}
			if len(buffer[start:]) < 8 {
				break // Wait for full header
			}

			dataLen := int(binary.BigEndian.Uint32(buffer[start+4 : start+8]))
			frameEnd := start + 8 + dataLen + 4
			if len(buffer) < frameEnd {
				break // Wait for full frame
			}

			dataField := buffer[start+8 : start+8+dataLen]
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
			sendACK(conn, len(avlRecords))

			// Remove processed frame from buffer
			buffer = buffer[frameEnd:]
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

// --- Send ACK to device ---
func sendACK(conn net.Conn, count int) {
	ack := make([]byte, 5)
	binary.BigEndian.PutUint32(ack, uint32(count))
	ack[4] = 0x01
	conn.Write(ack)
}

// --- Helpers ---
func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

// --- parseTeltonikaDataField and skipIO remain unchanged ---
