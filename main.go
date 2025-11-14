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
	// RawIO optional if you want the IO map (kept out to avoid large types)
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

	// Keep reading frames (Teltonika frames can be concatenated, ensure we process all bytes)
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

		data := make([]byte, n)
		copy(data, buf[:n])

		// Debug: show raw packet bytes
		dumpHex("📦 RAW PACKET RECEIVED", data)

		// Parse possibly multiple TL frames inside data
		offset := 0
		for offset < len(data) {
			// Need at least 12 bytes for the Teltonika header (4 zeros + 4 length + 1 codec + 1 numRecords + trailing numRecords2 + CRC(4))
			// But we will attempt to parse as much as available
			if len(data[offset:]) < 11 {
				// not enough bytes for a header — wait for more data in next read
				break
			}

			// Expect four zero bytes (preamble)
			if !(data[offset] == 0 && data[offset+1] == 0 && data[offset+2] == 0 && data[offset+3] == 0) {
				// If preamble not found at offset, try advance by 1 to find frame start
				offset++
				continue
			}

			// Read data length (4 bytes big endian)
			if offset+8 > len(data) {
				break // wait for more bytes
			}
			dataLen := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
			frameEnd := offset + 8 + dataLen + 4 // +4 for CRC
			if frameEnd > len(data) {
				// frame not fully received yet
				break
			}

			// Slice data field (from codec id to Number of Data 2)
			dataField := data[offset+8 : offset+8+dataLen]

			// Parse the Teltonika AVL data field (Codec8 / Codec8 Extended)
			avlRecords, err := parseTeltonikaDataField(dataField)
			if err != nil {
				log.Printf("❌ Failed to parse Teltonika frame: %v", err)
			} else {
				if len(avlRecords) > 0 {
					log.Printf("🔎 Parsed %d AVL record(s) for %s", len(avlRecords), imei)

					// store and forward
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
				}
			}

			// Send ACK to device for the full frame (Teltonika expects a 1 byte 0x01 after processing)
			conn.Write([]byte{0x01})

			// advance offset to next possible frame
			offset = frameEnd
		}
	}
}

// --- Read IMEI ---
func readIMEI(conn net.Conn) (string, error) {
	// IMEI sent as ASCII ending with 0x0D 0x0A or as bytes - read upto 32 bytes
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}
	// Many Teltonika devices send ASCII IMEI - sometimes prefixed by 0x0F — be robust
	imeiRaw := string(buf[:n])
	// Trim non-digit chars, keep only digits
	re := regexp.MustCompile(`\D`)
	imei := re.ReplaceAllString(imeiRaw, "")

	// respond with ACK as Teltonika expects (0x01)
	conn.Write([]byte{0x01})

	// Log raw and cleaned IMEI for debugging
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
		cleanIMEI := strings.TrimSpace(d.IMEI)
		if cleanIMEI == imei {
			_, _ = db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
			return d.ID, nil
		}
	}

	return 0, fmt.Errorf("device IMEI %s not found on backend", imei)
}

// --- Parse Teltonika data field (Codec8/Codec8 Extended) ---
func parseTeltonikaDataField(data []byte) ([]*AVLData, error) {
	// data: starts with codecID, numberOfData(1), then N records, then numberOfData2 (1)
	if len(data) < 2 {
		return nil, fmt.Errorf("data field too short")
	}
	reader := bytes.NewReader(data)
	var codecID byte
	if err := binary.Read(reader, binary.BigEndian, &codecID); err != nil {
		return nil, err
	}
	// number of records (1 byte)
	var recordsCount byte
	if err := binary.Read(reader, binary.BigEndian, &recordsCount); err != nil {
		return nil, err
	}

	records := make([]*AVLData, 0, recordsCount)

	for i := 0; i < int(recordsCount); i++ {
		// Each record:
		// Timestamp (8), Priority(1), Longitude(4), Latitude(4), Altitude(2),
		// Angle(2), Satellites(1), Speed(2), IO elements (variable)
		var timestamp uint64
		if err := binary.Read(reader, binary.BigEndian, &timestamp); err != nil {
			return nil, fmt.Errorf("failed to read timestamp: %v", err)
		}
		// priority - skip for now
		var priority byte
		if err := binary.Read(reader, binary.BigEndian, &priority); err != nil {
			return nil, fmt.Errorf("failed to read priority: %v", err)
		}

		// longitude & latitude are 4-byte signed ints (1e-7)
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

		// Parse IO elements:
		// Codec8 uses grouped IO counts: N1, N2, N4, N8 (each 1 byte)
		// For Codec8 Extended the structure is the same but certain IO ids/lengths
		// might be larger — we'll support N1/N2/N4/N8 and (optionally) NX if present.
		var n1 byte
		if err := binary.Read(reader, binary.BigEndian, &n1); err != nil {
			return nil, fmt.Errorf("failed to read N1: %v", err)
		}
		// N1 entries (id:1 byte, value:1 byte)
		for j := 0; j < int(n1); j++ {
			var id byte
			var val byte
			if err := binary.Read(reader, binary.BigEndian, &id); err != nil {
				return nil, fmt.Errorf("failed N1 id: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &val); err != nil {
				return nil, fmt.Errorf("failed N1 val: %v", err)
			}
			// optional: log.Printf("IO1 id=%d val=%d", id, val)
			_ = id
			_ = val
		}

		// N2 entries (id:1 byte, value:2 bytes)
		var n2 byte
		if err := binary.Read(reader, binary.BigEndian, &n2); err != nil {
			return nil, fmt.Errorf("failed to read N2: %v", err)
		}
		for j := 0; j < int(n2); j++ {
			var id byte
			var val uint16
			if err := binary.Read(reader, binary.BigEndian, &id); err != nil {
				return nil, fmt.Errorf("failed N2 id: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &val); err != nil {
				return nil, fmt.Errorf("failed N2 val: %v", err)
			}
			_ = id
			_ = val
		}

		// N4 entries (id:1 byte, value:4 bytes)
		var n4 byte
		if err := binary.Read(reader, binary.BigEndian, &n4); err != nil {
			return nil, fmt.Errorf("failed to read N4: %v", err)
		}
		for j := 0; j < int(n4); j++ {
			var id byte
			var val uint32
			if err := binary.Read(reader, binary.BigEndian, &id); err != nil {
				return nil, fmt.Errorf("failed N4 id: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &val); err != nil {
				return nil, fmt.Errorf("failed N4 val: %v", err)
			}
			_ = id
			_ = val
		}

		// N8 entries (id:1 byte, value:8 bytes)
		var n8 byte
		if err := binary.Read(reader, binary.BigEndian, &n8); err != nil {
			return nil, fmt.Errorf("failed to read N8: %v", err)
		}
		for j := 0; j < int(n8); j++ {
			var id byte
			var val uint64
			if err := binary.Read(reader, binary.BigEndian, &id); err != nil {
				return nil, fmt.Errorf("failed N8 id: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &val); err != nil {
				return nil, fmt.Errorf("failed N8 val: %v", err)
			}
			_ = id
			_ = val
		}

		// NOTE: Codec8 Extended may include NX (IO elements with 2-byte IDs and 2-byte lengths).
		// Many devices do not use NX. If there are remaining bytes that look like NX entries,
		// a more complex parser would be required. For now we keep safe parsing above and rely on
		// the Data Length boundary to skip leftover bytes.

		// Convert timestamp (ms since epoch) to time.Time
		ts := time.UnixMilli(int64(timestamp))

		lat := float64(latRaw) / 1e7
		lon := float64(lonRaw) / 1e7

		// Basic sanity: if coords are outside valid ranges, leave them as zero and allow fallback upstream
		if lat < -90 || lat > 90 {
			lat = 0
		}
		if lon < -180 || lon > 180 {
			lon = 0
		}

		avl := &AVLData{
			Timestamp:  ts,
			Latitude:   lat,
			Longitude:  lon,
			Altitude:   int(altitude),
			Angle:      int(angle),
			Satellites: int(satellites),
			Speed:      int(speed),
		}

		// Log the parsed low-level values for debugging
		log.Printf("📍 Parsed AVL: ts=%s lat=%f lon=%f speed=%d sat=%d angle=%d alt=%d",
			avl.Timestamp.Format(time.RFC3339),
			avl.Latitude,
			avl.Longitude,
			avl.Speed,
			avl.Satellites,
			avl.Angle,
			avl.Altitude,
		)

		records = append(records, avl)
	}

	// numberOfData2 (1 byte) should be next - read to advance the reader safely
	var numberOfData2 byte
	if err := binary.Read(reader, binary.BigEndian, &numberOfData2); err == nil {
		_ = numberOfData2
	}

	// CRC (4 bytes) is not validated here — Teltonika devices include it at the end of frame.
	// We don't need it for parsing, but the frame slicing in caller ensures we processed correct boundaries.

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
		_, err := stmt.Exec(deviceID, imei, avl.Timestamp, avl.Latitude, avl.Longitude, avl.Speed, avl.Angle, avl.Altitude, avl.Satellites)
		if err != nil {
			// Log and continue to avoid aborting entire batch on single bad row
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
