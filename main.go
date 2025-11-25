package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
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

// =======================
//      DATA STRUCTS
// =======================

type AVLData struct {
	Timestamp  time.Time
	Latitude   float64
	Longitude  float64
	Altitude   int
	Angle      int
	Satellites int
	Speed      int
}

// Device struct for backend lookup
type Device struct {
	ID   int    json:"id"
	IMEI string json:"imei"
}

// =======================
//      GLOBAL CONFIG
// =======================

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

// =======================
//        MAIN
// =======================

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
		go handleConnection(conn)
	}
}

// =======================
//   CONNECTION HANDLER
// =======================

func handleConnection(conn net.Conn) {
	defer conn.Close()

	imei, err := readIMEI(conn)
	if err != nil {
		log.Println("❌ Failed IMEI:", err)
		return
	}
	log.Printf("📡 Device connected: %s", imei)

	deviceID, err := ensureDevice(imei)
	if err != nil {
		log.Printf("❌ Device lookup failed: %v", err)
		return
	}

	buf := make([]byte, 0, 8192)
	tmp := make([]byte, 4096)

	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				log.Printf("🔌 Read error for %s: %v", imei, err)
			}
			return
		}

		buf = append(buf, tmp[:n]...)

		if n > 0 {
			log.Printf("🟢 Raw TCP bytes: %s", hex.EncodeToString(tmp[:n]))
		}

		for {
			frame, frameLen, ok := extractTeltonikaFrame(buf)
			if !ok {
				break
			}

			log.Printf("🧩 Extracted frame HEX: %s", hex.EncodeToString(frame))
			buf = buf[frameLen:]

			records, err := parseTeltonikaDataField(frame)
			if err != nil {
				log.Printf("❌ Frame parse error: %v", err)
				continue
			}

			log.Printf("🔎 Parsed %d AVL record(s) for %s", len(records), imei)

			if err := storePositionsBatch(deviceID, imei, records); err != nil {
				log.Printf("❌ DB batch insert failed: %v", err)
			}

			payload := make([]map[string]interface{}, 0, len(records))
			for _, avl := range records {
				if avl.Latitude == 0 || avl.Longitude == 0 {
					log.Printf("⚠️ Backend skip zero lat/lng: %+v", avl)
					continue
				}
				payload = append(payload, map[string]interface{}{
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

			if err := postPositionsToBackend(payload); err != nil {
				log.Printf("❌ Failed backend post: %v", err)
			}

			sendACK(conn, len(records))
		}
	}
}

// ===============================
//   TELTONIKA FRAME EXTRACTOR
// ===============================

func extractTeltonikaFrame(buf []byte) (frame []byte, frameLen int, ok bool) {
	if len(buf) < 12 {
		return nil, 0, false
	}

	if !(buf[0] == 0 && buf[1] == 0 && buf[2] == 0 && buf[3] == 0) {
		for i := 1; i < len(buf)-3; i++ {
			if buf[i] == 0 && buf[i+1] == 0 && buf[i+2] == 0 && buf[i+3] == 0 {
				return extractTeltonikaFrame(buf[i:])
			}
		}
		return nil, len(buf), false
	}

	if len(buf) < 8 {
		return nil, 0, false
	}

	dataLen := int(binary.BigEndian.Uint32(buf[4:8]))
	total := 8 + dataLen + 4

	if len(buf) < total {
		return nil, 0, false
	}

	data := buf[8 : 8+dataLen]
	return data, total, true
}

// ===============================
//        IMEI READER
// ===============================

func readIMEI(conn net.Conn) (string, error) {
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}
	raw := string(buf[:n])

	re := regexp.MustCompile(\D)
	imei := re.ReplaceAllString(raw, "")

	conn.Write([]byte{0x01})

	log.Printf("🔢 Raw IMEI read: %q, Cleaned IMEI: %s", raw, imei)
	return imei, nil
}

// ===============================
//        DEVICE LOOKUP
// ===============================

func ensureDevice(imei string) (int, error) {
	var id int

	err := db.QueryRow("SELECT id FROM devices WHERE imei=$1", imei).Scan(&id)
	if err == nil {
		return id, nil
	}

	resp, err := httpClient.Get("https://mytrack-production.up.railway.app/api/devices/list")
	if err != nil {
		return 0, fmt.Errorf("failed GET devices list: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var devices []Device
	if err := json.Unmarshal(body, &devices); err != nil {
		return 0, fmt.Errorf("parse devices list failed: %v", err)
	}

	for _, d := range devices {
		if strings.TrimSpace(d.IMEI) == imei {
			_, _ = db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
			return d.ID, nil
		}
	}
	return 0, fmt.Errorf("device IMEI %s not found", imei)
}

// ===============================
//   TELTONIKA DATA PARSER
// ===============================

func parseTeltonikaDataField(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short")
	}

	reader := bytes.NewReader(data)
	var codecID, recordCount byte
	binary.Read(reader, binary.BigEndian, &codecID)
	binary.Read(reader, binary.BigEndian, &recordCount)

	records := make([]*AVLData, 0, recordCount)

	for i := 0; i < int(recordCount); i++ {
		var (
			timestamp        uint64
			priority         byte
			latRaw, lngRaw   int32
			alt, angle, spd  uint16
			sats             byte
		)

		binary.Read(reader, binary.BigEndian, &timestamp)
		binary.Read(reader, binary.BigEndian, &priority)
		binary.Read(reader, binary.BigEndian, &latRaw)
		binary.Read(reader, binary.BigEndian, &lngRaw)
		binary.Read(reader, binary.BigEndian, &alt)
		binary.Read(reader, binary.BigEndian, &angle)
		binary.Read(reader, binary.BigEndian, &sats)
		binary.Read(reader, binary.BigEndian, &spd)

		if err := skipIO(reader); err != nil {
			log.Printf("⚠️ IO skip error: %v", err)
			break
		}

		ts := int64(timestamp)
		if ts <= 0 || ts > 32503680000000 {
			log.Printf("⚠️ Invalid ts: %d", ts)
			continue
		}

		lat := float64(latRaw) / 1e7
		lng := float64(lngRaw) / 1e7
		if lat == 0 || lng == 0 {
			log.Printf("⚠️ Zero lat/lng")
			continue
		}

		records = append(records, &AVLData{
			Timestamp:  time.UnixMilli(ts),
			Latitude:   lat,
			Longitude:  lng,
			Altitude:   int(alt),
			Angle:      int(angle),
			Satellites: int(sats),
			Speed:      int(spd),
		})
	}

	return records, nil
}

// ===============================
//     IO SKIPPER (SAFE)
// ===============================

func skipIO(r *bytes.Reader) error {
	var n1, n2, n4, n8 byte

	if err := binary.Read(r, binary.BigEndian, &n1); err != nil {
		return err
	}
	for i := 0; i < int(n1); i++ {
		r.Seek(2, io.SeekCurrent)
	}

	if err := binary.Read(r, binary.BigEndian, &n2); err != nil {
		return err
	}
	for i := 0; i < int(n2); i++ {
		r.Seek(3, io.SeekCurrent)
	}

	if err := binary.Read(r, binary.BigEndian, &n4); err != nil {
		return err
	}
	for i := 0; i < int(n4); i++ {
		r.Seek(5, io.SeekCurrent)
	}

	if err := binary.Read(r, binary.BigEndian, &n8); err != nil {
		return err
	}
	for i := 0; i < int(n8); i++ {
		r.Seek(9, io.SeekCurrent)
	}
	return nil
}

// ===============================
//     DB BATCH INSERT
// ===============================

func storePositionsBatch(deviceID int, imei string, recs []*AVLData) error {
	if len(recs) == 0 {
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

	for _, r := range recs {
		_, err := stmt.Exec(
			deviceID, r.Latitude, r.Longitude, r.Speed,
			r.Angle, r.Altitude, r.Satellites, r.Timestamp, imei,
		)
		if err != nil {
			log.Println("⚠️ Insert err:", err)
		}
	}

	return tx.Commit()
}

// ===============================
//      BACKEND FORWARDER
// ===============================

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

// ===============================
//       ACK SENDER
// ===============================

func sendACK(conn net.Conn, count int) {
	ack := make([]byte, 5)
	binary.BigEndian.PutUint32(ack, uint32(count))
	ack[4] = 0x01
	conn.Write(ack)
}

// ===============================
//         HELPERS
// ===============================

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}