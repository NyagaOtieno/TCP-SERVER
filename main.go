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

// =======================
//        STRUCTS
// =======================

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

// =======================
//     GLOBAL CONFIG
// =======================

var (
	tcpServerHost   string
	backendTrackURL string
	db              *sql.DB
	httpClient      = &http.Client{Timeout: 10 * time.Second}
	wg              sync.WaitGroup
)

// =======================
//        INIT
// =======================

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
		wg.Add(1)
		go handleConnection(conn)
	}

	wg.Wait()
}

// =======================
//   CONNECTION HANDLER
// =======================

func handleConnection(conn net.Conn) {
	defer wg.Done()
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

	residual := make([]byte, 0)
	tmp := make([]byte, 4096)

	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				log.Printf("🔌 Read error for %s: %v", imei, err)
			}
			return
		}
		if n > 0 {
			residual = append(residual, tmp[:n]...)
			log.Printf("🟢 Raw TCP bytes: %s", fmt.Sprintf("%x", tmp[:n]))
		}

		for len(residual) >= 4 {
			packetLen := int(binary.BigEndian.Uint32(residual[:4]))
			if len(residual) < 4+packetLen {
				break
			}

			frame := residual[4 : 4+packetLen]

			records, err := parseCodec8(frame)
			if err != nil {
				log.Printf("❌ Frame parse error: %v", err)
				residual = residual[4+packetLen:]
				continue
			}

			log.Printf("🔎 Parsed %d AVL record(s) for %s", len(records), imei)

			if err := storePositionsBatch(deviceID, imei, records); err != nil {
				log.Printf("❌ DB batch insert failed: %v", err)
			}

			payload := make([]map[string]interface{}, 0, len(records))
			for _, avl := range records {
				if avl.Latitude == 0 || avl.Longitude == 0 {
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
					"io_data":    avl.IOData,
				})
			}

			if err := postPositionsToBackend(payload); err != nil {
				log.Printf("❌ Failed backend post: %v", err)
			}

			sendACK(conn, len(records))
			residual = residual[4+packetLen:]
		}
	}
}

// ===============================
//       IMEI READER
// ===============================

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
	_, _ = conn.Write([]byte{0x01})
	log.Printf("🔢 Raw IMEI: %q, Cleaned IMEI: %s", raw, imei)
	return imei, nil
}

// ===============================
//       DEVICE LOOKUP
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
//  CODEC8 PARSER
// ===============================

func parseCodec8(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("frame too short")
	}
	reader := bytes.NewReader(data)

	var codecID byte
	var recordCount byte
	if err := binary.Read(reader, binary.BigEndian, &codecID); err != nil {
		return nil, err
	}
	if codecID != 0x08 {
		return nil, fmt.Errorf("unexpected codec ID: %d", codecID)
	}
	if err := binary.Read(reader, binary.BigEndian, &recordCount); err != nil {
		return nil, err
	}

	records := make([]*AVLData, 0, int(recordCount))
	for i := 0; i < int(recordCount); i++ {
		avl, err := parseSingleAVL(reader)
		if err != nil {
			return records, err
		}
		records = append(records, avl)
	}

	// skip 1 byte for number of records at the end (Codec8)
	var recordCount2 byte
	_ = binary.Read(reader, binary.BigEndian, &recordCount2)

	return records, nil
}

func parseSingleAVL(r *bytes.Reader) (*AVLData, error) {
	var timestamp uint64
	if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		return nil, err
	}
	var priority byte
	_ = binary.Read(r, binary.BigEndian, &priority)

	var lonRaw, latRaw int32
	if err := binary.Read(r, binary.BigEndian, &lonRaw); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &latRaw); err != nil {
		return nil, err
	}

	var altitude uint16
	var angle uint16
	var satellites byte
	var speed uint16
	binary.Read(r, binary.BigEndian, &altitude)
	binary.Read(r, binary.BigEndian, &angle)
	binary.Read(r, binary.BigEndian, &satellites)
	binary.Read(r, binary.BigEndian, &speed)

	ioData := parseIOElements(r)

	return &AVLData{
		Timestamp:  time.UnixMilli(int64(timestamp)),
		Latitude:   float64(latRaw) / 1e7,
		Longitude:  float64(lonRaw) / 1e7,
		Altitude:   int(altitude),
		Angle:      int(angle),
		Satellites: int(satellites),
		Speed:      int(speed),
		IOData:     ioData,
	}, nil
}

// ===============================
//     IO ELEMENT PARSER
// ===============================

func parseIOElements(r *bytes.Reader) map[uint8]interface{} {
	ioData := make(map[uint8]interface{})
	var n1, n2, n4, n8 byte

	binary.Read(r, binary.BigEndian, &n1)
	for i := 0; i < int(n1); i++ {
		var id, val uint8
		binary.Read(r, binary.BigEndian, &id)
		binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	binary.Read(r, binary.BigEndian, &n2)
	for i := 0; i < int(n2); i++ {
		var id uint8
		var val uint16
		binary.Read(r, binary.BigEndian, &id)
		binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	binary.Read(r, binary.BigEndian, &n4)
	for i := 0; i < int(n4); i++ {
		var id uint8
		var val uint32
		binary.Read(r, binary.BigEndian, &id)
		binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	binary.Read(r, binary.BigEndian, &n8)
	for i := 0; i < int(n8); i++ {
		var id uint8
		var val uint64
		binary.Read(r, binary.BigEndian, &id)
		binary.Read(r, binary.BigEndian, &val)
		ioData[id] = val
	}

	return ioData
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
		INSERT INTO positions (device_id, lat, lng, speed, angle, altitude, satellites, timestamp, imei, io_data)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range recs {
		if r.Latitude == 0 || r.Longitude == 0 {
			continue
		}
		_, _ = stmt.Exec(
			deviceID, r.Latitude, r.Longitude, r.Speed,
			r.Angle, r.Altitude, r.Satellites, r.Timestamp, imei, r.IOData,
		)
	}

	return tx.Commit()
}

// ===============================
//     BACKEND FORWARDER
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
	_, _ = conn.Write(ack)
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
