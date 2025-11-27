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

var (
	tcpServerHost   string
	backendTrackURL string
	db              *sql.DB
	httpClient      = &http.Client{Timeout: 10 * time.Second}
	wg              sync.WaitGroup
	positionsHasIoData bool
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

	positionsHasIoData = checkPositionsHasIoData()
}

func checkPositionsHasIoData() bool {
	var col string
	err := db.QueryRow(`
		SELECT column_name FROM information_schema.columns 
		WHERE table_name='positions' AND column_name='io_data' LIMIT 1
	`).Scan(&col)
	return err == nil && col == "io_data"
}

func main() {
	listener, err := net.Listen("tcp", tcpServerHost)
	if err != nil {
		log.Fatalf("❌ Failed to start TCP server: %v", err)
	}
	defer listener.Close()

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

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	imei, err := readIMEI(conn)
	if err != nil {
		log.Println("❌ Failed IMEI:", err)
		return
	}

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
		}

		for len(residual) >= 4 {
			packetLen := int(binary.BigEndian.Uint32(residual[:4]))
			if packetLen <= 0 || packetLen > 5*1024*1024 {
				residual = residual[4:]
				continue
			}

			if len(residual) < 4+packetLen {
				break
			}

			frame := residual[4 : 4+packetLen]
			codecPayload, err := normalizeToCodec8(frame)
			if err != nil {
				residual = residual[4+packetLen:]
				continue
			}

			records, err := parseCodec(codecPayload)
			if err != nil {
				residual = residual[4+packetLen:]
				continue
			}

			validRecords := make([]*AVLData, 0, len(records))
			for _, r := range records {
				if r != nil && r.Latitude != 0 && r.Longitude != 0 {
					validRecords = append(validRecords, r)
				}
			}

			if err := storePositionsBatch(deviceID, imei, validRecords); err != nil {
				log.Printf("❌ DB batch insert failed: %v", err)
			}

			payload := make([]map[string]interface{}, 0, len(validRecords))
			for _, avl := range validRecords {
				payload = append(payload, map[string]interface{}{
					"device_id":  deviceID,
					"imei":       imei,
					"timestamp":  avl.Timestamp.UTC().Format(time.RFC3339),
					"latitude":   avl.Latitude,
					"longitude":  avl.Longitude,
					"speed":      avl.Speed,
					"angle":      avl.Angle,
					"altitude":   avl.Altitude,
					"satellites": avl.Satellites,
					"io_data":    avl.IOData,
				})
			}

			_ = postPositionsToBackend(payload)
			sendACK(conn, len(validRecords))

			residual = residual[4+packetLen:]
		}
	}
}

func normalizeToCodec8(frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return nil, fmt.Errorf("empty frame")
	}
	if frame[0] == 0x08 || frame[0] == 0x8E {
		return frame, nil
	}
	idx := bytes.IndexByte(frame, 0x08)
	if idx == -1 {
		idx = bytes.IndexByte(frame, 0x8E)
		if idx == -1 {
			return nil, fmt.Errorf("codec not found")
		}
	}
	return frame[idx:], nil
}

func readIMEI(conn net.Conn) (string, error) {
	buf := make([]byte, 64)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buf)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return "", err
	}

	raw := buf[:n]
	if len(raw) >= 2 && raw[0] == 0x00 && raw[1] == 0x0F {
		raw = raw[2:]
	}

	re := regexp.MustCompile(`\D`)
	imei := re.ReplaceAllString(string(raw), "")

	_, _ = conn.Write([]byte{0x01})
	return imei, nil
}

func ensureDevice(imei string) (int, error) {
	var id int
	err := db.QueryRow("SELECT id FROM devices WHERE imei=$1", imei).Scan(&id)
	if err == nil {
		return id, nil
	}

	resp, err := httpClient.Get("https://mytrack-production.up.railway.app/api/devices/list")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var devices []Device
	_ = json.Unmarshal(body, &devices)

	for _, d := range devices {
		if strings.TrimSpace(d.IMEI) == imei {
			_, _ = db.Exec(
				"INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING",
				d.ID, d.IMEI,
			)
			return d.ID, nil
		}
	}
	return 0, fmt.Errorf("device not registered")
}

func parseCodec(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("too short")
	}
	reader := bytes.NewReader(data)

	var codecID byte
	binary.Read(reader, binary.BigEndian, &codecID)

	if codecID != 0x08 && codecID != 0x8E {
		return nil, fmt.Errorf("unsupported codec %02X", codecID)
	}

	var count byte
	binary.Read(reader, binary.BigEndian, &count)

	records := make([]*AVLData, 0, count)
	for i := 0; i < int(count); i++ {
		avl, err := parseSingleAVL(reader)
		if err == nil && avl != nil {
			records = append(records, avl)
		}
	}

	return records, nil
}

func parseSingleAVL(r *bytes.Reader) (*AVLData, error) {
	if r.Len() < 30 {
		return nil, fmt.Errorf("too short")
	}

	var timestamp uint64
	binary.Read(r, binary.BigEndian, &timestamp)

	if timestamp > uint64(time.Now().Add(365*24*time.Hour).UnixMilli()) {
		timestamp = uint64(time.Now().UnixMilli())
	}

	var priority byte
	binary.Read(r, binary.BigEndian, &priority)

	var lonRaw, latRaw int32
	binary.Read(r, binary.BigEndian, &lonRaw)
	binary.Read(r, binary.BigEndian, &latRaw)

	var altitude, angle uint16
	binary.Read(r, binary.BigEndian, &altitude)
	binary.Read(r, binary.BigEndian, &angle)

	var sats byte
	binary.Read(r, binary.BigEndian, &sats)
	var speed uint16
	binary.Read(r, binary.BigEndian, &speed)

	ioData, _ := parseIOElements(r)

	return &AVLData{
		Timestamp:  time.UnixMilli(int64(timestamp)),
		Latitude:   float64(latRaw) / 1e7,
		Longitude:  float64(lonRaw) / 1e7,
		Altitude:   int(altitude),
		Angle:      int(angle),
		Satellites: int(sats),
		Speed:      int(speed),
		IOData:     ioData,
	}, nil
}

func parseIOElements(r *bytes.Reader) (map[uint8]interface{}, error) {
	ioData := make(map[uint8]interface{})

	readCount := func() int {
		if r.Len() < 1 {
			return 0
		}
		var n byte
		binary.Read(r, binary.BigEndian, &n)
		return int(n)
	}

	for _, size := range []int{1, 2, 4, 8} {
		count := readCount()
		for i := 0; i < count; i++ {
			if r.Len() < 1+size {
				return ioData, fmt.Errorf("IO block truncated")
			}
			var id byte
			binary.Read(r, binary.BigEndian, &id)

			switch size {
			case 1:
				var v byte
				binary.Read(r, binary.BigEndian, &v)
				ioData[id] = v
			case 2:
				var v uint16
				binary.Read(r, binary.BigEndian, &v)
				ioData[id] = v
			case 4:
				var v uint32
				binary.Read(r, binary.BigEndian, &v)
				ioData[id] = v
			case 8:
				var v uint64
				binary.Read(r, binary.BigEndian, &v)
				ioData[id] = v
			}
		}
	}

	return ioData, nil
}

func storePositionsBatch(deviceID int, imei string, recs []*AVLData) error {
	if len(recs) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var stmt *sql.Stmt
	if positionsHasIoData {
		stmt, err = tx.Prepare(`
			INSERT INTO positions 
			(device_id, lat, lng, speed, angle, altitude, satellites, timestamp, imei, io_data)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`)
	} else {
		stmt, err = tx.Prepare(`
			INSERT INTO positions 
			(device_id, lat, lng, speed, angle, altitude, satellites, timestamp, imei)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`)
	}
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range recs {
		ioJSON, _ := json.Marshal(r.IOData)

		if positionsHasIoData {
			_, err = stmt.Exec(
				deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp.UTC(),
				imei, ioJSON,
			)
		} else {
			_, err = stmt.Exec(
				deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp.UTC(),
				imei,
			)
		}

		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

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

	io.ReadAll(resp.Body)
	return nil
}

func sendACK(conn net.Conn, count int) {
	ack := make([]byte, 5)
	binary.BigEndian.PutUint32(ack, uint32(count))
	ack[4] = 0x01
	conn.Write(ack)
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
