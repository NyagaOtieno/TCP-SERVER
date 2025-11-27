package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"io"
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
	Timestamp  time.Time              `json:"timestamp"`
	Latitude   float64                `json:"latitude"`
	Longitude  float64                `json:"longitude"`
	Altitude   int                    `json:"altitude"`
	Angle      int                    `json:"angle"`
	Satellites int                    `json:"satellites"`
	Speed      int                    `json:"speed"`
	IOData     map[uint8]interface{} `json:"io_data,omitempty"`
}

type Device struct {
	ID   int    `json:"id"`
	IMEI string `json:"imei"`
}

var (
	tcpServerHost     string
	backendTrackURL   string
	db                *sql.DB
	httpClient        = &http.Client{Timeout: 10 * time.Second}
	wg                sync.WaitGroup
	positionsHasIoData bool
)

func init() {
	_ = godotenv.Load()
	tcpServerHost = getEnv("TCP_SERVER_HOST", "0.0.0.0:5027")
	backendTrackURL = getEnv("BACKEND_TRACK_URL", "https://mytrack-production.up.railway.app/api/track")

	pgURL := getEnv("DATABASE_URL", "")
	if pgURL == "" {
		os.Exit(1)
	}

	var err error
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		os.Exit(1)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		os.Exit(1)
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
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			defer func() { recover() }()
			handleConnection(c)
		}(conn)
	}

	wg.Wait()
}

func handleConnection(conn net.Conn) {
	imei, err := readIMEI(conn)
	if err != nil {
		return
	}

	deviceID, err := ensureDevice(imei)
	if err != nil {
		return
	}

	residual := make([]byte, 0)
	tmp := make([]byte, 4096)

	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		n, err := conn.Read(tmp)
		if err != nil {
			return
		}

		if n > 0 {
			residual = append(residual, tmp[:n]...)
		}

		for {
			if len(residual) < 4 {
				break
			}

			packetLen := int(binary.BigEndian.Uint32(residual[:4]))
			if packetLen <= 0 || packetLen > 10*1024*1024 {
				residual = residual[1:]
				continue
			}

			if len(residual) < 4+packetLen {
				break
			}

			frame := residual[4 : 4+packetLen]
			residual = residual[4+packetLen:]

			codecPayload, err := normalizeToCodec8(frame)
			if err != nil {
				continue
			}

			records, err := parseCodec(codecPayload)
			if err != nil {
				continue
			}

			validRecords := filterValidRecords(records)
			storePositionsBatch(deviceID, imei, validRecords)
			postPositionsToBackend(validRecords, deviceID, imei)
			sendACK(conn, len(validRecords))
		}
	}
}

func normalizeToCodec8(frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return nil, io.EOF
	}
	if frame[0] == 0x08 || frame[0] == 0x8E {
		return frame, nil
	}
	idx := bytes.IndexByte(frame, 0x08)
	if idx == -1 {
		idx = bytes.IndexByte(frame, 0x8E)
		if idx == -1 {
			return nil, io.EOF
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
	conn.Write([]byte{0x01})
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
	json.Unmarshal(body, &devices)

	for _, d := range devices {
		if strings.TrimSpace(d.IMEI) == imei {
			db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
			return d.ID, nil
		}
	}
	return 0, io.EOF
}

func parseCodec(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, io.EOF
	}
	reader := bytes.NewReader(data)
	var codecID byte
	binary.Read(reader, binary.BigEndian, &codecID)
	if codecID != 0x08 && codecID != 0x8E {
		return nil, io.EOF
	}

	var recordCount byte
	binary.Read(reader, binary.BigEndian, &recordCount)

	records := make([]*AVLData, 0, int(recordCount))
	for i := 0; i < int(recordCount); i++ {
		avl, _ := parseSingleAVL(reader)
		if avl != nil {
			records = append(records, avl)
		}
	}

	return records, nil
}

func parseSingleAVL(r *bytes.Reader) (*AVLData, error) {
	if r.Len() < 15 {
		return nil, io.EOF
	}

	var timestamp uint64
	binary.Read(r, binary.BigEndian, &timestamp)
	if timestamp == 0 {
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

	var satellites byte
	binary.Read(r, binary.BigEndian, &satellites)

	var speed uint16
	binary.Read(r, binary.BigEndian, &speed)

	ioData, _ := parseIOElements(r)

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

func parseIOElements(r *bytes.Reader) (map[uint8]interface{}, error) {
	ioData := make(map[uint8]interface{})
	readCount := func() (uint16, error) {
		var cnt uint16
		binary.Read(r, binary.BigEndian, &cnt)
		return cnt, nil
	}

	for _, size := range []int{1, 2, 4, 8} {
		n, _ := readCount()
		for i := 0; i < int(n); i++ {
			var id byte
			binary.Read(r, binary.BigEndian, &id)
			switch size {
			case 1:
				var val byte
				binary.Read(r, binary.BigEndian, &val)
				ioData[id] = val
			case 2:
				var val uint16
				binary.Read(r, binary.BigEndian, &val)
				ioData[id] = val
			case 4:
				var val uint32
				binary.Read(r, binary.BigEndian, &val)
				ioData[id] = val
			case 8:
				var val uint64
				binary.Read(r, binary.BigEndian, &val)
				ioData[id] = val
			}
		}
	}
	return ioData, nil
}

func filterValidRecords(records []*AVLData) []*AVLData {
	valid := make([]*AVLData, 0, len(records))
	for _, r := range records {
		if r != nil && r.Latitude != 0 && r.Longitude != 0 {
			valid = append(valid, r)
		}
	}
	return valid
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
			stmt.Exec(deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp.UTC(),
				imei, ioJSON)
		} else {
			stmt.Exec(deviceID, r.Latitude, r.Longitude, r.Speed,
				r.Angle, r.Altitude, r.Satellites, r.Timestamp.UTC(), imei)
		}
	}

	return tx.Commit()
}

func postPositionsToBackend(records []*AVLData, deviceID int, imei string) error {
	if len(records) == 0 {
		return nil
	}

	payload := make([]map[string]interface{}, 0, len(records))
	for _, r := range records {
		payload = append(payload, map[string]interface{}{
			"device_id":  deviceID,
			"imei":       imei,
			"timestamp":  r.Timestamp.UTC().Format(time.RFC3339),
			"latitude":   r.Latitude,
			"longitude":  r.Longitude,
			"speed":      r.Speed,
			"angle":      r.Angle,
			"altitude":   r.Altitude,
			"satellites": r.Satellites,
			"io_data":    r.IOData,
		})
	}

	data, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", backendTrackURL, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := httpClient.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
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
