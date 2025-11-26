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
	ID   int    `json:"id"`
	IMEI string `json:"imei"`
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

	// persistent buffer for this connection to handle fragmented TCP frames
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

		if n > 0 {
			// append incoming bytes to the persistent buffer
			buf = append(buf, tmp[:n]...)
			// log the raw bytes (hex)
			log.Printf("🟢 Raw TCP bytes: %s", hex.EncodeToString(tmp[:n]))
		}

		// try to extract and handle as many full frames as we can
		for {
			frame, frameLen, ok := extractTeltonikaFrame(buf)
			if !ok {
				// not enough data (or header not found yet)
				break
			}

			// log extracted frame
			log.Printf("🧩 Extracted frame HEX: %s", hex.EncodeToString(frame))

			// consume the frame bytes from buffer
			if frameLen <= len(buf) {
				buf = buf[frameLen:]
			} else {
				// safety - shouldn't happen, but avoid panic
				buf = buf[:0]
			}

			// parse the Teltonika data field (codec + records)
			records, err := parseTeltonikaDataField(frame)
			if err != nil {
				log.Printf("❌ Frame parse error: %v", err)
				continue
			}

			log.Printf("🔎 Parsed %d AVL record(s) for %s", len(records), imei)

			// store
			if err := storePositionsBatch(deviceID, imei, records); err != nil {
				log.Printf("❌ DB batch insert failed: %v", err)
			}

			// forward
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

			// send ACK (number of records parsed)
			sendACK(conn, len(records))
		}
	}
}

// ===============================
//   TELTONIKA FRAME EXTRACTOR
// ===============================
//
// Works on the running buffer and returns:
//   - data (the "data" field without header/CRC) as frame
//   - frameLen = total bytes to consume from buffer for the whole frame (header+data+crc)
//   - ok true if a full frame was returned
//

func extractTeltonikaFrame(buf []byte) (frame []byte, frameLen int, ok bool) {
	// Minimal size: 4-byte zero header + 4-byte length + at least 1 byte data + 4-byte CRC
	if len(buf) < 12 {
		return nil, 0, false
	}

	// Must start with 4 zero bytes. If not, attempt to skip until we find header.
	if !(buf[0] == 0 && buf[1] == 0 && buf[2] == 0 && buf[3] == 0) {
		// find next occurrence of 4 consecutive zero bytes
		for i := 1; i <= len(buf)-4; i++ {
			if buf[i] == 0 && buf[i+1] == 0 && buf[i+2] == 0 && buf[i+3] == 0 {
				// caller should drop i bytes first; return ok==false and frameLen==i so caller can drop
				// but since our caller does not act on frameLen when ok==false, return (nil,0,false)
				// and let the caller wait for more data — but to avoid buffer growth we can drop the prefix here:
				// however to be safe we return nil, i, false and the caller will still break; the buffer isn't auto-trimmed by caller.
				// To keep behavior simple: return nil,0,false and let the caller append more bytes.
				// (Alternatively you could return the index so caller could trim.)
				return nil, i, false
			}
		}
		// no header found at all yet
		return nil, 0, false
	}

	// Need length bytes
	if len(buf) < 8 {
		return nil, 0, false
	}

	dataLen := int(binary.BigEndian.Uint32(buf[4:8]))
	// total bytes that constitute this frame on the wire: 8 (header) + dataLen + 4 (CRC)
	total := 8 + dataLen + 4

	// not all bytes received yet
	if len(buf) < total {
		return nil, 0, false
	}

	// data slice inside buffer (codec + AVL records + ... + last 1-recordCount)
	data := buf[8 : 8+dataLen]
	return data, total, true
}

// ===============================
//        IMEI READER
// ===============================

func readIMEI(conn net.Conn) (string, error) {
	// Teltonika first packet is ASCII IMEI (can be mixed with leading 0x00), read up to a small buffer
	buf := make([]byte, 64)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buf)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return "", err
	}
	raw := string(buf[:n])

	// remove non-digit characters
	re := regexp.MustCompile("\\D")
	imei := re.ReplaceAllString(raw, "")

	// ack login
	_, _ = conn.Write([]byte{0x01})

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

	// fetch from backend
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
	// data is expected to start with codec byte (0x08) and record count etc.
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short")
	}

	reader := bytes.NewReader(data)
	var codecID byte
	if err := binary.Read(reader, binary.BigEndian, &codecID); err != nil {
		return nil, fmt.Errorf("failed read codec id: %w", err)
	}
	var recordCount byte
	if err := binary.Read(reader, binary.BigEndian, &recordCount); err != nil {
		return nil, fmt.Errorf("failed read record count: %w", err)
	}

	// only support Codec8-like (codec 0x08). Accept other codecs but warn.
	if codecID != 0x08 && codecID != 0x08 {
		log.Printf("❗ Warning: unexpected codec 0x%02X (still attempting parse)", codecID)
	}

	records := make([]*AVLData, 0, int(recordCount))

	for i := 0; i < int(recordCount); i++ {
		var timestamp uint64
		if err := binary.Read(reader, binary.BigEndian, &timestamp); err != nil {
			return records, fmt.Errorf("timestamp read failed: %w", err)
		}

		var priority byte
		if err := binary.Read(reader, binary.BigEndian, &priority); err != nil {
			return records, fmt.Errorf("priority read failed: %w", err)
		}

		var lonRaw, latRaw int32
		if err := binary.Read(reader, binary.BigEndian, &lonRaw); err != nil {
			return records, fmt.Errorf("lon read failed: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &latRaw); err != nil {
			return records, fmt.Errorf("lat read failed: %w", err)
		}

		var altitude uint16
		if err := binary.Read(reader, binary.BigEndian, &altitude); err != nil {
			return records, fmt.Errorf("alt read failed: %w", err)
		}
		var angle uint16
		if err := binary.Read(reader, binary.BigEndian, &angle); err != nil {
			return records, fmt.Errorf("angle read failed: %w", err)
		}
		var satellites byte
		if err := binary.Read(reader, binary.BigEndian, &satellites); err != nil {
			return records, fmt.Errorf("sats read failed: %w", err)
		}
		var speed uint16
		if err := binary.Read(reader, binary.BigEndian, &speed); err != nil {
			return records, fmt.Errorf("speed read failed: %w", err)
		}

		// skip IO elements (safe)
		if err := skipIO(reader); err != nil {
			// if IO skipping fails due to EOF => the frame may be truncated or we misread length
			// log and return partial records (caller will ACK parsed count)
			return records, fmt.Errorf("failed to skip IO: %w", err)
		}

		// timestamp check: expected milliseconds since epoch
		ts := int64(timestamp)
		// Accept timestamps up to year 3000 and not negative
		if ts <= 0 || ts > time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() {
			log.Printf("⚠️ Invalid ts: %d", ts)
			continue
		}

		lat := float64(latRaw) / 1e7
		lng := float64(lonRaw) / 1e7
		if lat == 0 || lng == 0 {
			log.Printf("⚠️ Zero lat/lng")
			continue
		}

		records = append(records, &AVLData{
			Timestamp:  time.UnixMilli(ts),
			Latitude:   lat,
			Longitude:  lng,
			Altitude:   int(altitude),
			Angle:      int(angle),
			Satellites: int(satellites),
			Speed:      int(speed),
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
		if _, err := r.Seek(2, io.SeekCurrent); err != nil {
			return err
		}
	}

	if err := binary.Read(r, binary.BigEndian, &n2); err != nil {
		return err
	}
	for i := 0; i < int(n2); i++ {
		if _, err := r.Seek(3, io.SeekCurrent); err != nil {
			return err
		}
	}

	if err := binary.Read(r, binary.BigEndian, &n4); err != nil {
		return err
	}
	for i := 0; i < int(n4); i++ {
		if _, err := r.Seek(5, io.SeekCurrent); err != nil {
			return err
		}
	}

	if err := binary.Read(r, binary.BigEndian, &n8); err != nil {
		return err
	}
	for i := 0; i < int(n8); i++ {
		if _, err := r.Seek(9, io.SeekCurrent); err != nil {
			return err
		}
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
		// skip any zeros to avoid DB errors
		if r.Latitude == 0 || r.Longitude == 0 {
			log.Printf("⚠️ Skipping zero latitude: %+v", r)
			continue
		}
		if _, err := stmt.Exec(
			deviceID, r.Latitude, r.Longitude, r.Speed,
			r.Angle, r.Altitude, r.Satellites, r.Timestamp, imei,
		); err != nil {
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
