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

	// Persistent buffer for this device
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

		if n == 0 {
			// no data; continue
			continue
		}

		// append incoming bytes into persistent buffer
		buf = append(buf, tmp[:n]...)

		// Log raw TCP bytes in hex (trim long output to reasonable length)
		if n > 0 {
			// print only up to 4096 bytes to avoid giant logs
			toLog := tmp[:n]
			if len(toLog) > 4096 {
				toLog = toLog[:4096]
			}
			log.Printf("🟢 Raw TCP bytes (len=%d): %s", n, hex.EncodeToString(toLog))
		}

		// Extract frames as long as there is a complete one available
		for {
			frame, frameLen, ok := extractTeltonikaFrame(buf)
			if !ok {
				break // need more data
			}

			// Log extracted frame in hex (frame is the 'data' section, i.e. codec+payload)
			log.Printf("🧩 Extracted frame HEX (dataLen=%d): %s", len(frame), hex.EncodeToString(frame))

			// Advance buffer by the length of the full frame (header+data+crc)
			buf = buf[frameLen:]

			// Parse frame
			records, err := parseTeltonikaDataField(frame)
			if err != nil {
				log.Printf("❌ Frame parse error: %v", err)
				continue
			}

			log.Printf("🔎 Parsed %d AVL record(s) for %s", len(records), imei)

			// Store in DB
			if err := storePositionsBatch(deviceID, imei, records); err != nil {
				log.Printf("❌ DB batch insert failed: %v", err)
			}

			// Forward to backend
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
//
// This function returns the data field (codec + payload) and the
// total consumed length (header + data + crc). It does not mutate buf.
// If it returns ok==false you should wait for more data.
// ===============================

func extractTeltonikaFrame(buf []byte) (frame []byte, frameLen int, ok bool) {
	// need at least header (4 zeros + 4 len) + 4 CRC
	if len(buf) < 12 {
		return nil, 0, false
	}

	// find 4-zero header at start; if not at buf[0] scan for it
	if !(buf[0] == 0 && buf[1] == 0 && buf[2] == 0 && buf[3] == 0) {
		// search for next occurrence of header bytes
		found := -1
		for i := 1; i+3 < len(buf); i++ {
			if buf[i] == 0 && buf[i+1] == 0 && buf[i+2] == 0 && buf[i+3] == 0 {
				found = i
				break
			}
		}
		if found == -1 {
			// no header yet; wait for more bytes
			return nil, 0, false
		}
		// header found later: ask caller to advance buffer (caller can slice)
		// We return ok=false and frameLen=found so caller can consume noise until header.
		// But to be safe for the calling code that expects only ok==false to mean "need more data",
		// we will not remove bytes here. The caller (handleConnection) keeps the buffer and
		// will call again once more data arrives; so return need-more-data.
		// (Don't discard here to avoid accidental loss)
		return nil, 0, false
	}

	// We have header at buf[0:4] — ensure length field is present
	if len(buf) < 8 {
		return nil, 0, false
	}

	dataLen := int(binary.BigEndian.Uint32(buf[4:8]))
	// basic sanity check
	if dataLen < 1 || dataLen > 10_000_000 {
		// obviously invalid length — drop the header bytes and wait
		log.Printf("❗ Unexpected dataLen=%d (buffer len=%d); refusing to parse frame", dataLen, len(buf))
		return nil, 0, false
	}

	total := 8 + dataLen + 4 // header(8) + data + CRC(4)
	if len(buf) < total {
		// frame incomplete
		return nil, 0, false
	}

	data := make([]byte, dataLen)
	copy(data, buf[8:8+dataLen])

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

	// ACK login
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

	var devices []struct {
		ID   int    json:"id"
		IMEI string json:"imei"
	}
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
//
// This parser is defensive: every binary.Read is checked for errors.
// It expects 'data' to start with CodecID and recordCount (Codec8 style).
// ===============================

func parseTeltonikaDataField(data []byte) ([]*AVLData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short")
	}

	reader := bytes.NewReader(data)
	var codecID, recordCount byte
	if err := binary.Read(reader, binary.BigEndian, &codecID); err != nil {
		return nil, fmt.Errorf("failed read codecID: %w", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &recordCount); err != nil {
		return nil, fmt.Errorf("failed read recordCount: %w", err)
	}

	// log codec for debugging
	log.Printf("🔧 CodecID=0x%02X records=%d", codecID, recordCount)

	records := make([]*AVLData, 0, recordCount)

	for i := 0; i < int(recordCount); i++ {
		var (
			timestamp uint64
			priority  byte
			latRaw    int32
			lngRaw    int32
			alt       uint16
			angle     uint16
			spd       uint16
			sats      byte
		)

		if err := binary.Read(reader, binary.BigEndian, &timestamp); err != nil {
			return records, fmt.Errorf("read timestamp failed: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &priority); err != nil {
			return records, fmt.Errorf("read priority failed: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &latRaw); err != nil {
			return records, fmt.Errorf("read lat failed: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &lngRaw); err != nil {
			return records, fmt.Errorf("read lng failed: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &alt); err != nil {
			return records, fmt.Errorf("read alt failed: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &angle); err != nil {
			return records, fmt.Errorf("read angle failed: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &sats); err != nil {
			return records, fmt.Errorf("read sats failed: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &spd); err != nil {
			return records, fmt.Errorf("read speed failed: %w", err)
		}

		// Skip IO elements. If incomplete IO is encountered return an error
		if err := skipIO(reader); err != nil {
			return records, fmt.Errorf("skipIO failed: %w", err)
		}

		// timestamp in Codec8 is milliseconds since epoch (uint64)
		ts := int64(timestamp)
		// sanity check: accept timestamps between 2000..3000
		if ts <= 0 || ts > time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() {
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
//
// This reads the IO structure safely. If there aren't enough bytes
// available it returns an error so parseTeltonikaDataField can bail out.
func skipIO(r *bytes.Reader) error {
	// Per Teltonika Codec8 structure: after gps+speed there's an IO element:
	// 1 byte: number of 1-byte IOs (n1)
	// n1 * (1byte id + 1byte value)
	// 1 byte: number of 2-byte IOs (n2)
	// n2 * (1byte id + 2byte value)
	// 1 byte: number of 4-byte IOs (n4)
	// n4 * (1byte id + 4byte value)
	// 1 byte: number of 8-byte IOs (n8)
	// n8 * (1byte id + 8byte value)
	// Then: 1 byte total IO count (not used here)
	// This function will attempt to read exactly that sequence.
	var n1, n2, n4, n8 byte
	if err := binary.Read(r, binary.BigEndian, &n1); err != nil {
		return err
	}
	for i := 0; i < int(n1); i++ {
		// id (1) + value (1)
		if _, err := r.Seek(2, io.SeekCurrent); err != nil {
			return err
		}
	}

	if err := binary.Read(r, binary.BigEndian, &n2); err != nil {
		return err
	}
	for i := 0; i < int(n2); i++ {
		// id (1) + value (2)
		if _, err := r.Seek(3, io.SeekCurrent); err != nil {
			return err
		}
	}

	if err := binary.Read(r, binary.BigEndian, &n4); err != nil {
		return err
	}
	for i := 0; i < int(n4); i++ {
		// id (1) + value (4)
		if _, err := r.Seek(5, io.SeekCurrent); err != nil {
			return err
		}
	}

	if err := binary.Read(r, binary.BigEndian, &n8); err != nil {
		return err
	}
	for i := 0; i < int(n8); i++ {
		// id (1) + value (8)
		if _, err := r.Seek(9, io.SeekCurrent); err != nil {
			return err
		}
	}

	// There is a final "total number of IO elements" byte in many flavors;
	// attempt to read it but ignore if not present:
	var totalIO byte
	if err := binary.Read(r, binary.BigEndian, &totalIO); err == nil {
		// totalIO is present, nothing to do with it here
		_ = totalIO
	} else {
		// no final byte — okay for some devices but returning nil is fine
		// if we got EOF earlier, return that error
		if err != io.EOF {
			// ignore EOF here as some devices omit the final total byte
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
	// Put count into first 4 bytes then trailing 0x01 like many Teltonika examples
	binary.BigEndian.PutUint32(ack[0:4], uint32(count))
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

// ===============================
//   Utility: decode a full hex payload (one-off)
// ===============================
//
// Call this function manually from tests or REPL to decode a raw hex payload string.
// It will print the codec id and attempt to parse the AVL records using same parser.
// (This helper does not alter any other behaviour.)
func DecodeHexFrame(hexStr string) {
	data, err := hex.DecodeString(strings.TrimSpace(hexStr))
	if err != nil {
		log.Printf("decode hex error: %v", err)
		return
	}
	log.Printf("Decoded bytes len=%d: % X", len(data), data)
	// If the hex string is a full TCP frame including the 8-byte header and 4-byte CRC,
	// try to detect and extract the data field.
	if len(data) >= 12 && data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 0 {
		dataLen := int(binary.BigEndian.Uint32(data[4:8]))
		if 8+dataLen+4 == len(data) {
			payload := data[8 : 8+dataLen]
			log.Printf("Detected framed payload (dataLen=%d). Parsing...", dataLen)
			records, err := parseTeltonikaDataField(payload)
			if err != nil {
				log.Printf("parse error: %v", err)
			} else {
				log.Printf("Parsed %d records from hex payload", len(records))
				for i, r := range records {
					log.Printf("Record %d: ts=%s lat=%.7f lng=%.7f spd=%d sats=%d alt=%d angle=%d",
						i+1, r.Timestamp.UTC().Format(time.RFC3339Nano), r.Latitude, r.Longitude, r.Speed, r.Satellites, r.Altitude, r.Angle)
				}
			}
			return
		}
	}
	// else try parsing as raw data-field (starting with codec)
	records, err := parseTeltonikaDataField(data)
	if err != nil {
		log.Printf("parse as data-field failed: %v", err)
		return
	}
	log.Printf("Parsed %d records from raw data-field", len(records))
	for i, r := range records {
		log.Printf("Record %d: ts=%s lat=%.7f lng=%.7f spd=%d sats=%d alt=%d angle=%d",
			i+1, r.Timestamp.UTC().Format(time.RFC3339Nano), r.Latitude, r.Longitude, r.Speed, r.Satellites, r.Altitude, r.Angle)
	}
}