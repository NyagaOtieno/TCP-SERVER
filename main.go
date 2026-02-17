package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/joho/godotenv"
)

/*
Protocols supported:
1) Teltonika FMB920 (Codec8 / Codec8E): TCP AVL
   Frame: 00000000 + dataLen(4) + data(dataLen) + CRC(4)
   ACK: 4 bytes = number of accepted records

2) GT06 family:
   Frame: 0x7878 (or 0x7979) + length + protocol + info + serial(2) + crc(2) + 0D0A
   CRC is CRC-ITU/X25: init 0xFFFF, reflect in/out, xorout 0xFFFF (aka "~fcs" in their sample) 3
   ACK: 7878 05 <protocol> <serial(2)> <crc(2)> 0D0A (matches sample) 4

3) UniGuard S168:
   ASCII ending with '$'
   Example upstream LOCA / SYNC and downstream ACK formats 5 6
*/

type AVLData struct {
	Timestamp  time.Time
	Latitude   float64
	Longitude  float64
	Altitude   int
	Angle      int
	Satellites int
	Speed      int
	IOData     map[string]interface{} // generalized (Teltonika IO uses numeric IDs; UniGuard uses named fields)
	Source     string                 // "TELTONIKA" | "GT06" | "UNIGUARD"
}

type Device struct {
	ID   int    `json:"id"`
	IMEI string `json:"imei"`
}

var (
	tcpServerHost   string
	backendTrackURL string
	devicesListURL  string

	db                 *sql.DB
	httpClient         = &http.Client{Timeout: 15 * time.Second}
	wg                 sync.WaitGroup
	positionsHasIoData bool
	verbose            = true
)

func vLog(format string, a ...interface{}) {
	if verbose {
	log.Printf(format, a...)
	}
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	_ = godotenv.Load()

	tcpServerHost = getEnv("TCP_SERVER_HOST", "0.0.0.0:5027")
	backendTrackURL = getEnv("BACKEND_TRACK_URL", "https://mytrack-production.up.railway.app/api/track")
	devicesListURL = getEnv("DEVICES_LIST_URL", "https://mytrack-production.up.railway.app/api/devices/list")

	pgURL := getEnv("DATABASE_URL", "")
	if pgURL == "" {
	log.Fatal("❌ DATABASE_URL not set")
	}

	var err error
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
	log.Fatalf("❌ Failed to open PostgreSQL connection: %v", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
	log.Fatalf("❌ PostgreSQL ping failed: %v", err)
	}
	vLog("✅ PostgreSQL connected successfully")

	positionsHasIoData = checkPositionsHasIoData()
	if positionsHasIoData {
	vLog("ℹ️ positions.io_data column detected; will store IO JSON")
	} else {
	vLog("⚠️ positions.io_data column not detected; IO data will be omitted from DB inserts")
	}
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
	vLog("🚀 Starting TCP tracker server (Teltonika FMB920 + GT06 + UniGuard)...")

	listener, err := net.Listen("tcp", tcpServerHost)
	if err != nil {
	log.Fatalf("❌ Failed to start TCP server: %v", err)
	}
	defer listener.Close()

	vLog("✅ TCP Server listening on %s", tcpServerHost)

	for {
	conn, err := listener.Accept()
	if err != nil {
	vLog("⚠️ Accept error: %v", err)
	continue
	}
	wg.Add(1)
	go func(c net.Conn) {
	defer func() {
	if r := recover(); r != nil {
	vLog("🔥 Panic recovered: %v", r)
	}
	}()
	handleConnection(c)
	}(conn)
	}
}

// =====================================================
//                 CONNECTION HANDLING
// =====================================================

type Protocol string

const (
	PROTO_TELTONIKA Protocol = "TELTONIKA"
	PROTO_GT06      Protocol = "GT06"
	PROTO_UNIGUARD  Protocol = "UNIGUARD"
)

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	vLog("🔗 New connection from %s", remote)

	// Wrap with a buffered reader so we can Peek without consuming.
	br := bufio.NewReaderSize(conn, 64*1024)

	// Give devices enough time to speak first time (some open TCP then send later)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	proto, first, err := detectProtocol(br)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
	vLog("⚠️ Protocol detect failed from %s: %v", remote, err)
	return
	}

	vLog("🧭 Protocol from %s: %s", remote, proto)
	if len(first) > 0 {
	vLog("👀 First bytes HEX (%d): %s", len(first), hex.EncodeToString(first))
	vLog("👀 First bytes ASCII: %q", sanitizeASCII(first))
	}

	switch proto {
	case PROTO_TELTONIKA:
	handleTeltonika(conn, br)
	case PROTO_GT06:
	handleGT06(conn, br)
	case PROTO_UNIGUARD:
	handleUniGuard(conn, br)
	default:
	vLog("⚠️ Unknown protocol from %s", remote)
	}
}

func detectProtocol(br *bufio.Reader) (Protocol, []byte, error) {
	peek, err := br.Peek(32)
	if err != nil && !errors.Is(err, io.EOF) {
	// If nothing available yet, still return a meaningful error.
	return "", nil, err
	}
	if len(peek) == 0 {
	return "", nil, fmt.Errorf("no data")
	}

	// Teltonika IMEI handshake: 00 0F + 15 ASCII digits
	if len(peek) >= 2 && peek[0] == 0x00 && peek[1] == 0x0F {
	return PROTO_TELTONIKA, peek[:min(32, len(peek))], nil
	}

	// Teltonika AVL frame can also start with preamble 00 00 00 00
	if len(peek) >= 4 && bytes.Equal(peek[:4], []byte{0x00, 0x00, 0x00, 0x00}) {
	return PROTO_TELTONIKA, peek[:min(32, len(peek))], nil
	}

	// GT06 frame: 78 78 or 79 79
	if len(peek) >= 2 && ((peek[0] == 0x78 && peek[1] == 0x78) || (peek[0] == 0x79 && peek[1] == 0x79)) {
	return PROTO_GT06, peek[:min(32, len(peek))], nil
	}

	// UniGuard: ASCII starting with "S168" (spaces sometimes appear, but usually "S168#")
	trim := strings.TrimSpace(string(peek))
	if strings.HasPrefix(trim, "S168") {
	return PROTO_UNIGUARD, peek[:min(32, len(peek))], nil
	}

	// Fallback: if it's printable ASCII and contains "#", likely UniGuard
	if isMostlyASCII(peek) && bytes.Contains(peek, []byte("#")) && bytes.Contains(peek, []byte("S168")) {
	return PROTO_UNIGUARD, peek[:min(32, len(peek))], nil
	}

	return "", peek[:min(32, len(peek))], fmt.Errorf("unknown starting bytes")
}

func sanitizeASCII(b []byte) string {
	out := make([]byte, len(b))
	for i := range b {
	if b[i] >= 32 && b[i] <= 126 {
	out[i] = b[i]
	} else {
	out[i] = '.'
	}
	}
	return string(out)
}

func isMostlyASCII(b []byte) bool {
	if len(b) == 0 {
	return false
	}
	printable := 0
	for _, c := range b {
	if c == '\r' || c == '\n' || c == '\t' || (c >= 32 && c <= 126) {
	printable++
	}
	}
	return float64(printable)/float64(len(b)) > 0.8
}

// =====================================================
//                 DEVICE HANDLING (shared)
// =====================================================

func ensureDevice(imei string) (int, error) {
	imei = strings.TrimSpace(imei)
	if imei == "" {
	return 0, fmt.Errorf("empty imei")
	}

	var id int
	err := db.QueryRow("SELECT id FROM devices WHERE imei=$1", imei).Scan(&id)
	if err == nil {
	return id, nil
	}

	// Fetch devices list from your backend
	resp, err := httpClient.Get(devicesListURL)
	if err != nil {
	return 0, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var devices []Device
	if err := json.Unmarshal(body, &devices); err != nil {
	return 0, err
	}

	for _, d := range devices {
	if strings.TrimSpace(d.IMEI) == imei {
	_, _ = db.Exec("INSERT INTO devices(id, imei) VALUES($1,$2) ON CONFLICT DO NOTHING", d.ID, d.IMEI)
	return d.ID, nil
	}
	}

	return 0, fmt.Errorf("device IMEI %s not found", imei)
}

// =====================================================
//                 TELTONIKA (FMB920) HANDLER
// =====================================================

func handleTeltonika(conn net.Conn, br *bufio.Reader) {
	remote := conn.RemoteAddr().String()

	imei, err := teltonikaReadIMEI(conn, br)
	if err != nil {
	vLog("❌ Failed IMEI read (Teltonika) from %s: %v", remote, err)
	return
	}
	vLog("📡 Teltonika device connected: %s", imei)

	deviceID, err := ensureDevice(imei)
	if err != nil {
	vLog("❌ Device lookup failed for IMEI %s: %v", imei, err)
	return
	}

	// Read Teltonika AVL frames continuously
	for {
	// Some devices keep connection long; set a long read deadline.
	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

	frame, err := teltonikaReadFrame(br)
	if err != nil {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
	vLog("⏱ Teltonika read timeout for %s, closing", imei)
	} else if !errors.Is(err, io.EOF) {
	vLog("🔌 Teltonika read error for %s: %v", imei, err)
	}
	return
	}

	records, err := teltonikaParseAVL(frame)
	if err != nil {
	vLog("❌ Teltonika AVL parse error (imei=%s): %v", imei, err)
	// ACK 0 records to be safe
	_ = teltonikaSendAck(conn, 0)
	continue
	}

	valid := filterValid(records, imei)

	vLog("🔎 Teltonika parsed %d valid records (raw=%d)", len(valid), len(records))

	if err := storePositionsBatch(deviceID, imei, valid); err != nil {
	vLog("❌ DB insert failed (Teltonika): %v", err)
	}

	payload := buildBackendPayload(deviceID, imei, valid)
	_ = postPositionsToBackend(payload)

	// ACK = number of accepted records (4 bytes)
	_ = teltonikaSendAck(conn, len(valid))
	}
}

func teltonikaReadIMEI(conn net.Conn, br *bufio.Reader) (string, error) {
	// Teltonika IMEI packet: 2 bytes length + IMEI ASCII digits (15 chars)
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	// Read first 2 bytes
	h := make([]byte, 2)
	if _, err := io.ReadFull(br, h); err != nil {
	return "", err
	}

	imeiLen := int(binary.BigEndian.Uint16(h))
	if imeiLen <= 0 || imeiLen > 32 {
	return "", fmt.Errorf("invalid imei length: %d", imeiLen)
	}

	imeiBytes := make([]byte, imeiLen)
	if _, err := io.ReadFull(br, imeiBytes); err != nil {
	return "", err
	}

	imei := regexp.MustCompile(`\D`).ReplaceAllString(string(imeiBytes), "")
	if imei == "" {
	return "", fmt.Errorf("empty imei after sanitize")
	}

	// Teltonika expects 0x01 (1 byte) as accept
	_, _ = conn.Write([]byte{0x01})
	return imei, nil
}

func teltonikaReadFrame(br *bufio.Reader) ([]byte, error) {
	// Frame: preamble(4=0) + dataLen(4) + data(dataLen) + crc(4)
	header := make([]byte, 8)
	if _, err := io.ReadFull(br, header); err != nil {
	return nil, err
	}
	if !bytes.Equal(header[:4], []byte{0, 0, 0, 0}) {
	// Some devices may start directly with dataLen; try to recover by treating first 4 as dataLen.
	// Put back? We can't un-read, so fail loudly.
	return nil, fmt.Errorf("unexpected preamble: %s", hex.EncodeToString(header[:4]))
	}

	dataLen := int(binary.BigEndian.Uint32(header[4:8]))
	if dataLen <= 0 || dataLen > 8*1024*1024 {
	return nil, fmt.Errorf("invalid dataLen: %d", dataLen)
	}

	data := make([]byte, dataLen+4) // include CRC at end
	if _, err := io.ReadFull(br, data); err != nil {
	return nil, err
	}

	frame := data[:dataLen] // ignore CRC(4) for now
	return frame, nil
}

func teltonikaParseAVL(data []byte) ([]*AVLData, error) {
	// data starts with codec id (0x08 or 0x8E), then count, then records, then count2
	if len(data) < 3 {
	return nil, fmt.Errorf("frame too short")
	}
	codec := data[0]
	if codec != 0x08 && codec != 0x8E {
	return nil, fmt.Errorf("unsupported codec: 0x%X", codec)
	}
	r := bytes.NewReader(data)

	_ = readU8(r) // codec
	count := int(readU8(r))

	records := make([]*AVLData, 0, count)
	for i := 0; i < count; i++ {
	rec, err := teltonikaParseRecord(r, codec)
	if err != nil {
	// stop if stream desync
	return records, err
	}
	records = append(records, rec)
	}

	// trailing count2
	_ = readU8(r)
	return records, nil
}

func teltonikaParseRecord(r *bytes.Reader, codec byte) (*AVLData, error) {
	ts := int64(readU64(r))
	priority := readU8(r)
	_ = priority

	lon := int32(readU32(r))
	lat := int32(readU32(r))
	alt := int(readU16(r))
	angle := int(readU16(r))
	sats := int(readU8(r))
	speed := int(readU16(r))

	io := map[string]interface{}{}
	if codec == 0x08 {
	// Event IO ID (1), Total IO (1)
	eventIO := readU8(r)
	totalIO := readU8(r)
	io["event_io_id"] = eventIO
	io["total_io"] = totalIO

	// n1, n2, n4, n8 (all 1-byte)
	n1 := int(readU8(r))
	for i := 0; i < n1; i++ {
	id := readU8(r)
	val := readU8(r)
	io[fmt.Sprintf("io_%d", id)] = val
	}
	n2 := int(readU8(r))
	for i := 0; i < n2; i++ {
	id := readU8(r)
	val := readU16(r)
	io[fmt.Sprintf("io_%d", id)] = val
	}
	n4 := int(readU8(r))
	for i := 0; i < n4; i++ {
	id := readU8(r)
	val := readU32(r)
	io[fmt.Sprintf("io_%d", id)] = val
	}
	n8 := int(readU8(r))
	for i := 0; i < n8; i++ {
	id := readU8(r)
	val := readU64(r)
	io[fmt.Sprintf("io_%d", id)] = val
	}
	} else {
	// codec 0x8E: IDs & counts are 2 bytes
	eventIO := readU16(r)
	totalIO := readU16(r)
	io["event_io_id"] = eventIO
	io["total_io"] = totalIO

	n1 := int(readU16(r))
	for i := 0; i < n1; i++ {
	id := readU16(r)
	val := readU8(r)
	io[fmt.Sprintf("io_%d", id)] = val
	}
	n2 := int(readU16(r))
	for i := 0; i < n2; i++ {
	id := readU16(r)
	val := readU16(r)
	io[fmt.Sprintf("io_%d", id)] = val
	}
	n4 := int(readU16(r))
	for i := 0; i < n4; i++ {
	id := readU16(r)
	val := readU32(r)
	io[fmt.Sprintf("io_%d", id)] = val
	}
	n8 := int(readU16(r))
	for i := 0; i < n8; i++ {
	id := readU16(r)
	val := readU64(r)
	io[fmt.Sprintf("io_%d", id)] = val
	}
	}

	return &AVLData{
	Timestamp:  time.UnixMilli(ts).UTC(),
	Latitude:   float64(lat) / 1e7,
	Longitude:  float64(lon) / 1e7,
	Altitude:   alt,
	Angle:      angle,
	Satellites: sats,
	Speed:      speed,
	IOData:     io,
	Source:     "TELTONIKA",
	}, nil
}

func teltonikaSendAck(conn net.Conn, accepted int) error {
	ack := make([]byte, 4)
	binary.BigEndian.PutUint32(ack, uint32(accepted))
	_, err := conn.Write(ack)
	return err
}

// =====================================================
//                 GT06 HANDLER
// =====================================================

func handleGT06(conn net.Conn, br *bufio.Reader) {
	remote := conn.RemoteAddr().String()

	var imei string
	var deviceID int

	for {
	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
	pkt, err := gt06ReadPacket(br)
	if err != nil {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
	vLog("⏱ GT06 read timeout (%s), closing", remote)
	} else if !errors.Is(err, io.EOF) {
	vLog("🔌 GT06 read error (%s): %v", remote, err)
	}
	return
	}

	info, err := gt06ParsePacket(pkt)
	if err != nil {
	vLog("❌ GT06 parse error: %v", err)
	continue
	}

	// Login gives us IMEI
	if info.IMEI != "" && imei == "" {
	imei = info.IMEI
	vLog("📡 GT06 device connected: %s", imei)
	deviceID, err = ensureDevice(imei)
	if err != nil {
	vLog("❌ Device lookup failed for GT06 IMEI %s: %v", imei, err)
	return
	}
	}

	// ACK every packet (per protocol examples) 7
	_ = gt06SendAck(conn, info.Protocol, info.Serial)

	// If we got position data, store/post
	if info.Position != nil && imei != "" {
	valid := filterValid([]*AVLData{info.Position}, imei)
	if err := storePositionsBatch(deviceID, imei, valid); err != nil {
	vLog("❌ DB insert failed (GT06): %v", err)
	}
	payload := buildBackendPayload(deviceID, imei, valid)
	_ = postPositionsToBackend(payload)
	}
	}
}

type gt06Info struct {
	Protocol byte
	Serial   uint16
	IMEI     string
	Position *AVLData
}

func gt06ReadPacket(br *bufio.Reader) ([]byte, error) {
	// Start can be 7878 or 7979
	start, err := br.Peek(2)
	if err != nil {
	return nil, err
	}
	if !((start[0] == 0x78 && start[1] == 0x78) || (start[0] == 0x79 && start[1] == 0x79)) {
	// consume one byte and retry to resync
	_, _ = br.ReadByte()
	return nil, fmt.Errorf("gt06 resync: invalid start %s", hex.EncodeToString(start))
	}

	// For 7878 format:
	// totalLen = (PacketLen byte) + 5 (start2 + len1 + stop2)
	hdr := make([]byte, 3)
	if _, err := io.ReadFull(br, hdr); err != nil {
	return nil, err
	}
	pLen := int(hdr[2])

	total := pLen + 5
	pkt := make([]byte, total)
	copy(pkt[:3], hdr)
	if _, err := io.ReadFull(br, pkt[3:]); err != nil {
	return nil, err
	}

	// Expect stop 0D0A
	if len(pkt) >= 2 && !(pkt[len(pkt)-2] == 0x0D && pkt[len(pkt)-1] == 0x0A) {
	return pkt, fmt.Errorf("gt06 stop bits missing")
	}
	return pkt, nil
}

func gt06ParsePacket(pkt []byte) (*gt06Info, error) {
	if len(pkt) < 10 {
	return nil, fmt.Errorf("packet too short")
	}
	startOK := (pkt[0] == 0x78 && pkt[1] == 0x78) || (pkt[0] == 0x79 && pkt[1] == 0x79)
	if !startOK {
	return nil, fmt.Errorf("bad start bits")
	}

	pLen := int(pkt[2])
	if pLen+5 != len(pkt) {
	// tolerate mismatch
	}

	proto := pkt[3]

	// length region (for CRC): from Length byte to Serial inclusive:
	// [2 ... (len-2 stop) - 2 crc -1?]
	// Layout: start(2) len(1) proto(1) info(N) serial(2) crc(2) stop(2)
	// crc is calculated over: len + proto + info + serial
	crcStart := 2
	crcEnd := len(pkt) - 4 // exclude crc(2) + stop(2)
	crcCalc := crc16X25(pkt[crcStart:crcEnd])
	crcGot := binary.BigEndian.Uint16(pkt[len(pkt)-4 : len(pkt)-2])
	if crcCalc != crcGot {
	return nil, fmt.Errorf("crc mismatch: got=0x%04X calc=0x%04X", crcGot, crcCalc)
	}

	serial := binary.BigEndian.Uint16(pkt[len(pkt)-6 : len(pkt)-4])

	info := &gt06Info{Protocol: proto, Serial: serial}

	// info payload spans: proto+1 ... before serial
	infoPayload := pkt[4 : len(pkt)-6]

	switch proto {
	case 0x01: // login
	// login content: 8 bytes terminal id; IMEI is BCD-like (15 digits)
	if len(infoPayload) >= 8 {
	info.IMEI = gt06DecodeIMEI(infoPayload[:8])
	}
	case 0x12, 0x16: // GPS data packets
	pos, err := gt06ParseGPS(proto, infoPayload)
	if err == nil && pos != nil {
	pos.Source = "GT06"
	info.Position = pos
	}
	// else ignore
	case 0x13:
	// heartbeat/status packet — no position; still ACK 8
	default:
	// other protocols supported by ACK only
	}

	return info, nil
}

func gt06SendAck(conn net.Conn, protocol byte, serial uint16) error {
	// 78 78 05 <protocol> <serial(2)> <crc(2)> 0D 0A
	resp := make([]byte, 10)
	resp[0] = 0x78
	resp[1] = 0x78
	resp[2] = 0x05
	resp[3] = protocol
	binary.BigEndian.PutUint16(resp[4:6], serial)

	// CRC over: len + protocol + serial
	crc := crc16X25(resp[2:6])
	binary.BigEndian.PutUint16(resp[6:8], crc)
	resp[8] = 0x0D
	resp[9] = 0x0A

	_, err := conn.Write(resp)
	return err
}

func gt06DecodeIMEI(b []byte) string {
	// Usually 8 bytes BCD: each nibble is a digit; first nibble may be 0
	digits := make([]byte, 0, 16)
	for _, by := range b {
	hi := (by >> 4) & 0x0F
	lo := by & 0x0F
	digits = append(digits, '0'+hi, '0'+lo)
	}
	imei := strings.TrimLeft(string(digits), "0")
	if len(imei) > 15 {
	imei = imei[len(imei)-15:]
	}
	return imei
}

func gt06ParseGPS(proto byte, p []byte) (*AVLData, error) {
	// Common GT06 combined GPS+LBS contains:
	// DateTime(6) + GPSInfo(1) + Lat(4) + Lon(4) + Speed(1) + CourseStatus(2) + ...
	// Example layout in document 9
	if len(p) < 6+1+4+4+1+2 {
	return nil, fmt.Errorf("gps payload too short")
	}

	yy := int(p[0])
	mm := time.Month(p[1])
	dd := int(p[2])
	hh := int(p[3])
	mi := int(p[4])
	ss := int(p[5])
	ts := time.Date(2000+yy, mm, dd, hh, mi, ss, 0, time.UTC)

	gpsInfo := p[6]
	sats := int(gpsInfo & 0x0F)

	latRaw := binary.BigEndian.Uint32(p[7:11])
	lonRaw := binary.BigEndian.Uint32(p[11:15])

	lat := float64(latRaw) / 30000.0 / 60.0
	lon := float64(lonRaw) / 30000.0 / 60.0

	speed := int(p[15])
	courseStatus := binary.BigEndian.Uint16(p[16:18])

	// Sign bits (common GT06):
	// bit10: GPS positioned (1=valid)
	// bit11: South (1=south)
	// bit12: West (1=west)
	// course = low 10 bits
	if (courseStatus & (1 << 11)) != 0 {
	lat = -lat
	}
	if (courseStatus & (1 << 12)) != 0 {
	lon = -lon
	}
	angle := int(courseStatus & 0x03FF)

	return &AVLData{
	Timestamp:  ts,
	Latitude:   lat,
	Longitude:  lon,
	Altitude:   0,
	Angle:      angle,
	Satellites: sats,
	Speed:      speed,
	IOData: map[string]interface{}{
	"proto": fmt.Sprintf("0x%02X", proto),
	},
	Source: "GT06",
	}, nil
}

// CRC-ITU/X25 as per their sample implementation: init 0xFFFF, table-based, return ~fcs 10
func crc16X25(data []byte) uint16 {
	var fcs uint16 = 0xFFFF
	for _, b := range data {
	fcs = (fcs >> 8) ^ crctab16[(fcs^uint16(b))&0xFF]
	}
	return ^fcs
}

var crctab16 = [256]uint16{
	0x0000, 0x1189, 0x2312, 0x329B, 0x4624, 0x57AD, 0x6536, 0x74BF,
	0x8C48, 0x9DC1, 0xAF5A, 0xBED3, 0xCA6C, 0xDBE5, 0xE97E, 0xF8F7,
	0x1081, 0x0108, 0x3393, 0x221A, 0x56A5, 0x472C, 0x75B7, 0x643E,
	0x9CC9, 0x8D40, 0xBFDB, 0xAE52, 0xDAED, 0xCB64, 0xF9FF, 0xE876,
	0x2102, 0x308B, 0x0210, 0x1399, 0x6726, 0x76AF, 0x4434, 0x55BD,
	0xAD4A, 0xBCC3, 0x8E58, 0x9FD1, 0xEB6E, 0xFAE7, 0xC87C, 0xD9F5,
	0x3183, 0x200A, 0x1291, 0x0318, 0x77A7, 0x662E, 0x54B5, 0x453C,
	0xBDCB, 0xAC42, 0x9ED9, 0x8F50, 0xFBEF, 0xEA66, 0xD8FD, 0xC974,
	0x4204, 0x538D, 0x6116, 0x709F, 0x0420, 0x15A9, 0x2732, 0x36BB,
	0xCE4C, 0xDFC5, 0xED5E, 0xFCD7, 0x8868, 0x99E1, 0xAB7A, 0xBAF3,
	0x5285, 0x430C, 0x7197, 0x601E, 0x14A1, 0x0528, 0x37B3, 0x263A,
	0xDECD, 0xCF44, 0xFDDF, 0xEC56, 0x98E9, 0x8960, 0xBBFB, 0xAA72,
	0x6306, 0x728F, 0x4014, 0x519D, 0x2522, 0x34AB, 0x0630, 0x17B9,
	0xEF4E, 0xFEC7, 0xCC5C, 0xDDD5, 0xA96A, 0xB8E3, 0x8A78, 0x9BF1,
	0x7387, 0x620E, 0x5095, 0x411C, 0x35A3, 0x242A, 0x16B1, 0x0738,
	0xFFCF, 0xEE46, 0xDCDD, 0xCD54, 0xB9EB, 0xA862, 0x9AF9, 0x8B70,
	0x8408, 0x9581, 0xA71A, 0xB693, 0xC22C, 0xD3A5, 0xE13E, 0xF0B7,
	0x0840, 0x19C9, 0x2B52, 0x3ADB, 0x4E64, 0x5FED, 0x6D76, 0x7CFF,
	0x9489, 0x8500, 0xB79B, 0xA612, 0xD2AD, 0xC324, 0xF1BF, 0xE036,
	0x18C1, 0x0948, 0x3BD3, 0x2A5A, 0x5EE5, 0x4F6C, 0x7DF7, 0x6C7E,
	0xA50A, 0xB483, 0x8618, 0x9791, 0xE32E, 0xF2A7, 0xC03C, 0xD1B5,
	0x2942, 0x38CB, 0x0A50, 0x1BD9, 0x6F66, 0x7EEF, 0x4C74, 0x5DFD,
	0xB58B, 0xA402, 0x9699, 0x8710, 0xF3AF, 0xE226, 0xD0BD, 0xC134,
	0x39C3, 0x284A, 0x1AD1, 0x0B58, 0x7FE7, 0x6E6E, 0x5CF5, 0x4D7C,
	0xC60C, 0xD785, 0xE51E, 0xF497, 0x8028, 0x91A1, 0xA33A, 0xB2B3,
	0x4A44, 0x5BCD, 0x6956, 0x78DF, 0x0C60, 0x1DE9, 0x2F72, 0x3EFB,
	0xD68D, 0xC704, 0xF59F, 0xE416, 0x90A9, 0x8120, 0xB3BB, 0xA232,
	0x5AC5, 0x4B4C, 0x79D7, 0x685E, 0x1CE1, 0x0D68, 0x3FF3, 0x2E7A,
	0xE70E, 0xF687, 0xC41C, 0xD595, 0xA12A, 0xB0A3, 0x8238, 0x93B1,
	0x6B46, 0x7ACF, 0x4854, 0x59DD, 0x2D62, 0x3CEB, 0x0E70, 0x1FF9,
	0xF78F, 0xE606, 0xD49D, 0xC514, 0xB1AB, 0xA022, 0x92B9, 0x8330,
	0x7BC7, 0x6A4E, 0x58D5, 0x495C, 0x3DE3, 0x2C6A, 0x1EF1, 0x0F78,
}

// =====================================================
//                 UNIGUARD S168 HANDLER
// =====================================================

func handleUniGuard(conn net.Conn, br *bufio.Reader) {
	remote := conn.RemoteAddr().String()

	var imei string
	var deviceID int

	for {
	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

	line, err := uniReadFrame(br)
	if err != nil {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
	vLog("⏱ UniGuard read timeout (%s), closing", remote)
	} else if !errors.Is(err, io.EOF) {
	vLog("🔌 UniGuard read error (%s): %v", remote, err)
	}
	return
	}

	msg := strings.TrimSpace(line)
	if msg == "" {
	continue
	}

	parsed, err := uniParse(msg)
	if err != nil {
	vLog("❌ UniGuard parse error: %v (msg=%q)", err, msg)
	continue
	}

	if parsed.IMEI != "" && imei == "" {
	imei = parsed.IMEI
	vLog("📡 UniGuard device connected: %s", imei)
	deviceID, err = ensureDevice(imei)
	if err != nil {
	vLog("❌ Device lookup failed for UniGuard IMEI %s: %v", imei, err)
	return
	}
	}

	// ACK rules:
	// - LOCA: downstream ACK ^ LOCA 11
	// - SYNC: downstream ACK ^ SYNC, utc time (yyyymmddhhmmss) 12
	if parsed.Type == "LOCA" {
	_ = uniSendAck(conn, parsed.Serial, "LOCA", "")
	} else if parsed.Type == "SYNC" {
	nowUTC := time.Now().UTC().Format("20060102150405")
	_ = uniSendAck(conn, parsed.Serial, "SYNC", nowUTC)
	}

	// store/post if has position
	if parsed.Position != nil && imei != "" {
	valid := filterValid([]*AVLData{parsed.Position}, imei)
	if err := storePositionsBatch(deviceID, imei, valid); err != nil {
	vLog("❌ DB insert failed (UniGuard): %v", err)
	}
	payload := buildBackendPayload(deviceID, imei, valid)
	_ = postPositionsToBackend(payload)
	}
	}
}

func uniReadFrame(br *bufio.Reader) (string, error) {
	// UniGuard frames end with '$' 13
	var buf bytes.Buffer
	for {
	b, err := br.ReadByte()
	if err != nil {
	return "", err
	}
	buf.WriteByte(b)
	if b == '$' {
	return buf.String(), nil
	}
	// safety guard
	if buf.Len() > 64*1024 {
	return "", fmt.Errorf("uniguard frame too large")
	}
	}
}

type uniMsg struct {
	IMEI     string
	Serial   string
	Length   string
	Type     string // LOCA or SYNC etc
	Position *AVLData
}

func uniParse(msg string) (*uniMsg, error) {
	// Format like:
	// S168#<imei>#<serial>#<len>#LOCA: ... $ 14
	// or heartbeat SYNC... 15
	msg = strings.TrimSpace(msg)
	msg = strings.TrimSuffix(msg, "$")

	// Allow both "S168 # ..." and "S168#..."
	msg = strings.ReplaceAll(msg, " ", "")
	if !strings.HasPrefix(msg, "S168#") {
	return nil, fmt.Errorf("not S168")
	}

	parts := strings.Split(msg, "#")
	if len(parts) < 5 {
	return nil, fmt.Errorf("invalid parts count")
	}

	imei := parts[1]
	serial := parts[2]
	length := parts[3]
	body := parts[4] // starts with LOCA:... or SYNC...

	u := &uniMsg{IMEI: imei, Serial: serial, Length: length}

	if strings.HasPrefix(body, "LOCA") {
	u.Type = "LOCA"
	pos := uniParseLoca(body)
	if pos != nil {
	pos.Source = "UNIGUARD"
	u.Position = pos
	}
	return u, nil
	}

	if strings.HasPrefix(body, "SYNC") {
	u.Type = "SYNC"
	return u, nil
	}

	// Other commands, still treat as connected
	u.Type = "OTHER"
	return u, nil
}

func uniParseLoca(body string) *AVLData {
	// Example has GPS info in "GDATA: A, 12,160412154800,22.564025,113.242329,5.5,152,900;" 16
	// It also defines lat/lon/speed/heading/alt meaning 17

	// Normalize separators
	b := strings.ReplaceAll(body, " ", "")

	// Find "GDATA:"
	idx := strings.Index(b, "GDATA:")
	if idx == -1 {
	return nil
	}
	rest := b[idx+len("GDATA:"):]
	// up to ';'
	semi := strings.Index(rest, ";")
	if semi != -1 {
	rest = rest[:semi]
	}

	// Split by commas
	fields := strings.Split(rest, ",")
	// common: [A][12][yyyymmddhhmmss][lat][lon][speed][heading][alt]
	if len(fields) < 8 {
	return nil
	}

	tsStr := fields[2]
	ts, _ := time.ParseInLocation("060102150405", tsStr, time.UTC) // some devices send yyMMddHHmmss
	if ts.IsZero() {
	// some send 20160412154800 (yyyymmddhhmmss)
	ts2, _ := time.ParseInLocation("20060102150405", tsStr, time.UTC)
	if !ts2.IsZero() {
	ts = ts2
	} else {
	ts = time.Now().UTC()
	}
	}

	lat, _ := strconv.ParseFloat(fields[3], 64)
	lon, _ := strconv.ParseFloat(fields[4], 64)
	spd, _ := strconv.ParseFloat(fields[5], 64)
	head, _ := strconv.ParseFloat(fields[6], 64)
	alt, _ := strconv.ParseFloat(fields[7], 64)

	return &AVLData{
	Timestamp:  ts.UTC(),
	Latitude:   lat,
	Longitude:  lon,
	Altitude:   int(alt),
	Angle:      int(head),
	Satellites: 0,
	Speed:      int(spd),
	IOData: map[string]interface{}{
	"raw_gdata": rest,
	},
	Source: "UNIGUARD",
	}
}

func uniSendAck(conn net.Conn, serial string, kind string, extra string) error {
	// Downstream formats:
	// ACK ^ LOCA 18
	// ACK ^ SYNC, utc time (yyyymmddhhmmss) 19

	imeiZeros := "000000000000000"
	body := ""
	if kind == "LOCA" {
	body = "ACK^LOCA"
	} else if kind == "SYNC" {
	body = "ACK^SYNC," + extra
	} else {
	body = "ACK^" + kind
	}

	// length field in their examples is "xxxx" = actual length; some devices ignore.
	// We'll compute the body length as hex-ish width 4 (decimal also seen). Keep simple decimal width 4.
	length := fmt.Sprintf("%04d", len(body))

	resp := fmt.Sprintf("S168#%s#%s#%s#%s$", imeiZeros, serial, length, body)
	_, err := conn.Write([]byte(resp))
	return err
}

// Optional helper if you later enable AUTH for SYNC
func uniAuth(imei, keyString string) string {
	sum := md5.Sum([]byte(imei + keyString))
	return fmt.Sprintf("%x", sum)
}

// =====================================================
//                 COMMON: VALIDATION + DB + BACKEND
// =====================================================

func filterValid(records []*AVLData, imei string) []*AVLData {
	valid := make([]*AVLData, 0, len(records))
	for _, r := range records {
	if r == nil {
	continue
	}
	if r.Latitude == 0 || r.Longitude == 0 {
	vLog("⚠️ Skipping zero coordinates (imei=%s): LAT=%.7f LNG=%.7f SAT=%d", imei, r.Latitude, r.Longitude, r.Satellites)
	continue
	}
	if r.Latitude < -90 || r.Latitude > 90 || r.Longitude < -180 || r.Longitude > 180 {
	vLog("⚠️ Skipping out-of-range coordinates (imei=%s): LAT=%.7f LNG=%.7f", imei, r.Latitude, r.Longitude)
	continue
	}
	valid = append(valid, r)
	}
	return valid
}

func buildBackendPayload(deviceID int, imei string, recs []*AVLData) []map[string]interface{} {
	payload := make([]map[string]interface{}, 0, len(recs))
	for _, r := range recs {
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
	"source":     r.Source,
	})
	}
	return payload
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
	deviceID, r.Latitude, r.Longitude, r.Speed, r.Angle,
	r.Altitude, r.Satellites, r.Timestamp.UTC(), imei, ioJSON,
	)
	} else {
	_, err = stmt.Exec(
	deviceID, r.Latitude, r.Longitude, r.Speed, r.Angle,
	r.Altitude, r.Satellites, r.Timestamp.UTC(), imei,
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

	body, _ := io.ReadAll(resp.Body)
	vLog("📬 Backend response (%d): %.200s", resp.StatusCode, body)
	return nil
}

// =====================================================
//                 BINARY HELPERS
// =====================================================

func readU8(r *bytes.Reader) byte {
	b, _ := r.ReadByte()
	return b
}
func readU16(r *bytes.Reader) uint16 {
	var v uint16
	_ = binary.Read(r, binary.BigEndian, &v)
	return v
}
func readU32(r *bytes.Reader) uint32 {
	var v uint32
	_ = binary.Read(r, binary.BigEndian, &v)
	return v
}
func readU64(r *bytes.Reader) uint64 {
	var v uint64
	_ = binary.Read(r, binary.BigEndian, &v)
	return v
}

func min(a, b int) int {
	if a < b {
	return a
	}
	return b
}

func getEnv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
	return def
	}
	return val
}
