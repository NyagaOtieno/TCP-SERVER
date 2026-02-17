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

type AVLData struct {
	Timestamp  time.Time
	Latitude   float64
	Longitude  float64
	Altitude   int
	Angle      int
	Satellites int
	Speed      int
	IOData     map[string]interface{}
	Source     string
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

	deviceCache      = make(map[string]int)
	deviceCacheMutex sync.RWMutex
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
	backendTrackURL = getEnv("BACKEND_TRACK_URL", "")
	devicesListURL = getEnv("DEVICES_LIST_URL", "")

	pgURL := getEnv("DATABASE_URL", "")
	if pgURL == "" {
		log.Fatal("DATABASE_URL not set")
	}

	var err error
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		log.Fatal(err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}

	vLog("✅ PostgreSQL connected successfully")
	positionsHasIoData = checkPositionsHasIoData()
}

func checkPositionsHasIoData() bool {
	var col string
	err := db.QueryRow(`
		SELECT column_name FROM information_schema.columns
		WHERE table_name='positions' AND column_name='io_data'
	`).Scan(&col)
	return err == nil
}

func main() {
	vLog("🚀 Starting TCP tracker server...")
	listener, err := net.Listen("tcp", tcpServerHost)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	vLog("✅ TCP Server listening on %s", tcpServerHost)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		wg.Add(1)
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	vLog("🔗 New connection from %s", remote)

	br := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	proto, _, err := detectProtocol(br)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		vLog("Protocol detect failed: %v", err)
		return
	}

	switch proto {
	case "TELTONIKA":
		handleTeltonika(conn, br)
	case "GT06":
		handleGT06(conn, br)
	case "UNIGUARD":
		handleUniGuard(conn, br)
	}
}

func detectProtocol(br *bufio.Reader) (string, []byte, error) {
	peek, err := br.Peek(32)
	if err != nil {
		return "", nil, err
	}
	if len(peek) == 0 {
		return "", nil, fmt.Errorf("no data")
	}

	if len(peek) >= 2 && peek[0] == 0x00 && peek[1] == 0x0F {
		return "TELTONIKA", peek, nil
	}
	if len(peek) >= 2 && peek[0] == 0x78 && peek[1] == 0x78 {
		return "GT06", peek, nil
	}
	if strings.HasPrefix(string(peek), "S168") {
		return "UNIGUARD", peek, nil
	}
	return "", peek, fmt.Errorf("unknown protocol")
}
