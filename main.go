package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"time"
	"hash/crc32"

	_ "github.com/lib/pq"
	"github.com/joho/godotenv"
	"os"
)

// =======================
//      DATA STRUCTS
// =======================

type AVLData struct {
	Timestamp time.Time
	Latitude  float64
	Longitude float64
	Speed     float64
	IMEI      string
}

// PostgreSQL connection
var db *sql.DB

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using system env")
	}

	db, err = sql.Open("postgres", os.Getenv("POSTGRES_DSN"))
	if err != nil {
		log.Fatal("DB connection failed:", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("DB ping failed:", err)
	}

	log.Println("✅ PostgreSQL connected successfully")

	listener, err := net.Listen("tcp", ":5027")
	if err != nil {
		log.Fatal("TCP listen failed:", err)
	}
	defer listener.Close()

	log.Println("✅ TCP Server listening on :5027")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Connection accept error:", err)
			continue
		}

		log.Println("📡 New device connected:", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

// =======================
//   CONNECTION HANDLER
// =======================

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 0)

	tmp := make([]byte, 1024)
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			log.Println("Read error:", err)
			return
		}

		buffer = append(buffer, tmp[:n]...)

		for {
			if len(buffer) < 8 {
				break // need at least preamble+length
			}

			dataLength := binary.BigEndian.Uint32(buffer[4:8])
			frameLen := 8 + int(dataLength) + 4 // preamble+length + data + CRC

			if len(buffer) < frameLen {
				break // wait for more data
			}

			frame := buffer[:frameLen]
			dataField := frame[8 : 8+dataLength]
			crcExpected := binary.BigEndian.Uint32(frame[8+dataLength:frameLen])
			crcActual := crc32.ChecksumIEEE(dataField)

			if crcActual != crcExpected {
				log.Printf("❌ CRC mismatch! Expected: %08X, Actual: %08X\n", crcExpected, crcActual)
				buffer = buffer[frameLen:] // remove processed bytes
				continue
			}

			// Parse frame
			parseAVL(frame, conn)

			// Remove processed frame from buffer
			buffer = buffer[frameLen:]
		}
	}
}

// =======================
//   PARSE AVL FRAME
// =======================

func parseAVL(frame []byte, conn net.Conn) {
	dataLength := binary.BigEndian.Uint32(frame[4:8])
	data := frame[8 : 8+dataLength]

	if len(data) < 15 {
		log.Println("⚠️ Data too short to parse")
		return
	}

	imeiLen := int(data[0])
	if len(data) < 1+imeiLen {
		log.Println("⚠️ Data too short for IMEI")
		return
	}

	imei := string(data[1 : 1+imeiLen])
	log.Println("🔢 Parsed IMEI:", imei)

	// This example assumes 1 AVL record
	if len(data) < 15+imeiLen {
		log.Println("⚠️ Not enough AVL data")
		return
	}

	timestamp := int64(binary.BigEndian.Uint64(data[imeiLen+1 : imeiLen+9]))
	lat := float64(int32(binary.BigEndian.Uint32(data[imeiLen+9 : imeiLen+13]))) / 10000000
	lng := float64(int32(binary.BigEndian.Uint32(data[imeiLen+13 : imeiLen+17]))) / 10000000
	speed := float64(binary.BigEndian.Uint16(data[imeiLen+17 : imeiLen+19]))

	avl := AVLData{
		Timestamp: time.Unix(timestamp/1000, 0),
		Latitude:  lat,
		Longitude: lng,
		Speed:     speed,
		IMEI:      imei,
	}

	log.Printf("🛰️ Parsed AVL: %+v\n", avl)

	// Insert into PostgreSQL
	_, err := db.Exec(
		"INSERT INTO avl_data (imei, timestamp, latitude, longitude, speed) VALUES ($1,$2,$3,$4,$5)",
		avl.IMEI, avl.Timestamp, avl.Latitude, avl.Longitude, avl.Speed,
	)
	if err != nil {
		log.Println("DB insert error:", err)
	}

	// Send ACK (number of records received)
	records := data[1] // assuming single record count
	ack := make([]byte, 4)
	binary.BigEndian.PutUint32(ack, uint32(records))
	conn.Write(ack)
	log.Println("✅ ACK sent:", hex.EncodeToString(ack))
}
