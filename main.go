package main

import (
	"encoding/hex"
	"io"
	"log"
	"net"
)


func handle(conn net.Conn) {
	defer conn.Close()
	log.Println("New connection:", conn.RemoteAddr())

	buf := make([]byte, 4096)

	// --- 1) Read IMEI/login frame ---
	n, err := conn.Read(buf)
	if err != nil {
		log.Println("Read error:", err)
		return
	}
	raw := buf[:n]
	log.Printf("HEX (%d bytes): %s\n", n, hex.EncodeToString(raw))

	if n < 2 {
		log.Println("Too short for IMEI length")
		return
	}

	imeiLen := int(raw[0])<<8 | int(raw[1]) // 000f -> 15
	if n < 2+imeiLen {
		log.Printf("IMEI length says %d but only got %d bytes", imeiLen, n)
		return
	}

	imei := string(raw[2 : 2+imeiLen])
	log.Println("IMEI:", imei)

	// --- 2) ACK IMEI (most devices need this) ---
	_, err = conn.Write([]byte{0x01})
	if err != nil {
		log.Println("Failed to ACK IMEI:", err)
		return
	}
	log.Println("Sent IMEI ACK: 01")

	// --- 3) Now keep reading actual data packets ---
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Println("Connection closed: EOF")
			} else {
				log.Println("Read error:", err)
			}
			return
		}
		if n > 0 {
			log.Printf("DATA HEX (%d bytes): %s\n", n, hex.EncodeToString(buf[:n]))
		}
	}
}

func main() {
	ln, err := net.Listen("tcp", ":5027")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	log.Println("Listening on :5027")

	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go handle(c)
	}
}
