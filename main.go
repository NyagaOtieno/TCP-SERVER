package main

import (
	"encoding/hex"
	"log"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":5027")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Println("Listening on :5027")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handle(conn)
	}
}

func handle(conn net.Conn) {
	defer conn.Close()
	log.Println("New connection:", conn.RemoteAddr())

	buf := make([]byte, 4096)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("Connection closed:", err)
			return
		}

		log.Printf("HEX (%d bytes): %s\n", n, hex.EncodeToString(buf[:n]))
	}
}
