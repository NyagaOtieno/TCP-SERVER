package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

// Device represents a device on the backend
type Device struct {
	ID   int    `json:"id"`
	IMEI string `json:"imei"`
}

// Position struct for sending positions
type Position struct {
	DeviceID  int     `json:"device_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Speed     int     `json:"speed"`
	Altitude  int     `json:"altitude"`
	Angle     int     `json:"angle"`
	Timestamp string  `json:"timestamp"`
}

const (
	tcpServerHost     = "127.0.0.1:5027"
	devicesPostURL    = "https://mytrack-production.up.railway.app/api/devices/create"
	devicesListURL    = "https://mytrack-production.up.railway.app/api/devices/list"
	positionsPostURL  = "https://mytrack-production.up.railway.app/api/track"
	deviceLatestURL   = "https://mytrack-production.up.railway.app/api/devices/latest?imei="
)

var imeis = []string{
	"359016073166828",
	"359016073166829",
	"359016073166830",
}

func main() {
	for {
		if err := runForwarder(); err != nil {
			log.Println("⚠️ Error:", err)
			log.Println("🔄 Reconnecting in 5s...")
			time.Sleep(5 * time.Second)
		}
	}
}

func runForwarder() error {
	// Step 1: ensure devices exist on backend
	devicesMap, err := ensureDevices(imeis)
	if err != nil {
		return err
	}

	// Step 2: connect to TCP server
	conn, err := net.Dial("tcp", tcpServerHost)
	if err != nil {
		return fmt.Errorf("failed to connect to TCP server: %v", err)
	}
	defer conn.Close()
	log.Println("✅ Connected to TCP server")

	for _, imei := range imeis {
		// Send IMEI
		conn.Write(buildImeiPacket(imei))

		// Read ACK
		ack := make([]byte, 1)
		if _, err := conn.Read(ack); err != nil {
			return fmt.Errorf("failed to read IMEI ACK: %v", err)
		}
		if ack[0] != 0x01 {
			return fmt.Errorf("IMEI not acknowledged: %s", imei)
		}
		log.Printf("📡 IMEI acknowledged for %s", imei)

		// Send AVL packet
		lat, lon := randomLatLon() // optionally random for simulation
		alt, angle, sat, speed := 12, 180, 8, 45
		conn.Write(buildAvlPacket(lat, lon, alt, angle, sat, speed))

		// Read server ACK
		resp := make([]byte, 4)
		if _, err := conn.Read(resp); err != nil {
			return fmt.Errorf("failed to read AVL ACK: %v", err)
		}
		log.Printf("📤 Server response ACK for %s: %x", imei, resp)

		// Step 3: Send position to backend
		pos := Position{
			DeviceID:  devicesMap[imei],
			Latitude:  lat,
			Longitude: lon,
			Altitude:  alt,
			Angle:     angle,
			Speed:     speed,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}
		if err := postPosition(pos); err != nil {
			log.Printf("❌ Failed to send position for %s: %v", imei, err)
		} else {
			log.Printf("📥 Position sent to backend for %s", imei)
		}

		// Optional: GET latest position from backend
		latest, err := getLatestPosition(imei)
		if err != nil {
			log.Printf("❌ Failed to fetch latest position for %s: %v", imei, err)
		} else {
			log.Printf("📍 Latest backend position for %s: %+v", imei, latest)
		}
	}

	return nil
}

// ensureDevices creates devices if missing, returns map[imei]deviceID
func ensureDevices(imeis []string) (map[string]int, error) {
	devicesMap := make(map[string]int)

	// GET existing devices
	resp, err := http.Get(devicesListURL)
	if err != nil {
		return nil, fmt.Errorf("failed to GET devices list: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var existing []Device
	if err := json.Unmarshal(body, &existing); err != nil {
		return nil, fmt.Errorf("failed to parse devices list: %v", err)
	}
	for _, d := range existing {
		devicesMap[d.IMEI] = d.ID
	}

	// Create missing devices
	for _, imei := range imeis {
		if _, exists := devicesMap[imei]; exists {
			log.Printf("📥 Device %s already exists (duplicate key ignored)", imei)
			continue
		}

		data, _ := json.Marshal(map[string]string{"imei": imei})
		req, _ := http.NewRequest("POST", devicesPostURL, bytes.NewBuffer(data))
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("❌ Failed to create device %s: %v", imei, err)
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var newDevice Device
		if err := json.Unmarshal(respBody, &newDevice); err != nil {
			log.Printf("❌ Failed to parse device creation response for %s: %s", imei, string(respBody))
			continue
		}
		devicesMap[imei] = newDevice.ID
		log.Printf("📥 Device %s created on backend", imei)
	}

	return devicesMap, nil
}

func buildImeiPacket(imei string) []byte {
	buf := make([]byte, 1+len(imei))
	buf[0] = byte(len(imei))
	copy(buf[1:], []byte(imei))
	return buf
}

func buildAvlPacket(lat, lon float64, alt, angle, sat, speed int) []byte {
	buf := make([]byte, 45)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], 0)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], 0)
	offset += 4
	buf[offset] = 0x08
	offset++
	buf[offset] = 0x01
	offset++

	binary.BigEndian.PutUint64(buf[offset:], uint64(time.Now().UnixNano()/1e6))
	offset += 8
	buf[offset] = 0
	offset++

	binary.BigEndian.PutUint32(buf[offset:], uint32(int32(lon*1e7)))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], uint32(int32(lat*1e7)))
	offset += 4

	binary.BigEndian.PutUint16(buf[offset:], uint16(alt))
	offset += 2
	binary.BigEndian.PutUint16(buf[offset:], uint16(angle))
	offset += 2
	buf[offset] = byte(sat)
	offset++
	binary.BigEndian.PutUint16(buf[offset:], uint16(speed))
	offset++
	buf[offset] = 0
	offset++
	buf[offset] = 0
	offset++

	for i := offset; i < len(buf); i++ {
		buf[i] = 0
	}

	return buf
}

func postPosition(pos Position) error {
	data, _ := json.Marshal(pos)
	req, _ := http.NewRequest("POST", positionsPostURL, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP POST failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	log.Printf("📬 HTTP response: %s", string(body))
	return nil
}

// getLatestPosition fetches latest position for a device by IMEI
func getLatestPosition(imei string) (*Position, error) {
	resp, err := http.Get(deviceLatestURL + imei)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var latest Position
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &latest); err != nil {
		return nil, fmt.Errorf("failed to parse latest position: %s", string(body))
	}
	return &latest, nil
}

// Optional: simulate random positions (slight movement)
func randomLatLon() (float64, float64) {
	baseLat, baseLon := -1.30345, 36.81234
	return baseLat + (0.001 * float64(time.Now().UnixNano()%10)), baseLon + (0.001 * float64(time.Now().UnixNano()%10))
}
