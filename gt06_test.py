import socket
import binascii
import struct

HOST = "switchback.proxy.rlwy.net"
PORT = 15376

def crc_x25(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0x8408
            else:
                crc >>= 1
    return (~crc) & 0xFFFF

# Example IMEI (must be 15 digits)
imei = "123456789012345"

# Convert IMEI to BCD
imei_bcd = bytearray()
for i in range(0, len(imei), 2):
    imei_bcd.append(int(imei[i:i+2]))

protocol_number = b"\x01"
serial = b"\x00\x01"

packet_body = protocol_number + imei_bcd + serial

length = len(packet_body)
packet = b"\x78\x78" + struct.pack("B", length) + packet_body

crc = crc_x25(packet[2:])
packet += struct.pack(">H", crc) + b"\x0D\x0A"

print("Sending GT06 packet:", binascii.hexlify(packet))

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(packet)
    response = s.recv(1024)
    print("Server response:", binascii.hexlify(response))
