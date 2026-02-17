import socket

HOST = "switchback.proxy.rlwy.net"
PORT = 15376

packet = (
    "S168#123456789012345#0001#0077#"
    "LOCA: G;"
    "GDATA: A,12,160412154800,22.564025,113.242329,5.5,152,900;"
    "STATUS: 89,98$"
)

print("Sending UniGuard packet:")
print(packet)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(packet.encode())
    response = s.recv(1024)
    print("Server response:", response)
