import socket

client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_ip = "10.117.159.45"
server_port = 5000

while True:
    message = input("Client: ")

    client.sendto(message.encode(), (server_ip, server_port))

    if message.lower() == "exit":
        break

    data, server_address = client.recvfrom(1024)

    response = data.decode()
    print("Server:", response)

    if response.lower() == "exit":
        break

client.close()