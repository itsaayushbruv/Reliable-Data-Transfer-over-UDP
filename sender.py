import socket

server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server.bind(("0.0.0.0", 5000))

print("Server waiting...")

while True:
    data, client_address = server.recvfrom(1024)
    message = data.decode()
    print("Client:", message)

    if message.lower() == "exit":
        break

    response = input("Server: ")

    server.sendto(response.encode(), client_address)

    if response.lower() == "exit":
        break

server.close()