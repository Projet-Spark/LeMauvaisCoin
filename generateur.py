import socket
import json
import struct

    

def launch_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 9999))
    s.listen()
    print("Serveur en écoute...")

    while True:
        (clientsocket, address) = s.accept()
        with clientsocket:
            print(f"connecté par {address}")
            while True:
                event = generate_event()
                json_event = json.dumps(event.__dict__).encode("utf-8")
                header = struct.pack(">I", len(json_event))
                clientsocket.sendall(header + json_event)



if __name__ == "__main__":
    launch_server()
