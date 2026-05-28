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

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Generator

@dataclass
class Event:
    timestamp:   str
    user_id:     str
    user_city:   str
    product_id:  str
    product_cat: str
    seller_id:   str
    action_type: str 
    price:       float

VILLES      = ["Paris", "Lyon", "Marseille", "Bordeaux", "Lille", "Toulouse", "Nantes", "Strasbourg", "Nice", "Rennes", "Montpellier", "Grenoble", "Toulon", "Dijon", "Angers"]
CATEGORIES  = ["Véhicules", "Électronique", "Mode", "Maison", "Sport","Jardin", "Jeux vidéo", "Livres", "Beauté","Bricolage", "Musique", "Alimentation"]
ACTIONS     = ["AIME", "VOUT", "ACHAT"]

def generateur_evenements(n: int):
    for i in range(n):
        yield Event(
            timestamp   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            user_id     = f"usr_{random.randint(1000, 9999)}",
            user_city   = random.choice(VILLES),
            product_id  = f"prod_{random.randint(1000, 9999)}",
            product_cat = random.choice(CATEGORIES),
            seller_id   = f"sel_{random.randint(100, 999):04d}",
            action_type = random.choice(ACTIONS),
            price       = round(random.uniform(5.0, 2000.0), 2),
        )
