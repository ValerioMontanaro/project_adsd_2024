# node.py

import time
import threading
import requests
from flask import Flask, request, jsonify


class Node:
    """Questo è il costruttore della classe che inizializza un nodo con un ID, un indirizzo e una lista di nodi vicini.
    Inizializza anche un dizionario vuoto per i dati e una tabella di heartbeat."""

    def __init__(self, node_id, address, neighbors):
        self.node_id = node_id  # identificatore univioco del nodo
        self.address = address  # indirizzo del nodo (IP:porta)
        self.data = {}  # dizionario che contiene i dati del nodo
        self.neighbors = neighbors  # lista dei vicini del nodo
        self.heartbeat_table = {neighbor: time.time() for neighbor in neighbors}  # dizionario che contiene l'ultimo heartbeat ricevuto da ciascun vicino
        self.lock = threading.Lock()  # Un oggetto di lock per garantire che l'accesso ai dati condivisi sia thread-safe

    def store(self, key, value):
        with self.lock:
            self.data[key] = value

    def retrieve(self, key):
        with self.lock:
            return self.data.get(key)

    def send_heartbeat(self):
        """Invia heartbeat ai nodi vicini periodicamente."""
        while True:
            time.sleep(1)
            current_time = time.time()
            for neighbor in self.neighbors:
                try:
                    requests.post(f"http://{neighbor}/heartbeat", json={"node_id": self.node_id, "timestamp": current_time})
                except requests.exceptions.RequestException as e:
                    print(f"Failed to send heartbeat to {neighbor}: {e}")

    def receive_heartbeat(self, node_id, timestamp):
        with self.lock:
            self.heartbeat_table[node_id] = timestamp

    def check_heartbeats(self):
        while True:
            time.sleep(5)
            current_time = time.time()
            with self.lock:
                for node_id, last_heartbeat in self.heartbeat_table.items():
                    if current_time - last_heartbeat > 20:
                        print(f"Node {node_id} is considered failed")

    def start(self):
        # Avvio dei thread per l'invio e il controllo degli heartbeat
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        threading.Thread(target=self.check_heartbeats, daemon=True).start()

        # Creazione dell'app Flask per gestire le richieste di heartbeat
        app = Flask(__name__)

        @app.route('/heartbeat', methods=['POST'])
        def heartbeat():
            # Ricezione dei dati di heartbeat inviati dagli altri nodi
            data = request.get_json()
            node_id = data['node_id']
            timestamp = data['timestamp']
            # Aggiornamento della tabella di heartbeat
            self.receive_heartbeat(node_id, timestamp)
            # Risposta di conferma
            return jsonify({"status": "ok"})

        @app.route('/put', methods=['PUT'])
        def put_data():
            data = request.get_json()
            key = data['key']
            value = data['value']
            self.store(key, value)
            return jsonify({"status": "ok"})

        @app.route('/get', methods=['GET'])
        def get_data():
            key = request.args.get('key')
            value = self.retrieve(key)
            if value is not None:
                return jsonify({"key": key, "value": value})
            else:
                return jsonify({"error": "Key not found"}), 404

        # Avvio del server Flask con l'indirizzo e la porta del nodo
        app.run(host=self.address.split(':')[0], port=int(self.address.split(':')[1]))


# Example usage
if __name__ == '__main__':
    node1 = Node(node_id=1, address="localhost:5000", neighbors=["localhost:5001"])
    node2 = Node(node_id=2, address="localhost:5001", neighbors=["localhost:5000"])

    threading.Thread(target=node1.start, daemon=True).start()
    threading.Thread(target=node2.start, daemon=True).start()

    time.sleep(3)  # Attendere che i server Flask siano avviati

    # Eseguire test di base
    response = requests.put("http://localhost:5000/put", json={"key": "name", "value": "Alice"})
    print(f"PUT Response (node1): {response.json()}")

    response = requests.get("http://localhost:5000/get", params={"key": "name"})
    print(f"GET Response (node1): {response.json()}")

    response = requests.put("http://localhost:5001/put", json={"key": "age", "value": 30})
    print(f"PUT Response (node2): {response.json()}")

    response = requests.get("http://localhost:5001/get", params={"key": "age"})
    print(f"GET Response (node2): {response.json()}")

