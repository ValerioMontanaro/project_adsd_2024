import argparse
import requests
import threading
import time

from flask import Flask, request, jsonify


class Node:
    """Questo è il costruttore della classe che inizializza un nodo con un ID, un indirizzo e una lista di nodi vicini.
    Inizializza anche un dizionario vuoto per i dati e una tabella di heartbeat."""

    def __init__(self, node, fault_tolerance_address):
        self.fault_tolerance_address = fault_tolerance_address  # indirizzo del nodo di tolleranza ai guasti
        self.node = node  # coppia indirizzo-porta del nodo
        self.data = {}  # dizionario che contiene i dati del nodo
        self.lock = threading.Lock()  # Un oggetto di lock per garantire che l'accesso ai dati condivisi sia thread-safe
        self.stop_event = threading.Event()

    def store(self, key, value):
        with self.lock:
            self.data[key] = value

    def retrieve(self, key):
        with self.lock:
            return self.data.get(key)

    def send_heartbeat(self):
        while not self.stop_event.is_set():
            time.sleep(5)
            current_time = time.time()
            try:
                response = requests.post(f"http://{self.fault_tolerance_address}/heartbeat", json={"node": self.node, "timestamp": current_time})
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Failed to send heartbeat to fault tolerance node: {e} by node {self.node}")

    def start(self):
        self.stop_event.clear()
        # Avvio dei thread per l'invio e il controllo degli heartbeat
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

        # Creazione dell'app Flask per gestire le richieste di heartbeat
        app = Flask(__name__)

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
        app.run(host=self.node.split(':')[0], port=int(self.node.split(':')[1]))

    def stop(self):
        self.stop_event.set()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Node for fault tolerance system")
    parser.add_argument('--node', required=True, help='The address of the node (IP:port)')
    parser.add_argument('--fault_tolerance_address', required=True, help='The address of the fault tolerance node (IP:port)')

    args = parser.parse_args()

    node = Node(
        node=args.node,
        fault_tolerance_address=args.fault_tolerance_address
    )

    try:
        node.start()
    except KeyboardInterrupt:
        node.stop()
