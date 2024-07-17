import argparse
import requests
import threading
import time

from flask import Flask, request, jsonify


class Node:
    """
    Classe che rappresenta il singolo nodo/DB del sistema di storage distribuito.
    """

    def __init__(self, node, fault_tolerance_address):
        """
        Inizializza il nodo con l'indirizzo del nodo stesso e l'indirizzo del nodo di tolleranza ai guasti.
        param node: indirizzo del nodo (IP:porta)
        param fault_tolerance_address: indirizzo del nodo di tolleranza ai guasti (IP:porta)
        """
        self.fault_tolerance_address = fault_tolerance_address  # indirizzo del nodo di tolleranza ai guasti
        self.node = node  # coppia indirizzo-porta del nodo
        self.data = {}  # dizionario che contiene i dati del nodo
        self.lock = threading.Lock()  # Un oggetto di lock per garantire che l'accesso ai dati condivisi sia thread-safe
        self.stop_event = threading.Event()  # Un oggetto di evento per segnalare la terminazione dei thread

    def store(self, key, value):
        """
        Metodo per memorizzare un valore associato ad una chiave.
        param key: chiave del valore da memorizzare
        param value: valore da memorizzare
        """
        with self.lock:
            self.data[key] = value

    def retrieve(self, key):
        """
        Metodo per recuperare un valore associato ad una chiave.
        param key: chiave del valore da recuperare
        return: valore associato alla chiave
        """
        with self.lock:
            return self.data.get(key)

    def send_heartbeat(self):
        """
        Metodo per inviare un heartbeat al nodo di tolleranza ai guasti.
        """
        while not self.stop_event.is_set():
            time.sleep(5)
            current_time = time.time()
            try:
                response = requests.post(f"http://{self.fault_tolerance_address}/heartbeat", json={"node": self.node, "timestamp": current_time})
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Failed to send heartbeat to fault tolerance node: {e} by node {self.node}")

    def start(self):
        """
        Metodo per avviare il nodo.
        """
        self.stop_event.clear()
        # Avvio dei thread per l'invio e il controllo degli heartbeat
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

        # Creazione dell'app Flask per gestire le richieste di heartbeat
        app = Flask(__name__)

        # Endpoint per PUT (scrittura)
        @app.route('/put', methods=['PUT'])
        def put_data():
            data = request.get_json()
            key = data['key']
            value = data['value']
            self.store(key, value)
            return jsonify({"status": "ok"})

        # Endpoint per GET (lettura)
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
