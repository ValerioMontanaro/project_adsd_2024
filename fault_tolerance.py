import argparse
import logging
import requests
import threading
import time

from flask import Flask, request, jsonify

# Configura il logging
logging.basicConfig(level=logging.INFO)


class FaultTolerance:
    """
    Classe che rappresenta il nodo di tolleranza ai guasti del sistema di storage distribuito.
    """

    def __init__(self, address, all_nodes, coordinator_address):
        """
        Inizializza il nodo di tolleranza ai guasti con l'indirizzo, la lista di tutti i nodi e l'indirizzo del coordinatore.
        param address: indirizzo del nodo di tolleranza ai guasti (IP:porta)
        param all_nodes: lista di tutti gli indirizzi IP:porta dei nodi
        param coordinator_address: indirizzo del coordinatore (IP:porta)
        """
        self.address = address
        self.all_nodes = all_nodes  # lista di tutti gli id dei nodi
        self.coordinator_address = coordinator_address
        self.confirmed_failures = set()  # Set per tenere traccia dei nodi segnalati come offline
        self.lock = threading.Lock()
        self.heartbeat_table = {node: time.time() for node in self.all_nodes}  # Inizializzare heartbeat_table con node_id come chiave e 0 come valore

    def update_heartbeat_table(self, node, timestamp):
        """
        Aggiorna il timestamp dell'heartbeat di un nodo.
        param node: id del nodo da aggiornare (IP:porta)
        param timestamp: timestamp dell'heartbeat del nodo
        """
        with self.lock:
            self.heartbeat_table[node] = timestamp

    def check_heartbeat_table(self):
        """
        Controlla periodicamente la tabella degli heartbeat e notifica il coordinatore se un nodo non invia un heartbeat
        in un determinato delta di tempo, in questo caso 25 secondi.
        """
        while True:
            with self.lock:
                current_time = time.time()
                for node, timestamp in self.heartbeat_table.items():
                    if current_time - timestamp > 25:
                        self.notify_coordinator(node)
                        self.confirmed_failures.add(node)  # Aggiungi il nodo al set dei fallimenti confermati
            time.sleep(1)

    def notify_coordinator(self, node):
        """
        Notifica il coordinatore di un nodo offline.
        param node: id del nodo da notificare (IP:porta)
        """
        if node in self.confirmed_failures:
            return  # Se il nodo è già stato segnalato, non fare nulla
        try:
            response = requests.post(f"http://{self.coordinator_address}/node_offline", json={"node": node})
            response.raise_for_status()  # Raise an exception for 4xx/5xx status codes, se l'eccezione viene sollevata allora si passa direttamente al blocco except, ALTRIMENTI si va avanti nel blocco try
            print(f"Coordinator notified of node {node} failure")
        except requests.exceptions.RequestException as e:
            print(f"Failed to notify coordinator: {e}")

    def start(self):
        """
        Avvia il nodo di tolleranza ai guasti.
        """
        app = Flask(__name__)

        # Endpoint per ricevere gli heartbeat dai nodi
        @app.route('/heartbeat', methods=['POST'])
        def report_heartbeat():
            data = request.get_json()
            node = data['node']
            timestamp = data['timestamp']
            self.update_heartbeat_table(node, timestamp)
            return jsonify({"status": "ok"})

        # Log che indica che il server Flask è stato avviato
        logging.info(f"Starting Flask server on {self.address}")

        # Avvia un thread per controllare periodicamente la tabella degli heartbeat
        threading.Thread(target=self.check_heartbeat_table, daemon=True).start()

        # Log che indica che il controllo degli heartbeat è stato avviato
        logging.info("Heartbeat check & flask server started for fault tolerance node")

        try:
            # Avvia il server Flask con l'indirizzo e la porta specificati
            app.run(host=self.address.split(':')[0], port=int(self.address.split(':')[1]))
        except Exception as e:
            logging.error(f"ERROR IN FLASK APP: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fault Tolerance Node")
    parser.add_argument('--address', required=True, help='The address of the fault tolerance node (IP:port)')
    parser.add_argument('--all_nodes', required=True, type=str, help='A JSON string representing a list of all IP:port addresses of the nodes in the system ')
    parser.add_argument('--coordinator_address', required=True, help='The address of the coordinator node (IP:port)')

    args = parser.parse_args()

    # Converte la stringa JSON in una lista di nodi
    all_nodes = args.all_nodes.split(',')

    fault_tolerance = FaultTolerance(
        address=args.address,
        all_nodes=all_nodes,
        coordinator_address=args.coordinator_address
    )

    try:
        fault_tolerance.start()
        print("Fault tolerance node started")
    except KeyboardInterrupt:
        pass
