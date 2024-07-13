import argparse
import threading
import logging
from flask import Flask, request, jsonify
from consistent_hashing import ConsistentHashing
from replication import Replication
import time

# Configura il logging
logging.basicConfig(level=logging.INFO)

class Coordinator:
    """
    Questa classe rappresenta il coordinatore del sistema di storage distribuito.
    """
    def __init__(self, nodes_list, replication_factor, quorum_write, quorum_read, address):
        """
        Inizializza il coordinatore con la lista di nodi e il fattore di replicazione specificati.
        :param nodes_list: lista che contiene coppie indirizzo-porta dei nodi iniziali
        :param replication_factor: numero di repliche per ogni chiave
        :param quorum_write: quorum di scrittura
        :param quorum_read: quorum di lettura
        :param address: indirizzo del coordinatore (IP:porta)
        """
        self.hash_ring = ConsistentHashing()
        self.replication_factor = replication_factor
        self.quorum_write = quorum_write
        self.quorum_read = quorum_read
        self.replication = Replication(self.quorum_write, self.quorum_read)
        self.address = address
        self.node_offline = False  # Flag per indicare se un nodo è offline

        # Aggiunge i nodi iniziali al ring di consistent hashing
        for node in nodes_list:
            self.hash_ring.add_node(node)

    def put(self, key, value):
        """
        Scrive il valore della chiave su più nodi.
        :param key: chiave da scrivere
        :param value: valore da scrivere
        :return: True se almeno quorum_write scritture hanno avuto successo, False altrimenti
        """
        responsible_nodes = self.hash_ring.get_nodes(key, self.replication_factor)
        logging.info(f"Responsible nodes for key '{key}': {responsible_nodes}")
        return self.replication.replicate_write(key, value, responsible_nodes)

    def get(self, key):
        """
        Legge il valore della chiave da più nodi e restituisce la prima risposta valida ricevuta.
        :param key: chiave da leggere
        :return: valore della chiave se almeno quorum_read letture hanno avuto successo, None altrimenti
        """

        responsible_nodes = self.hash_ring.get_nodes(key, self.replication_factor)

        # Ottiene il valore della chiave da almeno quorum_read nodi o da quorum_read - 1 nodi se un nodo è offline
        value = self.replication.get_from_replicas(key, responsible_nodes)

        # Propaga il valore ai nodi che non ce l'hanno se il nodo è offline
        if value is not None:
            if self.node_offline:
                self.propagate_value(key, value, responsible_nodes)
            return value
        return None

    def propagate_value(self, key, value, responsible_nodes):
        """
        Propaga il valore ai nodi che non ce l'hanno.
        :param key: chiave da replicare
        :param value: valore da replicare
        :param responsible_nodes: nodi responsabili della chiave
        """
        for node in responsible_nodes:
            # Se il nodo non ha il valore, lo scriviamo di nuovo
            if not self.replication.has_value(node, key):
                self.replication.replicate_write(key, value, [node])

    def remove_node(self, node_id):
        """
        Rimuove un nodo dal ring di consistent hashing e le sue repliche virtuali corrispondenti.
        :param node_id: identificatore del nodo da rimuovere
        :return: None
        """
        self.hash_ring.remove_node(node_id)
        self.node_offline = True
        self.replication.update_quorum(self.quorum_write, self.quorum_read)
        logging.info(f"Node {node_id} removed and marked as offline.")

    def start(self):

        # Crea l'istanza dell'applicazione Flask
        app = Flask(__name__)

        # Endpoint per PUT (scrittura)
        @app.route('/put/<key>', methods=['PUT'])
        def put(key):
            value = request.json['value']
            if coordinator.put(key, value):
                return jsonify({"status": "success"})
            else:
                return jsonify({"status": "failure"}), 500

        # Endpoint per GET (lettura)
        @app.route('/get/<key>', methods=['GET'])
        def get(key):
            value = coordinator.get(key)
            if value is not None:
                return jsonify({"value": value})
            else:
                return jsonify({"value": None}), 404

        # Endpoint per notificare un nodo offline
        @app.route('/node_offline', methods=['POST'])
        def notify_node_offline():
            node_id = request.json['node']
            coordinator.remove_node(node_id)
            return jsonify({"status": "node removed"})

        # Avvia il server Flask con l'indirizzo e la porta specificati
        app.run(host=self.address.split(':')[0], port=int(self.address.split(':')[1]))


# Avvia l'applicazione Flask
if __name__ == '__main__':

    # Parsing degli argomenti da riga di comando
    parser = argparse.ArgumentParser(description="Coordinator for distributed storage system")
    parser.add_argument('--address', required=True, help='The address of the coordinator (IP:port)')
    parser.add_argument('--nodes', required=True, type=str, help='List of node addresses (IP:port) separated by commas')
    parser.add_argument('--replication_factor', type=int, default=3, help='Replication factor')
    parser.add_argument('--quorum_write', type=int, default=2, help='Quorum write value')
    parser.add_argument('--quorum_read', type=int, default=2, help='Quorum read value')

    args = parser.parse_args()

    nodes_list1 = args.nodes.split(',')  # Converte la stringa di nodi in una lista
    
    # Rimuovere gli apici singoli e ordinare la lista
    sorted_nodes = sorted([node.strip('"') for node in nodes_list1], key=lambda x: int(x.split(':')[1]))

    # Convertire la lista a una stringa con doppi apici
    nodes_list = [f'"{node}"' for node in sorted_nodes]
                  
    print(f"Nodes list: {nodes_list}")
    print(type(nodes_list))
    print(type(nodes_list[0]))

    coordinator = Coordinator(
        nodes_list=["127.0.0.1:8000", "127.0.0.1:8002", "127.0.0.1:8007","127.0.0.1:8005","127.0.0.1:8006"],
        replication_factor=args.replication_factor,
        quorum_write=args.quorum_write,
        quorum_read=args.quorum_read,
        address=args.address,
    )

    threading.Thread(target=coordinator.start, daemon=True).start()

    # Mantieni il main thread attivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping coordinator.")
