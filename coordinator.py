from flask import Flask, request, jsonify
from consistent_hashing import ConsistentHashing
from replication import Replication


class Coordinator:
    """
    Questa classe rappresenta il coordinatore del sistema di storage distribuito.
    """
    def __init__(self, nodes_list, replication_factor, quorum_write, quorum_read):
        """
        Inizializza il coordinatore con la lista di nodi e il fattore di replicazione specificati
        :param nodes_list: lista di nodi disponibili
        :param replication_factor: numero di repliche per ogni chiave
        :param quorum_write: quorum di scrittura
        :param quorum_read: quorum di lettura
        """
        self.hash_ring = ConsistentHashing()
        self.replication_factor = replication_factor
        self.replication = Replication(quorum_write, quorum_read)
        self.node_offline = False  # Flag per indicare se un nodo è offline
        for node in nodes_list:
            self.hash_ring.add_node(node)

    def put(self, key, value):
        """
        Scrive il valore della chiave su più nodi
        :param key: chiave da scrivere
        :param value: valore da scrivere
        :return: True se almeno quorum_write scritture hanno avuto successo, False altrimenti
        """
        responsible_nodes = self.hash_ring.get_nodes(key, count=self.replication_factor)
        return self.replication.replicate_write(key, value, responsible_nodes)

    def get(self, key):
        """
        Legge il valore della chiave da più nodi e restituisce la prima risposta valida ricevuta
        :param key: chiave da leggere
        :return: valore della chiave se almeno quorum_read letture hanno avuto successo, None altrimenti
        """
        responsible_nodes = self.hash_ring.get_nodes(key, count=self.replication_factor)
        reduced_quorum = self.node_offline
        value = self.replication.get_from_replicas(key, responsible_nodes, reduced_quorum)
        if value is not None:
            if self.node_offline:
                self.replication.verify_and_replicate(key, value, responsible_nodes, self.replication_factor, self.hash_ring)
            return value
        return None

    def remove_node(self, node_id):
        """
        Rimuove un nodo dal ring di consistent hashing e le sue repliche virtuali corrispondenti
        :param node_id: identificatore del nodo da rimuovere
        :return: None
        """
        self.hash_ring.remove_node(node_id)
        self.node_offline = True


# Crea l'istanza dell'applicazione Flask
app = Flask(__name__)

# Configurazione del numero di repliche per ogni chiave
N = 3  # Numero di repliche

# Configurazione del quorum di scrittura e lettura
W = 2  # Quorum di scrittura
R = 2  # Quorum di lettura

# Crea un'istanza del Coordinator con i nodi e il fattore di replicazione, il quorum di scrittura e lettura specificati
nodes = ["localhost:5001", "localhost:5002", "localhost:5003"]
coordinator = Coordinator(nodes, N, W, R)


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


# Avvia l'applicazione Flask
if __name__ == '__main__':
    app.run(port=5000)
