import hashlib
import threading


class ConsistentHashing:
    """
    Questa classe rappresenta la struttura dati per la consistent hashing.
    """
    def __init__(self, num_replicas=3):
        """
        Inizializza la struttura dati per la consistent hashing.
        :param num_replicas: numero di repliche virtuali per ogni nodo
        """
        self.num_replicas = num_replicas
        self.ring = {}  # dizionario che mappa l'hash di un nodo al nodo stesso
        self.sorted_nodes = []  # lista ordinata degli hash dei nodi
        self.nodes_status = {}  # dizionario che mappa lo stato di un nodo (online/offline)
        self.lock = threading.Lock()  # lock per garantire la consistenza della struttura dati

    @staticmethod
    def _hash(key):
        """
        Metodo privato per calcolare l'hash MD5 di una chiave e restituire un intero.
        :param key: chiave da hashare
        :return: intero rappresentante l'hash MD5 della chiave
        """
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        """
        Aggiunge un nodo al ring di consistent hashing con le repliche virtuali corrispondenti.
        :param node: identificatore del nodo da aggiungere
        """
        with self.lock:
            for i in range(self.num_replicas):
                node_hash = self._hash(f"{node}-{i}")
                self.ring[node_hash] = node
                self.sorted_nodes.append(node_hash)
                self.nodes_status[node] = True
            self.sorted_nodes.sort()

    def remove_node(self, node):
        """
        Rimuove un nodo dal ring di consistent hashing e le sue repliche virtuali corrispondenti.
        :param node: identificatore del nodo da rimuovere
        """
        with self.lock:
            for i in range(self.num_replicas):
                node_hash = self._hash(f"{node}-{i}")
                del self.ring[node_hash]
                self.sorted_nodes.remove(node_hash)
                self.nodes_status[node] = False

    def get_nodes(self, key, count):
        """
        Restituisce i nodi corrispondenti alla chiave fornita, utile per la replica dei dati.
        :param key: chiave per la quale trovare i nodi corrispondenti
        :param count: numero di nodi da restituire
        :return: lista di nodi corrispondenti alla chiave
        """
        nodes = []
        key_hash = self._hash(key)
        for i, node_hash in enumerate(self.sorted_nodes):
            if key_hash <= node_hash:
                start_idx = i
                break
        else:
            start_idx = 0
        for i in range(start_idx, start_idx + count):
            node = self.ring[self.sorted_nodes[i % len(self.sorted_nodes)]]
            if self.nodes_status[node]:
                nodes.append(node)
        return nodes
