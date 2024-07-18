import hashlib
import logging
import threading


class ConsistentHashing:
    """
    Questa classe rappresenta la struttura dati per la consistent hashing.
    """
    def __init__(self, num_virtual_nodes=3):
        """
        Inizializza la struttura dati per la consistent hashing.
        :param num_virtual_nodes: numero di repliche virtuali per ogni nodo
        """
        self.num_virtual_nodes = num_virtual_nodes
        self.ring = {}  # dizionario che mappa l'hash di un nodo al nodo stesso
        self.sorted_nodes = []  # lista ordinata degli hash dei nodi
        self.nodes_status = {}  # dizionario che mappa lo stato di un nodo (online/offline)
        self.lock = threading.Lock()  # lock per garantire la consistenza della struttura dati

    @staticmethod
    def _hash(key):
        """
        Metodo privato per calcolare l'hash MD5 di una chiave o nodo e restituire un intero.
        :param key: chiave da hashare
        :return: intero rappresentante l'hash MD5 della chiave
        """
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        """
        Aggiunge un nodo al ring di consistent hashing con le repliche virtuali corrispondenti.
        :param node: coppia indirizzo-porta del nodo da aggiungere
        """
        with self.lock:
            for i in range(self.num_virtual_nodes):
                node_hash = self._hash(f"{node}-{i}")  # calcola l'hash del nodo concatenato con un indice virtuale
                self.ring[node_hash] = node  # mappa l'hash del nodo al nodo stesso
                self.sorted_nodes.append(node_hash)  # aggiunge l'hash del nodo alla lista ordinata
            self.nodes_status[node] = True  # imposta lo stato del nodo come online
            self.sorted_nodes.sort()  # riordina la lista degli hash dei nodi

    def remove_node(self, node):
        """
        Rimuove un nodo dal ring di consistent hashing e le sue repliche virtuali corrispondenti.
        :param node: coppia indirizzo-porta del nodo da rimuovere
        """
        with self.lock:
            if node in self.nodes_status:
                self.nodes_status[node] = False  # imposta lo stato del nodo come offline

    def get_nodes(self, key, count):
        """
        Restituisce i nodi corrispondenti alla chiave fornita, utile per la replica dei dati.
        :param key: chiave per la quale trovare i nodi corrispondenti
        :param count: numero di nodi da restituire
        :return: lista di coppie indirizzo-porta dei nodi corrispondenti alla chiave
        """
        nodes = []
        key_hash = self._hash(key)
        logging.info(f"Hash dei nodi: {self.sorted_nodes}")

        # Trova l'indice del nodo a partire dal quale cercare i nodi corrispondenti alla chiave
        for i, node_hash in enumerate(self.sorted_nodes):
            if key_hash <= node_hash:
                start_idx = i
                break
        else:
            start_idx = 0
            
        logging.info(f"Start index for key '{key}' with hash {key_hash}: {start_idx}")

        # Trova i nodi corrispondenti alla chiave
        while len(nodes) < count :
            node = self.ring[self.sorted_nodes[start_idx % len(self.sorted_nodes)]]
            logging.info(f"Nodo: {node}")
            logging.info(f"hash: {self.sorted_nodes[start_idx % len(self.sorted_nodes)]}")
            if self.nodes_status[node] and node not in nodes:  # Assicurati che il nodo non sia duplicato
                nodes.append(node)
            start_idx += 1
        logging.info(f"Responsible nodes for key '{key}': {nodes}")
        return nodes
