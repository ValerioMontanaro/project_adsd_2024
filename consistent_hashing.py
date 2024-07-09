import hashlib


class ConsistentHashing:
    # Inizializza la struttura dati per la consistent hashing
    # Parametri:
    # - num_replicas: numero di repliche virtuali per ogni nodo
    # - ring: dizionario che mappa l'hash di un nodo al nodo stesso
    # - sorted_keys: lista ordinata in modo crescente degli hash dei nodi
    def __init__(self, num_replicas=3):
        self.num_replicas = num_replicas
        self.ring = {}
        self.sorted_keys = []

    # Metodo privato per calcolare l'hash MD5 di una chiave e restituire un intero
    # Parametri:
    # - key: chiave da hashare
    # Return:
    # - intero rappresentante l'hash MD5 della chiave
    @staticmethod
    def _hash(key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    # Aggiunge un nodo al ring di consistent hashing con le repliche virtuali corrispondenti
    # Parametri:
    # - node: identificatore del nodo da aggiungere
    def add_node(self, node):
        for i in range(self.num_replicas):
            # Calcola l'hash del nodo con un suffisso numerico per ogni replica virtuale
            node_hash = self._hash(f"{node}-{i}")
            # Aggiungi il nodo all'anello e alla lista ordinata degli hash
            self.ring[node_hash] = node
            self.sorted_keys.append(node_hash)
        self.sorted_keys.sort()

    # Rimuove un nodo dal ring di consistent hashing e le sue repliche virtuali corrispondenti
    # Parametri:
    # - node: identificatore del nodo da rimuovere
    def remove_node(self, node):
        for i in range(self.num_replicas):
            node_hash = self._hash(f"{node}-{i}")
            del self.ring[node_hash]
            self.sorted_keys.remove(node_hash)

    # Restituisce i nodi corrispondenti alla chiave fornita, utile per la replica dei dati
    # Parametri:
    # - key: chiave per la quale trovare i nodi corrispondenti
    # - count: numero di nodi da restituire
    # Return:
    # - lista di nodi corrispondenti alla chiave
    def get_nodes(self, key, count=1):
        nodes = []
        # Calcola l'hash della chiave
        key_hash = self._hash(key)
        # Trova il primo nodo nel ring che ha un hash maggiore o uguale all'hash della chiave
        start_idx = 0
        for i, node_hash in enumerate(self.sorted_keys):
            if key_hash <= node_hash:
                start_idx = i
                break
        # Aggiungi i nodi successivi al nodo trovato fino a raggiungere il numero richiesto
        for i in range(start_idx, start_idx + count):
            nodes.append(self.ring[self.sorted_keys[i % len(
                self.sorted_keys)]])  # Wrapping around cioè se arrivo alla fine del ring riparto dall'inizio grazie al %
        return nodes
