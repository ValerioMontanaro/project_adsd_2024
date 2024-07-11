import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


class Replication:
    """
    Classe per la gestione della replicazione dei dati.
    """

    def __init__(self, quorum_write, quorum_read):
        """
        Inizializza la classe Replication con i parametri di quorum specificati.
        :param quorum_write: quorum di scrittura
        :param quorum_read: quorum di lettura
        """
        self.quorum_write = quorum_write
        self.quorum_read = quorum_read

    @staticmethod
    def write_to_node(node, key, value):
        """
        Scrive il valore di una chiave su un nodo.
        :param node: nodo su cui scrivere il valore
        :param key: chiave da scrivere
        :param value: valore da scrivere
        :return: True se la scrittura ha avuto successo, False altrimenti
        """
        url = f"http://{node}/store/{key}"
        try:
            response = requests.put(url, json={"value": value})
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Failed to write to node {node}: {e}")
            return False

    def replicate_write(self, key, value, nodes):
        """
        Replica il valore di una chiave su più nodi.
        :param key: chiave da scrivere
        :param value: valore da scrivere
        :param nodes: lista di nodi su cui replicare il valore
        :return: True se almeno quorum_write scritture hanno avuto successo, False altrimenti
        """
        with ThreadPoolExecutor() as executor:
            future_to_node = {executor.submit(self.write_to_node, node, key, value): node for node in nodes}
            success_count = 0
            for future in as_completed(future_to_node):
                if future.result():
                    success_count += 1
                if success_count >= self.quorum_write:
                    break
        return success_count >= self.quorum_write

    @staticmethod
    def read_from_node(node, key):
        """
        Legge il valore di una chiave da un nodo.
        :param node: nodo da cui leggere il valore
        :param key: chiave da leggere
        :return: valore della chiave se la lettura ha avuto successo, None altrimenti
        """
        url = f"http://{node}/retrieve/{key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json().get('value')
        except requests.exceptions.RequestException as e:
            print(f"Failed to read from node {node}: {e}")
            return None

    def get_from_replicas(self, key, nodes, reduced_quorum):
        """
        Legge il valore di una chiave da più nodi e restituisce la prima risposta valida ricevuta.
        :param key: chiave da leggere
        :param nodes: lista di nodi da cui leggere il valore
        :param reduced_quorum: indica se usare un quorum ridotto
        :return: valore della chiave se almeno quorum_read letture hanno avuto successo, None altrimenti
        """
        quorum_read = self.quorum_read - 1 if reduced_quorum else self.quorum_read
        with ThreadPoolExecutor() as executor:
            future_to_node = {executor.submit(self.read_from_node, node, key): node for node in nodes}
            responses = []
            for future in as_completed(future_to_node):
                result = future.result()
                if result is not None:
                    responses.append(result)
                if len(responses) >= quorum_read:
                    break
            if len(responses) >= quorum_read:
                return responses[0]
            return None

    @staticmethod
    def remove_key_from_node(key, node):
        """
        Rimuove una chiave da un nodo.
        :param key: chiave da rimuovere
        :param node: nodo da cui rimuovere la chiave
        :return: True se la rimozione ha avuto successo, False altrimenti
        """
        url = f"http://{node}/remove/{key}"
        try:
            response = requests.delete(url)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Failed to remove key from node {node}: {e}")
            return False

    def verify_and_replicate(self, key, value, responsible_nodes, num_replicas, hash_ring):
        """
        Verifica e ripara le repliche di una chiave se necessario.
        :param key: chiave da verificare
        :param value: valore della chiave
        :param responsible_nodes: nodi responsabili della chiave
        :param num_replicas: numero di repliche richieste
        :param hash_ring: istanza di ConsistentHashing
        """
        actual_replica_count = sum(1 for node in responsible_nodes if hash_ring.nodes_status[node])
        if actual_replica_count < num_replicas:
            print(f"Repairing replicas for key {key}")

            # Trova i nuovi nodi responsabili per la chiave
            new_responsible_nodes = hash_ring.get_nodes(key, count=num_replicas)

            # Replica la chiave sui nuovi nodi responsabili
            self.replicate_write(key, value, new_responsible_nodes)
