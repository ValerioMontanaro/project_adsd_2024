import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging


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
        :param node: coppia indirizzo-porta del nodo su cui scrivere il valore
        :param key: chiave da scrivere
        :param value: valore da scrivere
        :return: True se la scrittura ha avuto successo, False altrimenti
        """
        url = f"http://{node}/put"  # costruisce l'URL per la richiesta PUT
        try:
            response = requests.put(url, json={"key": key, "value": value})  # invia una richiesta PUT al nodo
            response.raise_for_status()  # solleva un'eccezione se la richiesta ha avuto esito negativo
            logging.info(f"Successfully wrote to node {node}: {response.json()}")
            return True  # restituisce True se la richiesta ha avuto esito positivo
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to write to node {node}: {e}")
            return False  # restituisce False se la richiesta ha avuto esito negativo

    def replicate_write(self, key, value, nodes):
        """
        Replica il valore di una chiave su più nodi.
        :param key: chiave da scrivere
        :param value: valore da scrivere
        :param nodes: lista di coppie indirizzo-porta dei nodi su cui replicare il valore
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
        logging.info(f" wrote to {success_count} nodes")
        logging.info(f" quorum write {self.quorum_write}")
        return success_count >= self.quorum_write

    @staticmethod
    def read_from_node(node, key):
        """
        Legge il valore di una chiave da un nodo.
        :param node: coppia indirizzo-porta del nodo da cui leggere il valore
        :param key: chiave da leggere
        :return: valore della chiave se la lettura ha avuto successo, None altrimenti
        """
        url = f"http://{node}/get"  # costruisce l'URL per la richiesta GET
        try:
            response = requests.get(url, params={"key": key})  # invia una richiesta GET al nodo
            if response.status_code == 404:
                logging.info(f"Node {node} does not have the key {key} (not an error).")
                return None
            response.raise_for_status()  # solleva un'eccezione se la richiesta ha avuto esito negativo
            logging.info(f"Successfully read from node {node}: {response.json()}")
            return response.json().get('value')  # restituisce il valore della chiave se la richiesta ha avuto successo
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to read from node {node}: {e}")
            return None  # restituisce None se la richiesta ha avuto esito negativo

    def get_from_replicas(self, key, nodes):
        """
        Legge il valore di una chiave da più nodi e restituisce la prima risposta valida ricevuta.
        :param key: chiave da leggere
        :param nodes: lista di coppie indirizzo-porta dei nodi da cui leggere il valore
        :param reduced_quorum: indica se usare un quorum ridotto
        :return: valore della chiave se almeno quorum_read letture hanno avuto successo, None altrimenti
        """
        with ThreadPoolExecutor() as executor:
            future_to_node = {executor.submit(self.read_from_node, node, key): node for node in nodes}
            responses = []
            for future in as_completed(future_to_node):
                result = future.result()
                if result is not None:
                    responses.append(result)
                if len(responses) >= self.quorum_read:
                    break
            if len(responses) >= self.quorum_read:
                return responses[0]
            return None

    def has_value(self, node, key):
        """
        Verifica se il nodo ha il valore specificato.
        :param key: chiave da verificare
        :param node: nodo su cui verificare la chiave
        :return: True se il nodo ha il valore, False altrimenti
        """
        return self.read_from_node(node, key) is not None
    
    def update_quorum(self, quorum_write, quorum_read):
        """
        Aggiorna i parametri di quorum per la scrittura e la lettura se un nodo va offline.
        :param quorum_write: quorum di scrittura
        :param quorum_read: quorum di lettura
        """
        self.quorum_write = quorum_write - 1
        logging.info(f"Updated quorum write to {self.quorum_write}")
        self.quorum_read = quorum_read - 1
        logging.info(f"Updated quorum read to {self.quorum_read}")
