import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configura i parametri di quorum
W = 2  # Quorum di scrittura
R = 2  # Quorum di lettura


# Questa funzione si occupa di scrivere il valore di una chiave su un nodo
# Parametri:
# - node: nodo su cui scrivere il valore
# - key: chiave da scrivere
# - value: valore da scrivere
# Return:
# - True se la scrittura ha avuto successo, False altrimenti
def write_to_node(node, key, value):
    url = f"http://{node}/store/{key}"
    try:
        response = requests.put(url, json={"value": value})
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to write to node {node}: {e}")
        return False


# Questa funzione si occupa di replicare il valore di una chiave su più nodi
# Parametri:
# - key: chiave da scrivere
# - value: valore da scrivere
# - nodes: lista di nodi su cui replicare il valore
# Return:
# - True se almeno W scritture hanno avuto successo, False altrimenti
def replicate_write(key, value, nodes):
    # Eseguire le scritture in parallelo e aspettare W conferme
    with ThreadPoolExecutor() as executor:
        future_to_node = {executor.submit(write_to_node, node, key, value): node for node in nodes}
        success_count = 0
        for future in as_completed(future_to_node):
            if future.result():
                success_count += 1
            if success_count >= W:
                break

    return success_count >= W


# Questa funzione si occupa di leggere il valore di una chiave da un nodo
def read_from_node(node, key):
    url = f"http://{node}/retrieve/{key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get('value')
    except requests.exceptions.RequestException as e:
        print(f"Failed to read from node {node}: {e}")
        return None


# Questa funzione si occupa di leggere il valore di una chiave da più nodi e restituire la prima risposta valida ricevuta
# Parametri:
# - key: chiave da leggere
# - nodes: lista di nodi da cui leggere il valore
# Return:
# - valore della chiave se almeno R letture hanno avuto successo, None altrimenti
def get_from_replicas(key, nodes):
    # Eseguire le letture in parallelo e aspettare R risposte
    with ThreadPoolExecutor() as executor:
        future_to_node = {executor.submit(read_from_node, node, key): node for node in nodes}
        responses = []
        for future in as_completed(future_to_node):
            result = future.result()
            if result is not None:
                responses.append(result)
            if len(responses) >= R:
                break

        # Se ci sono risposte sufficienti, restituire la prima valida
        if len(responses) >= R:
            return responses[0]
        return None
