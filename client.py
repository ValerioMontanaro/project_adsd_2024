import argparse

import requests


class Client:
    """
    Client per interagire con il coordinatore.
    """
    def __init__(self, coordinator_url):
        """
        Inizializza il client con l'URL del coordinatore.
        :param coordinator_url: URL del coordinatore in formato "http://indirizzo:porta"
        """
        self.coordinator_url = coordinator_url

    def put(self, key, value):
        """
        Invia una richiesta PUT al coordinatore per scrivere un valore.
        :param key: chiave da scrivere
        :param value: valore da scrivere
        :return: risposta del coordinatore alla richiesta PUT (True/False)
        """
        url = f"http://{self.coordinator_url}/put/{key}"
        try:
            response = requests.put(url, json={"value": value})
            response.raise_for_status()  # solleva un'eccezione se la richiesta ha avuto esito negativo
            return response.json()  # restituisce la risposta del coordinatore come dizionario JSON solo se la richiesta ha avuto successo
        except requests.exceptions.RequestException as e:
            print(f"Failed to PUT data: {e}")
            return None  # restituisce None se la richiesta ha avuto esito negativo

    def get(self, key):
        """
        Invia una richiesta GET al coordinatore per leggere un valore.
        :param key: chiave da leggere
        :return: valore della chiave se la richiesta GET ha avuto successo, None altrimenti
        """
        url = f"http://{self.coordinator_url}/get/{key}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # solleva un'eccezione se la richiesta ha avuto esito negativo
            return response.json().get('value')  # restituisce il valore della chiave solo se la richiesta ha avuto successo
        except requests.exceptions.RequestException as e:
            print(f"Failed to GET data: {e}")
            return None  # restituisce None se la richiesta ha avuto esito negativo


# Esempio di utilizzo del client
if __name__ == "__main__":

    # Parsing degli argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Client for interacting with the distributed storage system")
    parser.add_argument('--coordinator_address', required=True, help='The address of the coordinator (IP:port)')
    parser.add_argument('--operation', required=True, choices=['put', 'get'], help='The operation to perform')
    parser.add_argument('--key', required=True, help='The key to operate on')
    parser.add_argument('--value', help='The value to put')  # Optional argument for PUT operation

    args = parser.parse_args()

    client = Client(coordinator_url=args.coordinator_address)

    if args.operation == 'put':
        if args.value is None:
            print("PUT operation requires a value.")
        else:
            response = client.put(args.key, args.value)
            print(f"PUT Response: {response}")
    elif args.operation == 'get':
        response = client.get(args.key)
        print(f"GET Response: {response}")
