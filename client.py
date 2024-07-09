import requests


class Client:
    def __init__(self, coordinator_url):
        # URL del coordinatore
        self.coordinator_url = coordinator_url

    def put(self, key, value):
        url = f"{self.coordinator_url}/put/{key}"
        try:
            response = requests.put(url, json={"value": value})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to PUT data: {e}")
            return None

    def get(self, key):
        url = f"{self.coordinator_url}/get/{key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json().get('value')
        except requests.exceptions.RequestException as e:
            print(f"Failed to GET data: {e}")
            return None


# Esempio di utilizzo del client
if __name__ == "__main__":
    client = Client("http://localhost:5000")

    # Test della scrittura di un valore
    put_response = client.put("testkey", "testvalue")
    print(f"PUT response: {put_response}")

    # Test della lettura di un valore
    get_response = client.get("testkey")
    print(f"GET response: {get_response}")
