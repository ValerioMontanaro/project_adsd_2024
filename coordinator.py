from flask import Flask, request, jsonify
from consistent_hashing import ConsistentHashing
from replication import replicate_write, get_from_replicas

# Crea l'istanza dell'applicazione Flask
app = Flask(__name__)

# Configurazione del numero di repliche per ogni chiave
N = 2  # Numero di repliche

# Crea un'istanza dell'hashing consistente
hash_ring = ConsistentHashing()

# Aggiungere nodi al ring (esempio con 3 nodi)
nodes = ["localhost:5001", "localhost:5002", "localhost:5003"]
for node in nodes:
    hash_ring.add_node(node)


# Endpoint per PUT (scrittura)
@app.route('/put/<key>', methods=['PUT'])
def put(key):
    # Ottieni il valore dalla richiesta JSON
    value = request.json['value']
    # Determinare i nodi responsabili per la chiave
    responsible_nodes = hash_ring.get_nodes(key, count=N)
    if replicate_write(key, value, responsible_nodes):
        return jsonify({"status": "success"})
    else:
        return jsonify({"status": "failure"}), 500


# Endpoint per GET (lettura)
@app.route('/get/<key>', methods=['GET'])
def get(key):
    # Determinare i nodi responsabili per la chiave
    responsible_nodes = hash_ring.get_nodes(key, count=N)  # Supponendo un fattore di replica di 3
    value = get_from_replicas(key, responsible_nodes)
    if value is not None:
        return jsonify({"value": value})
    else:
        return jsonify({"value": None}), 404


# Avvia l'applicazione Flask
if __name__ == '__main__':
    app.run(port=5000)
