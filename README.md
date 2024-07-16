# Distributed Key-Value Store

Questo progetto implementa un sistema di storage distribuito basato su un key-value store. Utilizza tecniche di hash consistente, replicazione e tolleranza ai guasti per garantire l'affidabilità e la disponibilità dei dati.

## Struttura del Progetto

La struttura del progetto è la seguente:

```plaintext
project_adsd_2024/
│
├── client.py
├── coordinator.py
├── node.py
├── consistent_hashing.py
├── replication.py
├── fault_tolerance.py
├── start.sh
├── requirements.txt
└── README.md
```

### Descrizione dei File

- **client.py**: Interfaccia cliente per interagire con il key-value store mediante operazioni GET e PUT.
- **coordinator.py**: Gestisce le richieste di lettura e scrittura e le inoltra ai nodi appropriati.
- **node.py**: Definisce la classe Node che rappresenta un singolo nodo nel sistema distribuito. Gestisce le operazioni di lettura e scrittura a livello locale.
- **consistent_hashing.py**: Contiene le funzioni per aggiungere e rimuovere nodi e trovare il nodo responsabile per una data chiave utilizzando l'hash consistente.
- **replication.py**: Implementa la logica per specificare il fattore di replica e distribuire i dati replicati sui nodi.
- **fault_tolerance.py**: Gestisce la rilevazione dei guasti e la notifica alla classe Coordinator.
- **start.sh**: Script per avviare e testare l'intero sistema tramite shell Linux.
- **requirements.txt**: Contiene le dipendenze necessarie per eseguire il progetto.
- **README.md**: Questo file, che descrive il progetto e fornisce istruzioni su come eseguirlo.

## Requisiti

Assicurati di avere installato Python 3 e pip. Le dipendenze possono essere installate utilizzando il file `requirements.txt`:

```sh
pip install -r requirements.txt
```

## Esecuzione del Progetto

Per eseguire il progetto, puoi utilizzare lo script **start.sh** che avvia il coordinatore, i nodi, e simula un nodo che va offline. Assicurati che lo script **start.sh** sia eseguibile:

```sh
chmod +x tests/start.sh
./start.sh
```

### Avvio Manuale

Se preferisci avviare i componenti manualmente, segui questi passaggi:

1. **Avvia il coordinatore**:
    ```sh
    python coordinator.py --address 127.0.0.1:8003 --nodes "127.0.0.1:8000,127.0.0.1:8007,127.0.0.1:8002,127.0.0.1:8005,127.0.0.1:8006" --replication_factor 3 --quorum_write 3 --quorum_read 3
    ```

2. **Avvia il nodo di fault tolerance**:
    ```sh
    python fault_tolerance.py --address 127.0.0.1:8004 --all_nodes "127.0.0.1:8000,127.0.0.1:8007,127.0.0.1:8002,127.0.0.1:8005,127.0.0.1:8006" --coordinator_address 127.0.0.1:8003
    ```

3. **Avvia i nodi normali**:
    ```sh
    python node.py --node 127.0.0.1:8000 --fault_tolerance_address 127.0.0.1:8004 &
    python node.py --node 127.0.0.1:8007 --fault_tolerance_address 127.0.0.1:8004 &
    python node.py --node 127.0.0.1:8002 --fault_tolerance_address 127.0.0.1:8004 &
    python node.py --node 127.0.0.1:8005 --fault_tolerance_address 127.0.0.1:8004 &
    python node.py --node 127.0.0.1:8006 --fault_tolerance_address 127.0.0.1:8004 &
    ```

4. **Esegui operazioni di PUT e GET**:
    ```sh
    python client.py --coordinator_address 127.0.0.1:8003 --operation put --key name --value Alice
    python client.py --coordinator_address 127.0.0.1:8003 --operation get --key name
    ```