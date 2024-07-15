#!/bin/bash

# Avvia il coordinatore
python coordinator.py --address 127.0.0.1:8003 --nodes "127.0.0.1:8000,127.0.0.1:8007,127.0.0.1:8002,127.0.0.1:8005,127.0.0.1:8006" --replication_factor 3 --quorum_write 3 --quorum_read 3 &
COORDINATOR_PID=$!
sleep 5

# Avvia il nodo di fault tolerance
python fault_tolerance.py --address 127.0.0.1:8004 --all_nodes "127.0.0.1:8000,127.0.0.1:8007,127.0.0.1:8002,127.0.0.1:8005,127.0.0.1:8006" --coordinator_address 127.0.0.1:8003 &
FT_PID=$!
sleep 5

# Avvia i nodi normali
python node.py --node 127.0.0.1:8000 --fault_tolerance_address 127.0.0.1:8004 &
NODE1_PID=$!
python node.py --node 127.0.0.1:8007 --fault_tolerance_address 127.0.0.1:8004 &
NODE2_PID=$!
python node.py --node 127.0.0.1:8002 --fault_tolerance_address 127.0.0.1:8004 &
NODE3_PID=$!
python node.py --node 127.0.0.1:8005 --fault_tolerance_address 127.0.0.1:8004 &
NODE4_PID=$!
python node.py --node 127.0.0.1:8006 --fault_tolerance_address 127.0.0.1:8004 &
NODE5_PID=$!

# Attendi che i nodi si avviino
sleep 10

# Esegui operazioni PUT e GET
python client.py --coordinator_address 127.0.0.1:8003 --operation put --key name --value Alice
python client.py --coordinator_address 127.0.0.1:8003 --operation get --key name

# Lascia che i nodi funzionino per un po'
sleep 20

# Simula il nodo che va offline
echo "Simulando il nodo 127.0.0.1:8002 che va offline..."
kill $NODE3_PID

# Lascia che il sistema rilevi il nodo offline
sleep 30

# Esegui operazioni PUT e GET dopo che il nodo è offline
python client.py --coordinator_address 127.0.0.1:8003 --operation get --key name
python client.py --coordinator_address 127.0.0.1:8003 --operation get --key name
python client.py --coordinator_address 127.0.0.1:8003 --operation put --key age --value 30
python client.py --coordinator_address 127.0.0.1:8003 --operation get --key age

# Ferma tutti i processi rimanenti
kill $COORDINATOR_PID
kill $FT_PID
kill $NODE2_PID
kill $NODE1_PID
kill $NODE4_PID
kill $NODE5_PID

echo "Test completato"
