import requests
import threading
import time
import sys

# Configuración
NGINX_URL = "http://IP_DE_TU_VM_NGINX"  # <--- Cambia esto
NUM_THREADS = 50  # Ajusta según la potencia de la VM

# Métricas globales
results = []
success_count = 0
failed_count = 0
lock = threading.Lock()

def send_request(line):
    global success_count, failed_count
    parts = line.strip().split()
    if not parts:
        return

    # Formatear el comando según el tipo de benchmark
    # Unnumbered: BUY <client_id> <request_id>
    # Numbered: BUY <client_id> <seat_id> <request_id>
    
    payload = {}
    endpoint = ""

    if len(parts) == 3: # Unnumbered
        endpoint = "/buy_unnumbered"
        payload = {
            "client_id": parts[1],
            "request_id": parts[2]
        }
    elif len(parts) == 4: # Numbered
        endpoint = "/buy_numbered"
        payload = {
            "client_id": parts[1],
            "seat_id": parts[2],
            "request_id": parts[3]
        }

    try:
        start_time = time.time()
        response = requests.post(f"{NGINX_URL}{endpoint}", json=payload, timeout=5)
        latency = time.time() - start_time
        
        with lock:
            if response.status_code == 200 and response.json().get("status") == "SUCCESS":
                success_count += 1
            else:
                failed_count += 1
    except Exception as e:
        with lock:
            failed_count += 1
            # print(f"Error: {e}")

def run_benchmark(file_path):
    global success_count, failed_count
    success_count = 0
    failed_count = 0
    
    with open(file_path, 'r') as f:
        lines = f.readlines()

    print(f"--- Iniciando Benchmark: {file_path} ---")
    print(f"Enviando {len(lines)} peticiones con {NUM_THREADS} hilos...")
    
    start_total = time.time()
    
    # Uso de hilos para simular concurrencia
    threads = []
    # Para no saturar la memoria, procesamos en bloques si el archivo es muy grande
    for line in lines:
        t = threading.Thread(target=send_request, args=(line,))
        threads.append(t)
        t.start()
        
        # Limitador de hilos activos para no colapsar la VM cliente
        if len(threads) >= NUM_THREADS:
            for t in threads:
                t.join()
            threads = []

    for t in threads: # Limpiar hilos restantes
        t.join()
        
    end_total = time.time()
    duration = end_total - start_total

    print("\n--- Resultados ---")
    print(f"Tiempo total: {duration:.2f} segundos")
    print(f"Throughput: {len(lines)/duration:.2f} ops/sec")
    print(f"Éxitos: {success_count}")
    print(f"Fallos/Rechazos: {failed_count}")
    print("------------------\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python client_benchmark.py <archivo_benchmark.txt>")
    else:
        run_benchmark(sys.argv[1])
