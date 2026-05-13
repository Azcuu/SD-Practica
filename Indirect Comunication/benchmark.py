import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0)

def run_monitor():
    target = 25997 # Cambia esto según tu prueba
    print(f"--- MONITOREO DE RENDIMIENTO ---")

    start_time = None

    while True:
        try:
            success = int(r.get("global_success") or 0)
            fail = int(r.get("global_fail") or 0)
            total = success + fail

            if total > 0 and start_time is None:
                start_time = time.time()
                print(">> Tráfico detectado. Cronómetro iniciado.")

            if start_time:
                elapsed = time.time() - start_time
                throughput = total / elapsed if elapsed > 0 else 0

                # Formato de salida limpia
                print(f"Progreso: {total:5d} | Éxitos: {success:5d} | Fallos: {fail:5d} | "
                      f"Tiempo: {elapsed:6.2f}s | Throughput: {throughput:8.2f} op/s", end="\r")

                if total >= target:
                    total_time = time.time() - start_time
                    print(f"\n\n--- RESULTADOS FINALES ---")
                    print(f"Total Execution Time: {total_time:.4f} seconds")
                    print(f"Throughput: {target / total_time:.2f} operations/second")
                    print(f"Success/Fail Ratio: {success}/{fail}")
                    break

            time.sleep(0.2)
        except Exception as e:
            print(f"\nError: {e}")
            break

if __name__ == "__main__":
    run_monitor()
