import redis
import time

r = redis.Redis(host='IP_REDIS', port=6379, db=0, decode_responses=True)

def reset_all():
    r.flushall()
    print(" [x] Sistema reseteado (Redis limpio).")

def get_metrics():
    start = r.get("bench_start")
    end = r.get("bench_end")
    success = int(r.get("global_success") or 0)
    fail = int(r.get("global_fail") or 0)
    total_ops = success + fail
    
    if start and end:
        duration = float(end) - float(start)
        throughput = total_ops / duration if duration > 0 else 0
        
        print("\n--- RESULTADOS DEL BENCHMARK ---")
        print(f"Tiempo Total: {duration:.2f} segundos")
        print(f"Operaciones Totales: {total_ops}")
        print(f"Éxitos: {success} | Fallos: {fail}")
        print(f"Throughput: {throughput:.2f} ops/sec")
        print("--------------------------------\n")
    else:
        print("Esperando datos...")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
        reset_all()
    else:
        get_metrics()
