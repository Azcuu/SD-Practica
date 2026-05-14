from fastapi import FastAPI, HTTPException
import redis
import os

app = FastAPI()

# Configuración de Redis
# RECOMENDACIÓN: Usa la IP privada de tu instancia de Redis en AWS
REDIS_HOST = os.getenv("REDIS_HOST", "10.0.1.143")
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

TOTAL_TICKETS = 30000

@app.get("/health")
def health_check():
    return {"status": "online"}

# --- MODELO 1: TICKETS NO NUMERADOS ---
@app.post("/buy_unnumbered")
def buy_unnumbered(data: dict):
    # data contiene {"client_id": "...", "request_id": "..."}
    
    # INCR es una operación atómica. Redis garantiza que 
    # aunque lleguen 1000 peticiones, se procesan de 1 en 1.
    current_count = r.incr("ticket_counter")
    
    if current_count <= TOTAL_TICKETS:
        # Aquí podrías guardar el log de la compra en un Set de Redis
        # r.sadd("successful_buys", f"{data['client_id']}:{data['request_id']}")
        return {"status": "SUCCESS", "ticket_number": current_count}
    else:
        # Si nos pasamos, decrementamos para mantener el contador limpio (opcional)
        # o simplemente rechazamos.
        return {"status": "REJECTED", "reason": "Sold out"}

# --- MODELO 2: TICKETS NUMERADOS ---
@app.post("/buy_numbered")
def buy_numbered(data: dict):
    # data contiene {"client_id": "...", "seat_id": "...", "request_id": "..."}
    seat_id = data.get("seat_id")
    client_id = data.get("client_id")

    # SETNX (Set if Not eXists) es la clave para la consistencia.
    # Solo tendrá éxito si la llave 'seat:X' no existe en Redis.
    seat_key = f"seat:{seat_id}"
    was_set = r.setnx(seat_key, client_id)
    
    if was_set:
        return {"status": "SUCCESS", "seat": seat_id}
    else:
        return {"status": "REJECTED", "reason": "Seat already taken"}

# --- RESET (Para limpiar el benchmark y repetir pruebas) ---
@app.post("/admin/reset")
def reset_system():
    r.flushall()
    return {"status": "System reset"}
