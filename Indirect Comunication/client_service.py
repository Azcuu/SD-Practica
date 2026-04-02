import pika
import json
import time

# Conexión a RabbitMQ
rabbit_broker_ip = 'localhost'
connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_broker_ip))
channel = connection.channel()

# Cola principal de tickets
channel.queue_declare(queue='tickets_queue', durable=True)

# Función para procesar cada línea del benchmark
def process_line(line):
    parts = line.strip().split()

    if parts[0] != "BUY":
        return

    # --- UNNUMBERED ---
    if len(parts) == 3:
        msg = {
            "type": "unnumbered",
            "client_id": parts[1],
            "request_id": parts[2]
        }

    # --- NUMBERED ---
    elif len(parts) == 4:
        msg = {
            "type": "numbered",
            "client_id": parts[1],
            "seat_id": int(parts[2]),
            "request_id": parts[3]
        }

    else:
        return

    channel.basic_publish(
        exchange='',
        routing_key='tickets_queue',
        body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2)
    )

# Leer benchmark
start = time.time()
with open('FILENAME') as f:
    for line in f:
        process_line(line)

# Medir tiempo total
end = time.time()
print(f"Tiempo total de envío: {end - start:.2f}s")
        
# Cerramos la conexión
connection.close()
