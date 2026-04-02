import pika
import json
import redis

# Conexión a Redis
r = redis.Redis(host='IP_REDIS', port=6379, db=0)

# Conexión a RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('IP_RABBIT_BROKER'))
channel = connection.channel()
channel.queue_declare(queue='tickets_queue', durable=True)

# Número total de tickets (para unnumbered)
TOTAL_TICKETS = 20000

def process_message(ch, method, properties, body):
    msg = json.loads(body)

    request_id = msg['request_id']

    # --- ID de mensaje ya procesado? ---
    if r.sismember("processed_requests", request_id):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # --- Procesar ticket ---
    if msg["type"] == "unnumbered":
        # Atomico: incrementar solo si no pasamos TOTAL_TICKETS
        ticket_sold = r.incr("unnumbered_sold")
        if ticket_sold > TOTAL_TICKETS:
            r.decr("unnumbered_sold")  # revertir
            # Ticket rechazado, opcional: log
        else:
            # Ticket vendido
            pass

    elif msg["type"] == "numbered":
        seat_id = msg['seat_id']
        # Intenta poner el asiento, si ya existe no vender
        if r.setnx(f"seat:{seat_id}", request_id):
            # Ticket vendido
            pass
        else:
            # Asiento ya vendido
            pass

    # --- Marcar request_id como procesado ---
    r.sadd("processed_requests", request_id)

    # Confirmar a RabbitMQ que procesamos el mensaje
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Consumo de la cola
channel.basic_qos(prefetch_count=1)  # evita sobrecarga en worker
channel.basic_consume(queue='tickets_queue', on_message_callback=process_message)

print("Worker listo. Esperando mensajes...")
channel.start_consuming()
