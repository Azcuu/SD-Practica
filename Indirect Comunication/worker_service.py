import pika
import json
import redis

r = redis.Redis(host='10.0.1.133', port=6379, db=0)
credentials = pika.PlainCredentials('admin', 'admin123')
parameters = pika.ConnectionParameters(
    host='10.0.1.142',
    credentials=credentials
)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='tickets_queue', durable=True)

TOTAL_TICKETS = 20000

def process_message(ch, method, properties, body):
    msg = json.loads(body)
    request_id = msg['request_id']

    # --- EVITAR REPROCESADO ---
    if not r.sadd("processed_requests", request_id):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # --- UNNUMBERED ---
    if msg["type"] == "unnumbered":
        ticket_sold = r.incr("unnumbered_sold")

        if ticket_sold > TOTAL_TICKETS:
            r.decr("unnumbered_sold")
            r.incr("global_fail")
        else:
            r.incr("global_success")

    # --- NUMBERED ---
    elif msg["type"] == "numbered":
        seat_id = msg['seat_id']

        if r.setnx(f"seat:{seat_id}", request_id):
            r.incr("global_success")
        else:
            r.incr("global_fail")

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='tickets_queue', on_message_callback=process_message)

print("Worker listo...")
channel.start_consuming()
