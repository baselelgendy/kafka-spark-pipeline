from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timezone


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


order_id = 1001


while True:

    order = {
        "order_id": order_id,
        "customer_id": random.randint(100, 500),
        "product_id": random.randint(1, 50),
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(5.00, 500.00), 2),
        "order_time": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    }

    producer.send(
        "topic1_logs",
        value=order
    )

    producer.flush()

    print("Sent:", order)

    order_id += 1

    time.sleep(random.uniform(1.0, 2.0))
