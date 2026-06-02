from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = [
    "Laptop",
    "Phone",
    "Monitor",
    "Keyboard",
    "Mouse"
]

while True:

    dirty_record = random.randint(1, 5)

    if dirty_record == 1:
        order = {
            "order_id": random.randint(1000,9999),
            "customer_name": fake.name(),
            "product": None,
            "amount": 1000,
            "payment_status": "SUCCESS",
            "timestamp": str(datetime.now())
        }

    elif dirty_record == 2:
        order = {
            "order_id": random.randint(1000,9999),
            "customer_name": fake.name(),
            "product": random.choice(products),
            "amount": -500,
            "payment_status": "SUCCESS",
            "timestamp": str(datetime.now())
        }

    elif dirty_record == 3:
        order = {
            "order_id": random.randint(1000,9999),
            "customer_name": fake.name(),
            "product": random.choice(products),
            "amount": 2000,
            "payment_status": "UNKNOWN",
            "timestamp": str(datetime.now())
        }

    elif dirty_record == 4:
        order = {
            "order_id": random.randint(1000,9999),
            "customer_name": None,
            "product": random.choice(products),
            "amount": 5000,
            "payment_status": "SUCCESS",
            "timestamp": str(datetime.now())
        }

    else:
        order = {
            "order_id": random.randint(1000,9999),
            "customer_name": fake.name(),
            "product": random.choice(products),
            "amount": random.randint(500,50000),
            "payment_status": random.choice(["SUCCESS","FAILED"]),
            "timestamp": str(datetime.now())
        }

    producer.send("orders", order)

    print(order)

    time.sleep(2)