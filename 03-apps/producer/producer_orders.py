import json
import os
import random
import time
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecom_orders")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
)

def make_order():
    items_n = random.randint(1, 5)
    items = []
    total = 0.0
    for _ in range(items_n):
        qty = random.randint(1, 3)
        price = round(random.uniform(5, 120), 2)
        items.append({
            "sku": fake.bothify(text="SKU-????-####"),
            "qty": qty,
            "unit_price": price
        })
        total += qty * price

    created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "event_type": "order_created",
        "event_ts": created_at,
        "order": {
            "order_id": f"ord_{fake.uuid4()}",
            "customer_id": f"cus_{fake.uuid4()}",
            "country": fake.country_code(),
            "payment_method": random.choice(["card", "paypal", "apple_pay"]),
            "currency": "EUR",
            "amount": round(total, 2),
            "items": items
        }
    }

if __name__ == "__main__":
    print(f"Producing to topic={TOPIC} bootstrap={BOOTSTRAP}")
    while True:
        evt = make_order()
        producer.send(TOPIC, evt)
        producer.flush()
        print("sent", evt["order"]["order_id"], "amount", evt["order"]["amount"])
        time.sleep(float(os.getenv("SLEEP_SEC", "0.5")))
