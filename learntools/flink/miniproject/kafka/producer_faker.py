import json
import time
import random
import uuid
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

KAFKA_TOPIC = "zomato_transactions_raw"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

# --- Simulated users & devices ---
USERS = [f"u_{i}" for i in range(1, 51)]
DEVICES = [f"d_{i}" for i in range(1, 20)]
RESTAURANTS = [f"r_{i}" for i in range(1, 30)]
CITIES = ["Bangalore", "Mumbai", "Delhi"]

# Assign stable device per normal user
USER_DEVICE_MAP = {
    user: random.choice(DEVICES) for user in USERS
}

def normal_transaction(user_id):
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "device_id": USER_DEVICE_MAP[user_id],
        "device_type": random.choice(["mobile", "web"]),
        "order_id": str(uuid.uuid4()),
        "amount": round(random.uniform(150, 500), 2),
        "restaurant_id": random.choice(RESTAURANTS),
        "city": random.choice(CITIES),
        "payment_method": random.choice(["UPI", "CARD"]),
        "is_cod": False,
        "event_time": int(time.time() * 1000),
        "behavior": "normal"
    }

def fraud_transaction(user_id, shared_device):
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "device_id": shared_device,
        "device_type": random.choice(["mobile", "web"]),
        "order_id": str(uuid.uuid4()),
        "amount": round(random.uniform(1500, 3000), 2),
        "restaurant_id": random.choice(RESTAURANTS),
        "city": random.choice(CITIES),
        "payment_method": "CARD",
        "is_cod": False,
        "event_time": int(time.time() * 1000),
        "behavior": "fraud_simulated"
    }

def run():
    fraud_device = "d_fraud_shared"

    while True:
        # 80% normal traffic
        if random.random() < 0.8:
            user = random.choice(USERS)
            event = normal_transaction(user)
            sleep_time = random.uniform(1, 3)

        # 20% fraud traffic
        else:
            user = random.choice(USERS)
            event = fraud_transaction(user, fraud_device)
            sleep_time = random.uniform(0.1, 0.3)

        producer.send(
            topic=KAFKA_TOPIC,
            key=event["user_id"],
            value=event
        )

        print(event)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run()
