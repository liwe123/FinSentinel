import json
import time
import random
import argparse
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import get_config

fake = Faker()

MERCHANTS = ["M3001", "M3002", "M3003", "M3004", "M3005"]
USERS = [f"U{1000 + i}" for i in range(20)]
CITIES = [
    {"city": "New York", "lat": 40.7128, "lon": -74.0060},
    {"city": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
    {"city": "London", "lat": 51.5074, "lon": -0.1278},
    {"city": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"city": "Shanghai", "lat": 31.2304, "lon": 121.4737},
]


def generate_normal_tx():
    """生成一笔正常交易"""
    location = random.choice(CITIES)
    return {
        "transaction_id": f"TXN-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
        "user_id": random.choice(USERS),
        "merchant_id": random.choice(MERCHANTS),
        "amount": round(random.uniform(10, 2000), 2),
        "currency": "USD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": {
            "latitude": location["lat"] + random.uniform(-0.1, 0.1),
            "longitude": location["lon"] + random.uniform(-0.1, 0.1),
            "city": location["city"],
        },
        "ip_address": fake.ipv4(),
        "device_id": f"DEV-{fake.uuid4()[:8]}",
        "transaction_type": random.choice(["purchase", "transfer", "withdrawal"]),
    }


def inject_fraud_tx(rule_type):
    """注入指定类型的欺诈交易"""
    base_tx = generate_normal_tx()

    if rule_type == "R1":
        base_tx["user_id"] = "FRAUD_USER_R1"
        base_tx["amount"] = 100.0
    elif rule_type == "R2":
        base_tx["user_id"] = "FRAUD_USER_R2"
        base_tx["location"] = {
            "latitude": 35.6762,
            "longitude": 139.6503,
            "city": "Tokyo",
        }
    elif rule_type == "R3":
        base_tx["amount"] = random.uniform(6000, 10000)
        base_tx["timestamp"] = datetime.now(timezone.utc).replace(
            hour=random.randint(0, 4), minute=random.randint(0, 59)
        ).isoformat()
    elif rule_type == "R4":
        base_tx["merchant_id"] = "FRAUD_MERCHANT_R4"
        base_tx["amount"] = 50.0

    return base_tx


def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=3,
        retry_backoff_ms=1000,
        acks="all",
    )


def run_generator(inject_rule=None, continuous=True):
    """运行数据生成器"""
    config = get_config()
    kafka_config = config["kafka"]
    gen_config = config["generator"]

    producer = create_producer(kafka_config["bootstrap_servers"])
    topic = kafka_config["topic"]

    print(f"Starting transaction generator -> Kafka topic: {topic}")

    try:
        if inject_rule:
            tx = inject_fraud_tx(inject_rule)
            producer.send(topic, key=tx["user_id"], value=tx)
            producer.flush()
            print(f"[FRAUD INJECT] Rule {inject_rule}: {tx['transaction_id']}")
        else:
            count = 0
            batch_size = gen_config.get("batch_size", 1)
            interval = gen_config.get("interval_seconds", 1)
            while continuous:
                for _ in range(batch_size):
                    tx = generate_normal_tx()
                    producer.send(topic, key=tx["user_id"], value=tx)
                    count += 1
                if count % 10 == 0:
                    print(f"Generated {count} transactions")
                time.sleep(interval)
    except KeyboardInterrupt:
        print("Generator stopped")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transaction Generator")
    parser.add_argument(
        "--inject-fraud",
        type=str,
        choices=["R1", "R2", "R3", "R4"],
        help="Inject fraud transaction for specified rule",
    )
    args = parser.parse_args()

    if args.inject_fraud:
        run_generator(inject_rule=args.inject_fraud, continuous=False)
    else:
        run_generator()
