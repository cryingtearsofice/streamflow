import json
from uuid import UUID, uuid4
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
from schemas import TransactionEvent, TransactionType, TransactionSource, TransactionStatus
from decimal import Decimal

def generate_valid_event_dict() -> dict:
    return {
        "event_id": uuid4(),
        "event_type": random.choice(list(TransactionType)),
        "event_ts": datetime.now(timezone.utc),
        "source": random.choice(list(TransactionSource)),
        "account_id": f"ACCT-{random.randint(10_000_000, 99_999_999)}",
        "amount": Decimal(random.randint(100, 500000)) / 100,
        "status": random.choice(list(TransactionStatus))
    }

def dropped_field(d):
    d.pop(random.choice(list(d)), None)
    return d

def bad_status(d):
    d["status"] = "CANCELLED"
    return d

def bad_amount_type(d):
    d["amount"] = "Not a number"
    return d

CORRUPTIONS = [dropped_field, bad_status, bad_amount_type]

def maybe_corrupt(d, invalid_rate=0.2):
    if random.random() < invalid_rate:
        corruption = random.choice(list(CORRUPTIONS))
        return corruption(d), corruption.__name__
    return d, None

def json_default(obj):
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {obj!r}")

def serializable(d, corruption_name):
    if corruption_name is None:
        return TransactionEvent(**d).model_dump_json()
    return json.dumps(d, default=json_default)

kafka_producer = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(err, msg):
    if err is not None:
        print(f"FAILED: {err}")
    else:
        print(f"delivered to {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


for _ in range(20):
    d, corruption_name = maybe_corrupt(generate_valid_event_dict())
    payload = serializable(d, corruption_name)
    kafka_producer.produce(
        "streamflow.events",
        key=d["account_id"],
        value=payload,
        callback=delivery_report,
    )
    kafka_producer.poll(0)

kafka_producer.flush()