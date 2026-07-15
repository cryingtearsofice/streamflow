import itertools
import json
import random
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

from confluent_kafka import Producer
from pydantic import BaseModel, Field

from schemas import TransactionEvent, TransactionType, TransactionSource, TransactionStatus

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "producer_config.json"


class ProducerConfig(BaseModel):
    bootstrap_servers: str
    topic: str
    event_interval_seconds: float = Field(gt=0)
    total_events: int | None = None
    num_accounts: int = Field(gt=0)
    invalid_rate: float = Field(ge=0, le=1)


def load_config(path: Path = CONFIG_PATH) -> ProducerConfig:
    with open(path) as f:
        return ProducerConfig(**json.load(f))


def generate_valid_event_dict(account_pool: list[str]) -> dict:
    return {
        "event_id": uuid4(),
        "event_type": random.choice(list(TransactionType)),
        "event_ts": datetime.now(timezone.utc),
        "source": random.choice(list(TransactionSource)),
        "account_id": random.choice(account_pool),
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


def maybe_corrupt(d, invalid_rate):
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


def delivery_report(err, msg):
    if err is not None:
        print(f"FAILED: {err}")
    else:
        print(f"delivered to {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


def run(config: ProducerConfig):
    kafka_producer = Producer({"bootstrap.servers": config.bootstrap_servers})
    account_pool = [f"ACCT-{random.randint(10_000_000, 99_999_999)}" for _ in range(config.num_accounts)]
    iterator = range(config.total_events) if config.total_events else itertools.count()

    try:
        for _ in iterator:
            d, corruption_name = maybe_corrupt(generate_valid_event_dict(account_pool), config.invalid_rate)
            payload = serializable(d, corruption_name)
            print(f"corruption = {corruption_name!r:20} -> {payload}")
            kafka_producer.produce(
                config.topic,
                key=d["account_id"],
                value=payload,
                callback=delivery_report,
            )
            kafka_producer.poll(0)
            time.sleep(config.event_interval_seconds)
    except KeyboardInterrupt:
        print("Interrupted - flushing remaining messages...")
    finally:
        kafka_producer.flush()


if __name__ == "__main__":
    run(load_config())
