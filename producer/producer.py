"""
Order Producer
--------------
Generates random purchase-order messages and publishes them to the
Kafka "orders" topic, serialized with Avro (schema is fetched from /
registered with the Schema Registry automatically the first time it runs).

Run:
    python producer.py
"""

import os
import time
import random

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka import SerializingProducer

# ---------------------------------------------------------------------------
# Configuration (override with environment variables if you like)
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("ORDERS_TOPIC", "orders")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order.avsc")

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

# ---------------------------------------------------------------------------
# Avro / Kafka setup
# ---------------------------------------------------------------------------
with open(SCHEMA_PATH, "r") as f:
    schema_str = f.read()

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
}
producer = SerializingProducer(producer_conf)


def delivery_report(err, msg):
    """Called once per message to indicate delivery result."""
    if err is not None:
        print(f"[PRODUCER] Delivery FAILED for order {msg.key()}: {err}")
    else:
        print(
            f"[PRODUCER] Delivered order {msg.key()} -> "
            f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def generate_order(order_id: int) -> dict:
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def main():
    order_id = 1000
    print(f"[PRODUCER] Starting. Sending to topic '{TOPIC}' on {BOOTSTRAP_SERVERS} ...")
    try:
        while True:
            order_id += 1
            order = generate_order(order_id)

            producer.produce(
                topic=TOPIC,
                key=order["orderId"],
                value=order,
                on_delivery=delivery_report,
            )
            # Serve delivery callbacks without blocking
            producer.poll(0)

            time.sleep(1)  # one order per second - tune as you like
    except KeyboardInterrupt:
        print("\n[PRODUCER] Stopping (Ctrl+C received)...")
    finally:
        print("[PRODUCER] Flushing remaining messages...")
        producer.flush()


if __name__ == "__main__":
    main()
