"""
Order Consumer
--------------
Consumes order messages (Avro-deserialized) from the "orders" topic and:

  1. Maintains a real-time running average of order prices.
  2. Simulates transient processing failures and retries them with
     exponential backoff (Retry Logic requirement).
  3. Sends messages that permanently fail (after exhausting retries, or
     that hit a non-recoverable error) to a Dead Letter Queue topic
     ("orders-dlq") instead of dropping them (DLQ requirement).

Run:
    python consumer.py
"""

import os
import json
import time
import random

from confluent_kafka import DeserializingConsumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders-dlq")
GROUP_ID = os.getenv("GROUP_ID", "order-consumer-group")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order.avsc")

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1
# Purely for demo purposes: chance that "processing" a message fails
# transiently, so you can actually SEE the retry logic kick in during
# your live demo.
TRANSIENT_FAILURE_RATE = 0.25

# ---------------------------------------------------------------------------
# Avro / Kafka setup
# ---------------------------------------------------------------------------
with open(SCHEMA_PATH, "r") as f:
    schema_str = f.read()

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": avro_deserializer,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    # We commit manually AFTER a message has been fully handled
    # (processed OR routed to DLQ) so we never silently lose messages.
    "enable.auto.commit": False,
}
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([TOPIC])

# Plain (non-Avro) producer for the DLQ: we store the original order
# plus the error reason as JSON, which is simplest for a dead-letter
# topic that you'll typically just want to inspect manually.
dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


# ---------------------------------------------------------------------------
# Real-time aggregation
# ---------------------------------------------------------------------------
class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, price: float) -> float:
        self.count += 1
        self.total += price
        return self.total / self.count


running_avg = RunningAverage()


# ---------------------------------------------------------------------------
# Processing + retry + DLQ
# ---------------------------------------------------------------------------
class TransientProcessingError(Exception):
    """Represents a recoverable, temporary failure (network blip, etc.)."""


def process_order(order: dict) -> None:
    """
    'Business logic' for handling one order.
    Randomly raises a TransientProcessingError to simulate a temporary
    failure (e.g. a downstream service timeout) so the retry path is
    exercised during the demo.
    """
    if random.random() < TRANSIENT_FAILURE_RATE:
        raise TransientProcessingError(
            f"Simulated temporary failure while processing order {order['orderId']}"
        )

    avg = running_avg.update(order["price"])
    print(
        f"[CONSUMER] Processed order {order['orderId']:>6} | "
        f"product={order['product']:<6} price={order['price']:>7.2f} | "
        f"running_avg={avg:.2f} (n={running_avg.count})"
    )


def send_to_dlq(order: dict, error_reason: str) -> None:
    payload = json.dumps({"order": order, "error": error_reason}).encode("utf-8")
    dlq_producer.produce(DLQ_TOPIC, key=order["orderId"], value=payload)
    dlq_producer.flush()
    print(f"[CONSUMER] -> Sent order {order['orderId']} to DLQ ({DLQ_TOPIC}): {error_reason}")


def handle_message_with_retry(order: dict) -> None:
    """
    Try to process the order. On a transient failure, retry with
    exponential backoff up to MAX_RETRIES times. If it still fails
    (or a non-transient error occurs), send it to the DLQ.
    """
    attempt = 0
    while True:
        try:
            process_order(order)
            return  # success
        except TransientProcessingError as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                send_to_dlq(order, f"Exceeded {MAX_RETRIES} retries: {e}")
                return
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(
                f"[CONSUMER] Transient error on order {order['orderId']} "
                f"(attempt {attempt}/{MAX_RETRIES}) - retrying in {backoff}s..."
            )
            time.sleep(backoff)
        except Exception as e:
            # Any other exception is treated as a permanent failure -
            # no point retrying, straight to the DLQ.
            send_to_dlq(order, f"Permanent error: {e}")
            return


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    print(f"[CONSUMER] Starting. Listening on topic '{TOPIC}' ({BOOTSTRAP_SERVERS}) ...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[CONSUMER] Kafka error: {msg.error()}")
                continue

            order = msg.value()
            if order is None:
                # Tombstone / undeserializable message - skip
                consumer.commit(msg)
                continue

            handle_message_with_retry(order)
            consumer.commit(msg)  # commit only after the message is fully handled
    except KeyboardInterrupt:
        print("\n[CONSUMER] Stopping (Ctrl+C received)...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
