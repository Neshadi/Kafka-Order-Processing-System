"""
DLQ Viewer
----------
Small helper to tail the Dead Letter Queue topic so you can show, during
your live demo, exactly which orders failed permanently and why.

Run:
    python dlq_viewer.py
"""

import os
import json

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders-dlq")

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "dlq-viewer",
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([DLQ_TOPIC])

print(f"[DLQ VIEWER] Watching '{DLQ_TOPIC}' on {BOOTSTRAP_SERVERS} ... (Ctrl+C to stop)")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[DLQ VIEWER] Error: {msg.error()}")
            continue
        data = json.loads(msg.value().decode("utf-8"))
        print(f"[DLQ] order={data['order']} | reason={data['error']}")
except KeyboardInterrupt:
    print("\n[DLQ VIEWER] Stopping...")
finally:
    consumer.close()
