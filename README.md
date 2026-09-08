# Kafka Order System

Kafka-based producer/consumer for order messages, using Avro serialization,
a running average of prices, retry logic, and a dead letter queue (DLQ).

## Project layout

```
kafka-order-system/
├── docker-compose.yml       # Redpanda broker (Kafka + Schema Registry)
├── schemas/order.avsc       # Avro schema for Order messages
├── producer/producer.py     # sends random orders
└── consumer/
    ├── consumer.py          # consumes orders, aggregates, retries, DLQs
    └── dlq_viewer.py         # shows failed messages in orders-dlq
```

## Setup

Requires Docker.

```bash
docker compose up -d
docker exec -it redpanda rpk topic create orders orders-dlq
```

Python environments (repeat for both `producer/` and `consumer/`):

```bash
cd producer
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Windows (PowerShell):

```powershell
cd .\producer
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Running it

Open 3 terminals (consumer first, then producer, then the DLQ viewer):

```bash
cd consumer && source .venv/bin/activate && python consumer.py
cd producer && source .venv/bin/activate && python producer.py
cd consumer && source .venv/bin/activate && python dlq_viewer.py
```

The producer sends one order per second. The consumer processes each
one and prints a running average price. About 1 in 4 messages fails
on purpose (simulated) to trigger a retry with backoff; if it fails
3 times it gets sent to the DLQ instead of being dropped.

## Where each requirement is implemented

- Avro serialization: `schemas/order.avsc`
- Running average: `RunningAverage` class in `consumer.py`
- Retry logic: `handle_message_with_retry()` in `consumer.py`
- DLQ: `send_to_dlq()` in `consumer.py`, topic `orders-dlq`

## Submitting

```bash
git remote add origin <your-repo-url>
git push -u origin main
```
