# Kafka Order System (Avro + Retry + DLQ)

A Kafka-based system that produces and consumes purchase-order messages,
serialized with **Avro**, with real-time price aggregation, **retry
logic** for transient failures, and a **Dead Letter Queue (DLQ)** for
permanently failed messages.

## Architecture

```
 producer.py --(Avro-encoded Order)--> [ orders ] topic --> consumer.py
                                                                |  |
                                                running average |  | on permanent
                                                (printed live)  |  | failure
                                                                v  v
                                                          [ orders-dlq ] topic
                                                                |
                                                          dlq_viewer.py
```

- **Broker**: [Redpanda](https://redpanda.com/) — a Kafka-API-compatible
  broker that comes with a **built-in Schema Registry**, so there's no
  separate Zookeeper/Kafka/Schema-Registry stack to wrangle. Your code
  talks to it exactly as it would to real Apache Kafka.
- **Serialization**: Avro, via `confluent-kafka`'s `AvroSerializer` /
  `AvroDeserializer`, using the schema in `schemas/order.avsc`.
- **Language**: Python 3.9+ (the assignment allows any language — this
  is one valid, well-supported choice).

## Project layout

```
kafka-order-system/
├── docker-compose.yml       # Redpanda broker + web console
├── schemas/
│   └── order.avsc           # Avro schema for Order messages
├── producer/
│   ├── producer.py          # generates & sends random orders
│   └── requirements.txt
└── consumer/
    ├── consumer.py          # consumes, aggregates, retries, DLQs
    ├── dlq_viewer.py         # tails the DLQ topic (for demos)
    └── requirements.txt
```

## 1. Start the Kafka broker

Requires Docker + Docker Compose.

```bash
docker compose up -d
```

This starts:
- Redpanda (Kafka API on `localhost:19092`, Schema Registry on `localhost:8081`)
- Redpanda Console (web UI) at http://localhost:8080 — handy for showing
  topics/messages live during your demo.

Topics are auto-created on first use, but you can create them explicitly
(and it looks more deliberate in a demo) with `rpk`:

```bash
docker exec -it redpanda rpk topic create orders orders-dlq
```

## 2. Set up Python environments

If you're using Windows PowerShell, use the commands below instead of the Bash syntax.

```powershell
cd .\producer
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cd ..

cd .\consumer
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cd ..
```

If PowerShell blocks script execution, run this once in the current shell before activating the venv:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

For macOS/Linux, the original Bash version is still valid:

```bash
cd producer && python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cd ..

cd consumer && python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cd ..
```

## 3. Run it

Open **three terminals** and activate the virtual environment in each one.

```powershell
# Terminal 1 — consumer (start this first so it's ready to receive)
cd .\consumer
.\.venv\Scripts\Activate.ps1
python consumer.py

# Terminal 2 — producer
cd .\producer
.\.venv\Scripts\Activate.ps1
python producer.py

# Terminal 3 — DLQ viewer (optional, but great for a live demo)
cd .\consumer
.\.venv\Scripts\Activate.ps1
python dlq_viewer.py
```

On macOS/Linux, use:

```bash
# Terminal 1 — consumer
cd consumer && source .venv/bin/activate && python consumer.py

# Terminal 2 — producer
cd producer && source .venv/bin/activate && python producer.py

# Terminal 3 — DLQ viewer
cd consumer && source .venv/bin/activate && python dlq_viewer.py
```

What you should see:
- The producer prints a delivery confirmation for every order.
- The consumer prints each processed order along with the **running
  average price**.
- Every so often (~25% of messages, tunable via `TRANSIENT_FAILURE_RATE`
  in `consumer.py`), the consumer will simulate a transient failure,
  print retry attempts with increasing backoff, and either succeed on
  a later attempt or — after `MAX_RETRIES` — forward the order to the
  DLQ, which shows up in the `dlq_viewer.py` terminal.

## 4. How each requirement is satisfied

| Requirement | Where |
|---|---|
| Avro serialization | `schemas/order.avsc`, `AvroSerializer`/`AvroDeserializer` in both producer & consumer |
| Real-time aggregation (running avg) | `RunningAverage` class in `consumer.py`, updated per message |
| Retry logic | `handle_message_with_retry()` in `consumer.py` — exponential backoff, `MAX_RETRIES` |
| Dead Letter Queue | `send_to_dlq()` in `consumer.py`, publishes to `orders-dlq` topic |
| Live demo | Run producer + consumer + dlq_viewer side by side as above |

## 5. Tuning knobs (worth understanding, not just running)

- `TRANSIENT_FAILURE_RATE` in `consumer.py` — probability a message
  "temporarily" fails, to make retries visible in a demo. In a real
  system this would be actual failure conditions (timeouts, downstream
  5xxs, etc.), not a random roll.
- `MAX_RETRIES` / `BASE_BACKOFF_SECONDS` — retry count and exponential
  backoff base.
- `TOPIC` / `DLQ_TOPIC` / `GROUP_ID` / `BOOTSTRAP_SERVERS` — all
  overridable via environment variables if you need to point at a
  different cluster.

## 6. Git repository & submission

```bash
cd kafka-order-system
git init
git add .
git commit -m "Kafka order system: Avro, running average, retry logic, DLQ"
# create a repo on GitHub/GitLab, then:
git remote add origin <your-repo-url>
git branch -M main
git push -u origin main
```

Commit as you go (e.g. one commit for schema + producer, one for
consumer, one for retry/DLQ, one for README) rather than a single
giant commit — it shows your working process, which many rubrics for
this kind of assignment specifically look for.

## Notes / things to be ready to explain in your live demo

- Why Avro over JSON: compact binary format, enforced schema, and the
  Schema Registry allows schema evolution without breaking consumers.
- Why `enable.auto.commit=False` and manual `consumer.commit(msg)`
  after processing: guarantees you don't lose (or silently skip) a
  message that failed and hasn't yet been retried or DLQ'd — this is
  "at-least-once" processing.
- Why exponential backoff for retries rather than a fixed delay:
  avoids hammering a struggling downstream dependency.
- The DLQ stores the *original order* plus the *failure reason*, so a
  human (or a replay tool) can inspect and potentially reprocess it
  later — nothing is silently dropped.
