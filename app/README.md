# Kafka Notification System

A fault-tolerant notification delivery system built to survive crashes and duplicate messages without ever losing or double-sending a notification. Python · Postgres · Kafka · Redis · Docker Compose.

## What it does

Delivers notifications through a message queue, with the guarantee that:

- every notification is durably recorded **before** it's ever published, and
- a crash, restart, or duplicate delivery can never corrupt the outcome.

## Architecture

The flow is ordered deliberately for safety:

1. **Producer** writes a `PENDING` row to **Postgres first**, _then_ publishes the notification id to **Kafka**. Because the row always exists before the publish, a message with no matching row is impossible to be real — it can be safely ignored (_safe by construction_).
2. **Consumer** reads the id from Kafka and looks up the **fresh status in Postgres** — it never trusts a status riding on the message, because an in-flight status can be stale.
3. On success, the status is flipped to **`SENT`** — and only after the send actually succeeds.
4. On failure, the message is **retried with exponential backoff**. The attempt count lives in **Redis**, not in process memory, so the count survives a consumer crash.
5. Once retries are exhausted, the message is routed to a **Dead Letter Queue (DLQ)**.

### Why the split

- **Postgres** is the source of truth — written on every message. Losing it means losing correctness.
- **Redis** holds the retry counter — fast and forgettable. Losing it degrades speed but never correctness (the counter just resets; the retry philosophy still holds).
- The **consumer is stateless** — it keeps no memory between messages, so it's safe to crash, restart, or run several in parallel.

## Tech stack

- **Python** — producer and consumer
- **PostgreSQL** — durable status store (source of truth)
- **Apache Kafka** — message queue / decoupling + buffering
- **Redis** — retry counter (in-memory, loss-tolerant)
- **Docker Compose** — runs Postgres, Kafka, and Redis together

## Running it

Bring the infrastructure up first — the scripts can't connect to anything until Postgres, Kafka, and Redis are running:

```bash
docker compose up -d
docker compose ps        # confirm all services are Up
```

Then, from the `app/` directory with the virtualenv active:

```bash
source venv/bin/activate
python consumer.py       # start the consumer (leave running)
python producer.py       # in a second terminal, publish a notification
```

## Status

Producer, consumer, idempotency handling, retry/backoff, DLQ, and Redis integration are in place. The Redis-backed retry counter logic and producer broken-state handling are the current work in progress.
