# Phase 1 — Kafka + Data Ingestion

Reads the LOA EV charging dataset and replays it into the `ev-telemetry` Kafka topic, simulating real-time data from charging stations. One thread per station, messages published in time order.

---

## Prerequisites

Download `LOA.csv` and place it at:

```
datasets/LOA-5min/LOA.csv
```

> Google Drive link: https://drive.google.com/drive/folders/1xP5vyOobbNP82tG6gri0kp9OEJiwv0Gi?usp=drive_link

---

## Setup

```bash
cd phase1
docker compose up -d
pip install -r requirements.txt
```

---

## Running the Producer

**Full dataset** (53.8M rows, ~521 stations):
```bash
python producer.py
```

**Quick test** (50k rows, no replay delay — good for verifying messages flow):
```bash
python producer.py --max-rows 50000 --speed 0
```

### All options

| Flag | Default | Description |
|---|---|---|
| `--csv PATH` | `../datasets/LOA-5min/LOA.csv` | Path to LOA.csv |
| `--bootstrap BROKER` | `localhost:9092` | Kafka broker address |
| `--speed N` | `100.0` | Replay speed multiplier (0 = no delay, max throughput) |
| `--max-stations N` | `0` (all) | Limit number of stations loaded |
| `--max-rows N` | `0` (all) | Stop reading after N rows |

---

## Verifying Messages

Once the producer is running, check that messages are flowing:

```bash
docker exec -it phase1-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ev-telemetry \
  --from-beginning \
  --max-messages 5
```

Expected output:
```json
{"station_id": "10137141", "time_new": "2023-01-01 00:00:00", "duration": 0.0, "kwh": 0.0}
```

---

## Message Format

Each message published to `ev-telemetry` looks like:

```json
{
  "station_id": "10137141",
  "time_new": "2023-01-01 00:05:00",
  "duration": 0.0,
  "kwh": 0.0
}
```

The Kafka message **key** is `station_id` — this ensures all records for the same station land on the same partition, which Phase 2 (Flink) relies on for per-station windowing.
