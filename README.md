
---

## What This Is

A five-phase streaming pipeline that replays Ontario EV charging data (LOA dataset), computes per-station load features in 15-minute windows, runs XGBoost inference to predict load and set dynamic prices, stores results in PostgreSQL, and triggers grid stress alerts when regional capacity utilization exceeds 80%.

Data flow: `LOA.csv → Kafka → Flink → FastAPI/XGBoost → PostgreSQL + Alerts`

---

## Prerequisites

- Docker Desktop running
- Python 3.11 (Phase 2 requires 3.11 specifically)
- Java 11+ (needed by Flink)
- `LOA.csv` placed at `datasets/LOA-5min/LOA.csv`

Each phase has its own dependencies. Install them before running each phase (see per-phase steps below).

---

## Running the Full Pipeline

Open six terminals from the repo root. Start them in this order — consumers before producers so no messages are dropped.

### Terminal 1 — Infrastructure

```bash
docker compose up -d
```

This starts Kafka, PostgreSQL/TimescaleDB, and a short-lived init container that creates all five Kafka topics (`ev-telemetry`, `features`, `dynamic-prices`, `data-quality`, `alerts`). Wait about 30–40 seconds for Kafka to finish initializing before moving on.

Confirm topics were created:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Then create the database tables (only needed once):

```bash
cd phase4
pip install psycopg2-binary confluent-kafka
python init_db.py
cd ..
```

---

### Terminal 2 — Phase 4 consumer service

```bash
cd phase4
python phase4_service.py
```

Subscribes to `dynamic-prices` and `data-quality`. Writes inference results to PostgreSQL and forwards station-level alerts to the `alerts` topic.

---

### Terminal 3 — Phase 4 grid stress job

```bash
cd phase4
python grid_stress_job.py
```

Runs every 15 minutes. Computes average CUR across all active stations and triggers a regional alert if it exceeds 0.8.

---

### Terminal 4 — Phase 3 FastAPI inference service

```bash
cd phase3
pip install kafka-python -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

Wait for `Background Kafka consumer thread started.` before proceeding. This service subscribes to `features`, runs XGBoost inference on each vector, and publishes results to `dynamic-prices`.

---

### Terminal 5 — Phase 2 Flink feature engineering

```bash
cd phase2
.venv\Scripts\activate      # Windows
# source .venv/bin/activate  # Linux/Mac
python flink_feature_engineering.py
```

Subscribes to `ev-telemetry`, groups records into 15-minute tumbling windows per station, computes feature vectors, and publishes to `features`. Anomalous readings (> 3 std dev from rolling mean) are published to `data-quality`.

---

### Terminal 6 — Phase 1 data producer

```bash
cd phase1
pip install -r requirements.txt
python producer.py --max-rows 5000 --speed 0
```

Replays LOA charging records into `ev-telemetry`, one thread per station in time order. `--speed 0` means replay as fast as possible. Drop `--max-rows` to run the full dataset (~53M rows).

---

## Verifying the Pipeline

Once Phase 1 starts producing, check that results are flowing into the database:

```bash
docker exec -it postgres psql -U evuser -d evdb -c "SELECT COUNT(*) FROM inference_results;"
docker exec -it postgres psql -U evuser -d evdb -c "SELECT station_id, alert_level, price_multiplier FROM inference_results ORDER BY created_at DESC LIMIT 10;"
```

For grid stress entries (only appear after 15-minute windows close):

```bash
docker exec -it postgres psql -U evuser -d evdb -c "SELECT * FROM grid_stress ORDER BY created_at DESC LIMIT 5;"
```

Phase 3 (Terminal 4) will print a line per inference:
```
Inference done for station 1000604065 -> alert: normal, price: 0.12 CAD
```

Phase 4 (Terminal 2) will print a line per insert:
```
Inserted inference_result for station 1000604065 at 2023-06-01 14:00:00
```

---

## Full washout

```bash
docker compose down -v
```