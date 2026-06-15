# Phase 5 — Docker Compose + Live Dashboard

This phase wraps the full pipeline (phases 1–4) into a single
`docker-compose.yml` so the project can be brought up with one command, and
exposes a live Streamlit dashboard that consumes Kafka and PostgreSQL in
real time.

## What it gives you

- A unified Docker Compose stack with all five services on the same
  bridge network (`evnet`):
  - **kafka** — single-broker KRaft Kafka with dual listeners
    (`localhost:9092` for the host, `kafka:29092` inside the network)
  - **postgres** — TimescaleDB-flavored PostgreSQL 15
  - **db-init** — one-shot job that creates `inference_results`,
    `grid_stress`, `data_quality_events`
  - **phase1-producer** — replays `LOA.csv` into `ev-telemetry`
  - **phase2-flink** — PyFlink job that emits 15-min feature vectors to
    `features`
  - **phase3-fastapi** — XGBoost model behind FastAPI. On first start
    trains the model from the dataset, caches it in the `phase3_models`
    volume, then publishes predictions to `dynamic-prices`
  - **phase4-service** + **phase4-grid-stress** — write to Postgres,
    emit single-station and regional alerts on `alerts`
  - **dashboard** — Streamlit app on `:8501`

- A live dashboard (`app.py`) with:
  - per-station predicted-load **heatmap** (last 16 windows)
  - **price multiplier** per station (color-coded by alert level)
  - **Grid Stress Indicator** with the regional stress threshold line
  - **alerts feed** consuming the `alerts` Kafka topic in real time
  - **end-to-end latency** measured as
    `dashboard_arrival_time − Kafka message timestamp` on `dynamic-prices`

## Prerequisites

1. Docker Desktop (or Docker Engine + the Compose plugin)
2. The LOA dataset placed at:

   ```
   datasets/LOA-5min/LOA.csv
   ```

   See `datasets/LOA-5min/init.py` for the download link.

## Run the whole stack

From the **repo root** (so the `../datasets` mount resolves):

```bash
docker compose -f phase5/docker-compose.yml up --build
```

The first start is slow because:

- the Phase 2 image installs Java + PyFlink (~700 MB),
- Phase 3 trains the XGBoost model from `LOA.csv` (a few minutes on the
  full dataset). The model is cached in the `phase3_models` volume so
  subsequent starts are instant.

Once everything is up:

| Service        | URL                                |
| -------------- | ---------------------------------- |
| Dashboard      | <http://localhost:8501>            |
| FastAPI docs   | <http://localhost:8000/docs>       |
| PostgreSQL     | `localhost:5432` (evuser/evpass)   |
| Kafka (host)   | `localhost:9092`                   |

Tear down (and clear all volumes for a clean re-run):

```bash
docker compose -f phase5/docker-compose.yml down -v
```

## Running just the dashboard against an existing stack

If phases 1–4 are already running locally (not in compose), you can
launch only the dashboard:

```bash
cd phase5
pip install -r requirements.txt
KAFKA_BOOTSTRAP=localhost:9092 \
DB_HOST=localhost \
streamlit run app.py
```

## End-to-end latency

The dashboard reports two latency numbers:

- **avg E2E latency** — mean of `now − message.timestamp` measured on
  every record arriving on `dynamic-prices`. Because Kafka stamps the
  message at produce time (Phase 3 inference), this is the
  *FastAPI-to-dashboard* hop.
- **p95** — 95th percentile of the same series, once at least 20
  samples have been collected.

For the full ingest-to-price latency (Phase 1 → dashboard) we record the
wall-clock difference between the original `time_new` of the source
record and the dashboard arrival; this is reported in the demo video and
the final report rather than on the live UI (the LOA replay is
accelerated, so the figure on a busy display reflects replay speed, not
production latency).

## Configuration knobs

| Env var            | Default        | Where it's read         |
| ------------------ | -------------- | ----------------------- |
| `KAFKA_BOOTSTRAP`  | `kafka:29092`  | dashboard               |
| `DYNAMIC_PRICES_TOPIC` | `dynamic-prices` | dashboard          |
| `ALERTS_TOPIC`     | `alerts`       | dashboard               |
| `DB_HOST`          | `postgres`     | dashboard               |
| `REFRESH_SECONDS`  | `6`            | dashboard auto-refresh  |
| `REPLAY_SPEED`     | `300`          | Phase 1 producer        |
| `MODEL_PATH`       | `/models/xgboost_model.pkl` | Phase 3 entrypoint |

## Troubleshooting

- **Dashboard shows "Waiting for predictions…"** — open the FastAPI
  container logs (`docker logs ev-phase3-fastapi`) and verify it's
  consuming the `features` topic. The XGBoost training must finish
  before any prediction is produced.
- **Phase 2 container restart loop** — PyFlink needs Java 17. If your
  Docker host has memory pressure you may need to bump Docker's RAM to
  ≥ 6 GB.
- **Postgres "relation does not exist"** — the `db-init` container must
  finish before Phase 4 starts. The compose file enforces this via
  `service_completed_successfully`, but if you `docker compose up` only
  a subset of services, run `docker compose run --rm db-init` first.

## Known issues found during integration testing

While wiring up the full stack end-to-end, we hit a few issues worth
flagging to the rest of the team:

- **Phase 1 producer can OOM on the full dataset.** `phase1/producer.py`
  used to load the entire CSV into memory before replaying it, which
  hung/OOMed the container on the full `LOA.csv` (2.27 GB, ~53.8M rows).
  This has since been fixed upstream: the producer now streams the CSV
  in chunks and feeds per-station queues from a thread pool, and
  `MAX_STATIONS` / `MAX_ROWS` are read from the environment (both can be
  set on `phase1-producer` in this compose file to cap a test run).
- **`features` topic must be compacted.** Phase 2's `features` sink now
  uses the `upsert-kafka` connector (`PRIMARY KEY (station_id,
  time_bin)`) so that `rate_of_change` can look up the previous window.
  upsert-kafka requires `cleanup.policy=compact` on the target topic.
  This compose file handles it via the `kafka-init` service, which
  explicitly creates all five topics (with `features` set to `compact`)
  before any producer/consumer starts. This also fixes the old Flink
  AdminClient crash-loop on a fresh broker, since the topics now exist
  before Phase 2 subscribes to `ev-telemetry`.
- **Flink watermark drops "late" data on re-runs.** All timestamps in the
  LOA dataset fall within January 2023. Once Phase 2's event-time
  watermark has advanced past a given point (from any prior run), any
  data sent afterwards — regardless of station — with an earlier
  `time_new` is treated as late and silently dropped, so `features` and
  `dynamic-prices` stop receiving new messages. For a clean, reproducible
  run (e.g. before recording the demo), reset everything first:
  `docker compose -f docker-compose.yml down -v` wipes Kafka, Postgres and
  the cached model so the watermark starts fresh.

## Demo recording setup

`docker-compose.demo.yml` is a compose override used to record the demo
video: it replays `datasets/LOA-5min/LOA_demo.csv` (a 2,000-row slice — 4
stations × 500 rows) at `REPLAY_SPEED=1500`, so the whole run takes
~60-100 seconds and the dashboard fills up live on camera, including a
real "Avg E2E Latency" reading.

```bash
# 1. clean slate
docker compose -f phase5/docker-compose.yml -f phase5/docker-compose.demo.yml down -v

# 2. bring up everything except the producer (so consumers are ready first)
docker compose -f phase5/docker-compose.yml -f phase5/docker-compose.demo.yml \
  up -d --build kafka postgres db-init phase2-flink phase3-fastapi phase4-service phase4-grid-stress dashboard

# 3. start recording, then run the producer
docker compose -f phase5/docker-compose.yml -f phase5/docker-compose.demo.yml up phase1-producer --build
```
