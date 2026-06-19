# Live Demo Script — EV Charging IoT Pipeline

**Total demo time:** ~8–12 minutes  
**Setup required before the room:** See Pre-Demo Checklist below

---

## Architecture Overview (slide/whiteboard — 1 min)

> **Speaker notes:** Start here before touching the terminal. Walk the audience through the data flow on the slide.

"We built a five-phase streaming pipeline on Ontario's public LOA EV charging dataset.
Raw telemetry flows from a Kafka producer → PyFlink feature engineering → XGBoost inference on FastAPI → PostgreSQL. A live Streamlit dashboard shows everything updating in real time."

```
LOA.csv → [Phase 1] Kafka Producer
              ↓  ev-telemetry topic
         [Phase 2] PyFlink  →  15-min feature vectors
              ↓  features topic
         [Phase 3] FastAPI + XGBoost  →  predicted load, price, alert
              ↓  dynamic-prices topic
         [Phase 4] PostgreSQL writer + grid stress job
              ↓
         [Phase 5] Streamlit Dashboard  ←  also reads alerts topic live
```


DOWN LOCAL POSTGRES: Stop-Service -Name "postgresql-x64-15"


---

## Pre-Demo Checklist (do before entering the room)

- [ ] Docker Desktop is running
- [ ] `datasets/LOA-5min/LOA.csv` exists in the repo root
- [ ] `datasets/LOA-5min/LOA_demo.csv` exists (the 2,000-row demo slice — 4 stations × 500 rows)
- [ ] Run a clean teardown so the Flink watermark resets fresh:
  ```bash
  docker compose -f phase5/docker-compose.yml -f phase5/docker-compose.demo.yml down -v
  ```
- [ ] Pre-pull / pre-build images to avoid wait time in front of the audience:
  ```bash
  docker compose -f phase5/docker-compose.yml -f phase5/docker-compose.demo.yml build
  ```
- [ ] Open two browser tabs in advance: `http://localhost:8501` and `http://localhost:8000/docs`
- [ ] Have two terminal windows open and positioned side by side

---

## Step 1 — Start the Stack (all services except the producer)

> **Speaker notes:** "Everything runs in Docker — one compose file for the full pipeline. We bring up consumers and the dashboard first, then start the producer last so no messages are dropped."

**Terminal 1:**
```bash
docker compose -f phase5/docker-compose.yml -f phase5/docker-compose.demo.yml \
  up -d --build kafka postgres db-init phase2-flink phase3-fastapi phase4-service phase4-grid-stress dashboard
```

Wait for the XGBoost model to finish training (watch logs):
```bash
docker logs -f ev-phase3-fastapi
```

> **Speaker notes:** "Phase 3 trains the XGBoost model from the LOA dataset on first start — MAE of 0.068 kWh. The model is then cached in a Docker volume so subsequent starts are instant."

Wait until you see:
```
Background Kafka consumer thread started.
```

Then press `Ctrl+C` to stop following logs.

---

## Step 2 — Show the Dashboard (pre-data)

> **Speaker notes:** "Here's the live dashboard — it's consuming Kafka and PostgreSQL in real time. Right now it's waiting for the first predictions. Once the producer starts, you'll see the heatmap, prices, and alerts populate live."

Open `http://localhost:8501` in the browser. Show the audience the empty state — "Waiting for predictions…" confirms the pipeline is wired up and listening.

---

## Step 3 — Show the FastAPI Inference Endpoint

> **Speaker notes:** "Phase 3 exposes a REST API. The Kafka consumer calls this internally, but you can also hit it directly. Let me show you a manual prediction."

Open `http://localhost:8000/docs` → **POST /predict** → click **Try it out** → paste:

```json
{
  "station_id": "1000604065",
  "time_bin": "2023-06-01 14:00:00",
  "mean_kwh": 0.65,
  "variance_kwh": 0.12,
  "rate_of_change": 0.03,
  "capacity_utilization_ratio": 0.72,
  "hour_of_day": 14,
  "day_of_week": 3,
  "data_completeness": 0.95,
  "anomaly_flag": 0
}
```

Click **Execute**. Expected response:
```json
{
  "predicted_kwh": 0.3599,
  "price_multiplier": 1.05,
  "alert_level": "warning",
  "final_price_cad": 0.126
}
```

> **Speaker notes:** "CUR of 0.72 puts this station in the warning band — linear pricing kicks in between 1× and 2×. Above 0.9 CUR it jumps to 3× critical. Below 0.7 it's normal flat rate of $0.12/kWh."

Pricing table for reference:

| CUR | Alert | Multiplier |
|-----|-------|------------|
| < 0.7 | normal | 1.0× |
| 0.7–0.9 | warning | 1.0×–2.0× (linear) |
| > 0.9 | critical | 3.0× |

---

## Step 4 — Start the Producer (data flows live)

> **Speaker notes:** "Now we fire up the producer. This replays the demo CSV — 4 stations, 500 rows each — at an accelerated rate. Watch the dashboard."

**Terminal 2:**
```bash
docker compose -f phase5/docker-compose.yml -f phase5/docker-compose.demo.yml up phase1-producer
```

Switch to the dashboard tab immediately. Within 30–60 seconds you'll see:
- The **heatmap** filling in with predicted load per station per 15-min window
- **Price multiplier** panels color-coded (green = normal, yellow = warning, red = critical)
- The **Grid Stress Indicator** rising as more windows close
- The **Alerts feed** showing station-level and regional alerts

> **Speaker notes:** "The dashboard polls Kafka and PostgreSQL every 6 seconds. The E2E latency metric in the top-right measures the time from when Phase 3 stamped the Kafka message to when the dashboard received it — that's the FastAPI-to-dashboard hop."

---

## Step 5 — Query PostgreSQL Directly

> **Speaker notes:** "The results are also persisted in TimescaleDB. Let me show the raw rows."

Open a third terminal:
```bash
docker exec -it postgres psql -U evuser -d evdb -c \
  "SELECT station_id, alert_level, price_multiplier, predicted_kwh, created_at FROM inference_results ORDER BY created_at DESC LIMIT 10;"
```

Then show grid stress (appears after the first 15-min window closes):
```bash
docker exec -it postgres psql -U evuser -d evdb -c \
  "SELECT * FROM grid_stress ORDER BY created_at DESC LIMIT 5;"
```

> **Speaker notes:** "Grid stress is a regional aggregate — it averages CUR across all active stations every 15 minutes. If that average exceeds 80%, a regional alert fires to the `alerts` Kafka topic, which you can see reflected in the dashboard feed."

original_kwh vs corrected_kwh — what the raw sensor sent vs. what was used
reason — why it was flagged (e.g. "negative value", "spike detected")
raw_payload — the full original JSON message, stored as JSONB for debugging

---

## Step 6 — Show Kafka Topics

> **Speaker notes:** "Five Kafka topics wire everything together. Let me show them."

```bash
$ docker exec ev-kafka //opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected output:
```
alerts
data-quality
dynamic-prices
ev-telemetry
features
```

> **Speaker notes:** "Data-quality is for anomaly events — Phase 2 flags readings more than 3 standard deviations from the rolling station mean and routes them here instead of into the feature pipeline."

---

## Step 7 — Wrap-Up

> **Speaker notes:** "To recap: a single `docker compose up` command brings up the entire pipeline — Kafka, Flink, XGBoost, PostgreSQL, and a live dashboard. The model achieves a MAE of 0.068 kWh on 15-minute load prediction, dynamic pricing responds to real-time capacity utilization, and regional grid stress alerts fire automatically. The full LOA dataset is ~53 million rows across 1,000+ Ontario stations."

Key numbers to call out:
- **MAE:** 0.068 kWh at 15-min horizon
- **RMSE:** 0.303 kWh
- **Accuracy degrades:** 34% at 30 min, 59% at 45 min → 15 min is the optimal alert window
- **Grid stress threshold:** 80% regional CUR → regional alert
- **Dataset:** ~53.8M rows, Jan 2023, Ontario EVSE network

---

## Quick Reset (between rehearsal runs — no full teardown)

> **Use this instead of `down -v`** when you just want to re-run the demo data without restarting Kafka, Postgres, FastAPI, or the dashboard.

Two things actually need resetting between runs: the Postgres rows, and Flink's in-memory watermark (the cause of the "late data silently dropped" issue — see Troubleshooting below).

```bash
# 1. Clear out previous demo run's rows
docker exec ev-postgres psql -U evuser -d evdb -c "TRUNCATE inference_results, grid_stress, data_quality_events RESTART IDENTITY;"

# 2. Restart just the Flink container to reset its watermark
docker compose -f phase5/docker-compose.yml restart phase2-flink
```

Everything else stays up. Once `phase2-flink` is back (check `docker logs -f ev-phase2-flink` for the consumer to reattach), re-run the producer (Step 4) for another take.

> Leftover messages from the previous run sitting in `dynamic-prices`/`alerts`/`features` are harmless — each consumer group has already advanced past them, so they won't be reprocessed or show up again on the dashboard.

---

## Teardown (after demo)

```bash
docker compose -f phase5/docker-compose.yml -f phase5/docker-compose.demo.yml down -v
```

> `-v` wipes volumes (Kafka offsets, Postgres data, cached model) so the next run starts completely clean.

---

## Troubleshooting Quick Reference

| Symptom | Fix |
|---------|-----|
| Dashboard stuck on "Waiting for predictions…" | Check `docker logs ev-phase3-fastapi` — model may still be training |
| Phase 2 container restarting | Docker RAM < 6 GB — bump memory in Docker Desktop settings |
| No rows in `inference_results` | Check `docker logs ev-phase4-service` for consumer errors |
| Flink drops all messages silently | Watermark already advanced — run `down -v` and restart clean |
| `features` topic missing | `kafka-init` service didn't finish — wait 30–40 s after `up` |
