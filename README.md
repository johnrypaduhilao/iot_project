# Real-Time EV Charging Load Forecasting and Dynamic Pricing Engine (ENGR 5785G)

**Team:** Algorithm Army  
**Members:** Johnry · Deepta · Helen · Zahid · Camilo

Real-time streaming pipeline that replays Los Angeles EV charging station data, engineers 15-minute load features using Apache Flink, runs XGBoost inference to predict station load and set dynamic prices, persists results to PostgreSQL/TimescaleDB, and displays everything on a live Streamlit dashboard.

```
LOA.csv → [Phase 1] Kafka producer (ev-telemetry)
              ↓
         [Phase 2] PyFlink — 15-min feature vectors (features, data-quality)
              ↓
         [Phase 3] FastAPI + XGBoost — load prediction + dynamic pricing (dynamic-prices)
              ↓
         [Phase 4] PostgreSQL writer + grid stress alerting (alerts)
              ↓
         [Phase 5] Streamlit dashboard — live Kafka + DB reads
```

---

## Dataset

**Los Angeles EV Charging Load (LOA) dataset** — 5-minute charging records from public Level 2 EVSE stations across Los Angeles.

Download `LOA-5min.zip`, extract, and place `LOA.csv` at `datasets/LOA-5min/LOA.csv` before running anything:

> Zenodo: https://zenodo.org/records/15814263?preview_file=LOA-5min.zip

The file is large and is excluded from the repository.

---

## Prerequisites

- **Docker Desktop** — runs Kafka, PostgreSQL/TimescaleDB, and all containerized services
- **Python 3.11** — Phase 2 requires exactly 3.11 (PyFlink constraint); other phases work with 3.11+
- **Java 11+** — needed by PyFlink at runtime
- The LOA dataset at `datasets/LOA-5min/LOA.csv`

---

## Running the Full Pipeline

The recommended way to run everything is through Phase 5's unified Docker Compose, which wires all services onto one network. See `phase5/README.md` for the single-command setup.

If you want to run phases individually (e.g. for development), use the six-terminal sequence below.

### Step 1 — Infrastructure

```bash
docker compose up -d
```

Starts Kafka and PostgreSQL. The `kafka-init` container creates all five topics (`ev-telemetry`, `features`, `dynamic-prices`, `data-quality`, `alerts`) and exits. Wait 30–40 seconds for Kafka to finish initializing.

Verify topics:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Create database tables (once per environment):

```bash
cd phase4
pip install psycopg2-binary confluent-kafka
python init_db.py
cd ..
```

### Step 2 — Phase 4 consumer

```bash
cd phase4
python phase4_service.py
```

Listens to `dynamic-prices` and `data-quality`, writes to PostgreSQL, forwards critical station alerts to `alerts`.

### Step 3 — Phase 4 grid stress job

```bash
cd phase4
python grid_stress_job.py
```

Runs every 15 minutes. Averages CUR across all active stations and sends a regional alert to `alerts` if the mean exceeds 0.8.

### Step 4 — Phase 3 FastAPI inference service

```bash
cd phase3
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

Wait for `Background Kafka consumer thread started.` before continuing. The service subscribes to `features`, runs XGBoost inference on each vector, and publishes results to `dynamic-prices`.

### Step 5 — Phase 2 Flink feature engineering

```bash
cd phase2
py -3.11 -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/Mac
pip install -r requirements.txt
python flink_feature_engineering.py
```

Subscribes to `ev-telemetry`, groups records into 15-minute tumbling windows per station, computes feature vectors, and publishes to `features`. Readings more than 3 standard deviations from a station's rolling mean are flagged to `data-quality` instead.

### Step 6 — Phase 1 data producer

```bash
cd phase1
pip install -r requirements.txt
python producer.py --max-rows 5000 --speed 0
```

Replays LOA records into `ev-telemetry` — one thread per station, messages in time order. `--speed 0` sends as fast as possible; drop `--max-rows` to run the full dataset (~53.8M rows).

---

## Verifying the Pipeline

Once Phase 1 is producing, check that results are landing in the database:

```bash
docker exec -it postgres psql -U evuser -d evdb \
  -c "SELECT station_id, alert_level, dynamic_price_cad FROM inference_results ORDER BY created_at DESC LIMIT 10;"

docker exec -it postgres psql -U evuser -d evdb \
  -c "SELECT * FROM grid_stress ORDER BY created_at DESC LIMIT 5;"
```

Phase 3 logs one line per inference:
```
[Inference] station=1000604065 alert=warning dynamic=$0.126 guaranteed=$0.138 ...
```

Phase 4 logs one line per database insert:
```
Saved → station=1000604065 | dynamic=$0.126 | guaranteed=$0.138 | session=session_ongoing | alert=warning
```

---

## Project Structure

```
.
├── datasets/LOA-5min/      # LOA.csv goes here (not committed)
├── phase1/                 # Kafka producer (Johnry)
├── phase2/                 # PyFlink feature engineering (Deepta)
├── phase3/                 # XGBoost model + FastAPI (Helen)
├── phase4/                 # PostgreSQL writer + alerting (Zahid)
├── phase5/                 # Docker Compose + Streamlit dashboard (Camilo)
├── Evaluation/RQ1/         # RQ1: Grid Stress Indicator vs. isolation (Johnry)
│   └── run_evaluation.py   # offline harness — does not modify phases 1–5
├── docker-compose.yml      # Shared Kafka + PostgreSQL (development)
└── DEMO.md                 # Live demo script
```

---

## Dependencies

Each phase has its own `requirements.txt`. Install them inside the relevant phase directory.

| Phase | Key dependencies |
|-------|-----------------|
| 1 | `confluent-kafka`, `pandas` |
| 2 | `apache-flink==2.0.0`, Python 3.11 required |
| 3 | `fastapi`, `uvicorn`, `xgboost`, `scikit-learn`, `kafka-python` |
| 4 | `psycopg2-binary`, `confluent-kafka` |
| 5 | `streamlit`, `plotly`, `psycopg2-binary`, `confluent-kafka` |

For Phase 2, create an isolated 3.11 virtual environment before installing:

```bash
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

---

## Team Contributions

**Johnry — Phase 1 + Evaluation RQ1**  
Kafka data producer. Streams the LOA CSV into the `ev-telemetry` topic, one thread per station, in chronological order. Supports configurable replay speed, row cap, and station cap via CLI flags. Handles OOM on large datasets by streaming in chunks.

Also ran the offline evaluation for RQ1 (Grid Stress Indicator). The research question is whether multi-station aggregation (mean CUR across stations, alert if mean > 0.8) gives earlier and more accurate regional overload warnings than monitoring stations in isolation. The evaluation (`Evaluation/RQ1/`) sweeps four detector variants — AGG-CURRENT, AGG-FORECAST, IND-CURRENT, IND-FORECAST — across 25 randomised scenarios and reports precision, recall, false-alarm rate, and advance warning time at a fixed false-alarm budget. Because the real LOA trace has no overload events (peak regional stress reaches only 0.037 with the default 15 kWh/15-min capacity), coordinated demand events are constructed on top of the real diurnal load shape to make the comparison meaningful.

**Deepta — Phase 2**  
PyFlink feature engineering. Reads `ev-telemetry`, computes 15-minute tumbling-window aggregates per station (mean kWh, variance, rate of change, capacity utilisation ratio, data completeness), and detects anomalies via 3-sigma rolling statistics. Clean records go to `features`; anomalous ones go to `data-quality`.

**Helen — Phase 3 + Evaluation RQ2**  
XGBoost inference service. Trains a regression model on the LOA dataset (MAE 0.068 kWh at 15-min horizon) and serves it via FastAPI. Implements three pricing tiers: real-time dynamic pricing, a session-locked guaranteed price set at plug-in, and a day-ahead price estimated from historical averages. Publishes results to `dynamic-prices`.

Also ran RQ2 (Peak Demand Alert horizon study) via `phase3/peak_demand_alert.py`. The experiment retrains the same XGBoost model at three prediction horizons to find the optimal alert lead time:

| Horizon | MAE (kWh) | RMSE (kWh) | Accuracy loss vs. 15-min |
|---------|-----------|------------|--------------------------|
| 15 min  | 0.0679    | 0.3031     | baseline                 |
| 30 min  | 0.0911    | 0.3699     | −34%                     |
| 45 min  | 0.1082    | 0.4087     | −59%                     |

Finding: 15 minutes is the recommended alert window. Accuracy degrades sharply at longer horizons, so the pipeline issues alerts one window (15 min) ahead.

**Zahid — Phase 4**  
PostgreSQL persistence and alerting. Consumes `dynamic-prices` and `data-quality` and writes to three TimescaleDB tables (`inference_results`, `grid_stress`, `data_quality_events`). Forwards critical station alerts to `alerts`. A separate 15-minute cron job computes the regional grid stress score and triggers a regional alert when the mean CUR exceeds 0.8.

**Camilo — Phase 5 + System Evaluation**  
Unified Docker Compose stack and live Streamlit dashboard. All five phases run on a single bridge network with one compose command. The dashboard polls Kafka and PostgreSQL every 6 seconds and shows a per-station load heatmap, price multiplier bars (colour-coded by alert level), a grid stress trend line, a live alerts feed, and end-to-end latency from Phase 3 inference to dashboard receipt.

Also responsible for the system evaluation — Experimental Results under Normal Behavior. Ran the full end-to-end pipeline on the demo slice (4 stations × 500 rows) and recorded key system metrics: messages flow correctly through all five Kafka topics, the dashboard updates live with no missed windows, and the FastAPI-to-dashboard latency stays well under 1 second under normal load. The demo compose override (`phase5/docker-compose.demo.yml`) is the reproducible setup used for this evaluation.

---

## Teardown

```bash
docker compose down -v
```

`-v` removes volumes, clearing Kafka offsets, Postgres data, and the cached XGBoost model so the next run starts clean.
