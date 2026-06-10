# Phase 4 - Database and Alerting

## Overview
The main goal of Phase 4 was to take the predictions from Phase 3 and store them in a PostgreSQL 
database. It also handles the alerting system for both individual  stations and the overall grid.

## Files

| File | Description |
|------|-------------|
| `phase4_service.py` | Main service that listens to Kafka and saves data into the database |
| `grid_stress_job.py` | Runs every 15 minutes and computes the regional grid stress score |
| `init_db.py` | Creates all three database tables |
| `docker-compose.yml` | Sets up Kafka and PostgreSQL (TimescaleDB) in Docker |
| `test_dynamic_producer.py` | Test script to send a dummy message to the dynamic-prices topic |
| `test_data_quality_producer.py` | Test script to send a dummy message to the data-quality topic |
| `check_db.py` | Verifies data in the inference_results table |
| `check_data_quality.py` | Verifies data in the data_quality_events table |
| `check_grid_stress.py` | Verifies data in the grid_stress table |

## Database Tables

**inference_results** - stores every prediction the model makes, one row per station per 15 minute window.

**data_quality_events** - stores anomaly events that come from  Phase 2 when a bad sensor reading is detected.

**grid_stress** - stores the regional stress score that gets  computed every 15 minutes across all active stations.

## How to Run

**Step 1** - Start Docker:
```bash
docker compose up -d
```

**Step 2** - Create the database tables:
```bash
python init_db.py
```

**Step 3** - Start the main service:
```bash
python phase4_service.py
```

**Step 4** - Start the grid stress job in a new terminal:
```bash
python grid_stress_job.py
```

## How to Verify and ## Testing 


To test the pipeline, the test producer scripts can be used to
send dummy messages to Kafka. After running them, the data can
be verified directly in PostgreSQL by querying the
inference_results, data_quality_events, and grid_stress tables
to confirm everything is being saved and processed correctly.
After running the test producers, use the check scripts to  confirm data is being saved correctly:

```bash
python test_dynamic_producer.py
python test_data_quality_producer.py
```

## Alerting Logic

- If a station has `alert_level == critical`, a station alert
is sent to the `alerts` Kafka topic automatically.
- Every 15 minutes the grid stress score is calculated. If the
average capacity utilization across all stations goes above 0.8,
a regional alert is sent to the `alerts` topic.
