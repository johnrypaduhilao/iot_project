# Phase 3 — XGBoost Model + FastAPI Inference Service

Trains an XGBoost model on the LOA EV charging dataset to predict station load 15 minutes ahead. Serves predictions via a FastAPI microservice that outputs predicted load, dynamic price multiplier, and final charging price in Canadian dollars.

---

## Setup

```bash
cd phase3
pip install -r requirements.txt
```

---

## Step 1 — Train the Model

```bash
python train_model.py
```

Loads LOA data, computes 15-minute window features, trains XGBoost, saves `xgboost_model.pkl`.

Expected output:
```
Training set:   1332916 rows
Validation set: 333230 rows
MAE:  0.0679 kWh
RMSE: 0.3031 kWh
Model saved to xgboost_model.pkl
```

---

## Step 2 — Run the FastAPI Service

```bash
uvicorn app:app --reload
```

Service available at `http://127.0.0.1:8000`
API docs at `http://127.0.0.1:8000/docs`

---

## API

### POST /predict

**Request:**
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

**Response:**
```json
{
  "station_id": "1000604065",
  "time_bin": "2023-06-01 14:00:00",
  "predicted_kwh": 0.3599,
  "price_multiplier": 1.05,
  "alert_level": "warning",
  "base_price_cad": 0.12,
  "final_price_cad": 0.126
}
```

### GET /health
```json
{ "status": "ok" }
```

---

## Pricing Logic

| CUR Range | Alert Level | Price Multiplier |
|---|---|---|
| Below 0.7 | normal | 1.0x |
| 0.7 to 0.9 | warning | 1.0x to 2.0x (linear) |
| Above 0.9 | critical | 3.0x |

```
final_price_cad = 0.12 CAD/kWh × price_multiplier
```

---

## Step 3 — Peak Demand Alert Experiment

```bash
python peak_demand_alert.py
```

Tests prediction accuracy at three horizons to determine optimal alert lead time.

| Horizon | MAE (kWh) | RMSE (kWh) |
|---|---|---|
| 15 min ahead | 0.0679 | 0.3031 |
| 30 min ahead | 0.0911 | 0.3699 |
| 45 min ahead | 0.1082 | 0.4087 |

**Finding:** 15-minute horizon gives the best accuracy. Accuracy degrades 34% at 30 min and 59% at 45 min. 15 minutes is the recommended alert window.