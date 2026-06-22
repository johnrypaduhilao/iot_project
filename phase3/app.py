import threading
import pickle
import json
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaConsumer, KafkaProducer
from typing import Optional

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "features"
OUTPUT_TOPIC = "dynamic-prices"
GROUP_ID = "phase3-inference-group"
BASE_PRICE_CAD = 0.12
GUARANTEE_PREMIUM = 1.1         # Guaranteed price = dynamic price at plug-in × 1.1
LOA_CSV_PATH = "../datasets/LOA-5min/LOA.csv"

# ─── Load XGBoost model ───────────────────────────────────────────────────────
with open("xgboost_model.pkl", "rb") as f:
    model = pickle.load(f)

# ─── Pre-compute day-ahead prices from historical average ─────────────────────
# Loads first 5M rows of LOA data at startup and builds a lookup table:
# (station_id, hour_of_day, day_of_week) → historical average kWh
# This is used as the "tomorrow's estimated price" shown to users before they plug in.
print("Pre-computing day-ahead price lookup table from historical data...")
_df = pd.read_csv(LOA_CSV_PATH, nrows=5000000)
_df["time_new"] = pd.to_datetime(_df["time_new"])
_df["hour_of_day"] = _df["time_new"].dt.hour
_df["day_of_week"] = _df["time_new"].dt.dayofweek
# Adaptive capacity baseline: 95th percentile of station-level mean kWh
_station_means = _df.groupby('station_id')['kwh'].mean()
RATED_POWER_KW = float(_station_means.quantile(0.95))
print(f"Adaptive rated power baseline: {RATED_POWER_KW:.4f} kWh")
_hist = (
    _df.groupby(["station_id", "hour_of_day", "day_of_week"])["kwh"]
    .mean()
    .reset_index()
    .rename(columns={"kwh": "hist_avg_kwh"})
)
DAY_AHEAD_LOOKUP = {
    (str(row.station_id), int(row.hour_of_day), int(row.day_of_week)): float(row.hist_avg_kwh)
    for _, row in _hist.iterrows()
}
print(f"Day-ahead lookup ready: {len(DAY_AHEAD_LOOKUP)} entries.")
del _df, _hist  # free memory

# ─── Session state (guaranteed price lock) ────────────────────────────────────
# Tracks whether each station is currently active and what price was locked in.
# Keyed by station_id.
# Structure: {station_id: {"is_active": bool, "locked_price": float | None}}
station_session_state: dict = {}

# ─── Pydantic input schema ────────────────────────────────────────────────────
class FeatureVector(BaseModel):
    station_id: str
    time_bin: str
    mean_kwh: float
    variance_kwh: float
    rate_of_change: float
    capacity_utilization_ratio: float
    hour_of_day: int
    day_of_week: int
    anomaly_flag: int
    data_completeness: float

# ─── Pricing helpers ──────────────────────────────────────────────────────────
def compute_price_multiplier(cur: float) -> float:
    """
    CUR < 0.7  → 1.0x  (normal, standard price)
    CUR 0.7–0.9 → linear scale 1.0x to 2.0x  (warning zone)
    CUR > 0.9  → 3.0x  (critical, strong deterrent)
    """
    if cur < 0.7:
        return 1.0
    elif cur <= 0.9:
        return 1.0 + (cur - 0.7) / (0.9 - 0.7) * 1.0
    else:
        return 3.0

def compute_alert_level(cur: float) -> str:
    if cur >= 0.9:
        return "critical"
    elif cur >= 0.7:
        return "warning"
    return "normal"

def compute_day_ahead_price(station_id: str, hour: int, dow: int) -> float:
    """
    Look up the historical average kWh for this station/hour/day-of-week,
    then convert that to a price using the same multiplier logic.
    Falls back to BASE_PRICE_CAD if no history exists for this combination.
    """
    avg_kwh = DAY_AHEAD_LOOKUP.get((station_id, hour, dow), None)
    if avg_kwh is None:
        return round(BASE_PRICE_CAD, 4)
    avg_cur = avg_kwh / RATED_POWER_KW
    multiplier = compute_price_multiplier(avg_cur)
    return round(BASE_PRICE_CAD * multiplier, 4)

def compute_guaranteed_price(
    station_id: str,
    cur: float,
    dynamic_final_price: float
) -> tuple[float, str]:
    """
    Session state machine:
    - idle → active:  lock guaranteed price = dynamic price at plug-in × 1.1
    - active → active: keep locked price, return session_ongoing
    - active → idle:  clear lock, return idle
    Returns (guaranteed_price, session_status).
    """
    prev = station_session_state.get(
        station_id, {"is_active": False, "locked_price": None}
    )
    is_active = cur > 0

    if is_active and not prev["is_active"]:
        locked = round(dynamic_final_price * GUARANTEE_PREMIUM, 4)
        station_session_state[station_id] = {"is_active": True, "locked_price": locked}
        return locked, "session_started"

    elif is_active and prev["is_active"]:
        return prev["locked_price"], "session_ongoing"

    else:
        station_session_state[station_id] = {"is_active": False, "locked_price": None}
        return round(BASE_PRICE_CAD * GUARANTEE_PREMIUM, 4), "idle"

# ─── Core inference ───────────────────────────────────────────────────────────
FEATURE_COLS = [
    "mean_kwh", "variance_kwh", "rate_of_change",
    "capacity_utilization_ratio", "hour_of_day", "day_of_week",
    "data_completeness", "anomaly_flag",
]

def run_inference(features: dict) -> dict:
    X = [[features[c] for c in FEATURE_COLS]]
    predicted_kwh = float(model.predict(X)[0])

    cur = features["capacity_utilization_ratio"]
    multiplier = compute_price_multiplier(cur)
    alert_level = compute_alert_level(cur)
    dynamic_final_price = round(BASE_PRICE_CAD * multiplier, 4)

    station_id = str(features["station_id"])
    hour = int(features["hour_of_day"])
    dow = int(features["day_of_week"])

    guaranteed_price, session_status = compute_guaranteed_price(
        station_id, cur, dynamic_final_price
    )
    day_ahead_price = compute_day_ahead_price(station_id, hour, dow)

    return {
        "station_id": station_id,
        "time_bin": features["time_bin"],
        "predicted_kwh": round(predicted_kwh, 4),
        # Dynamic pricing (real-time, changes every window)
        "price_multiplier": round(multiplier, 4),
        "base_price_cad": BASE_PRICE_CAD,
        "dynamic_price_cad": dynamic_final_price,
        # Guaranteed pricing (locked at plug-in moment, stable for entire session)
        "guaranteed_price_cad": guaranteed_price,
        "session_status": session_status,
        # Day-ahead pricing (historical average, shown to users before they plug in)
        "day_ahead_price_cad": day_ahead_price,
        # Alert
        "alert_level": alert_level,
    }

# ─── Kafka integration ────────────────────────────────────────────────────────
def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def kafka_consumer_loop():
    producer = get_producer()
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m is not None else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    for message in consumer:
        try:
            features = message.value
            if features is None:
                # Tombstone/retraction record from the compacted upsert-kafka
                # "features" topic (Phase 2's self-join can retract a previous
                # row before re-emitting it) — nothing to infer on, skip it.
                continue
            result = run_inference(features)
            producer.send(OUTPUT_TOPIC, value=result)
            print(
                f"[Inference] station={result['station_id']} "
                f"alert={result['alert_level']} "
                f"dynamic=${result['dynamic_price_cad']} "
                f"guaranteed=${result['guaranteed_price_cad']} "
                f"day_ahead=${result['day_ahead_price_cad']} "
                f"session={result['session_status']}"
            )
        except Exception as e:
            print(f"Error processing message: {e}")

# ─── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI()

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    thread.start()
    print("Background Kafka consumer thread started.")

@app.post("/predict")
def predict(features: FeatureVector):
    """Manual POST endpoint for testing without Kafka."""
    result = run_inference(features.dict())
    return result

@app.get("/health")
def health():
    return {"status": "ok"}