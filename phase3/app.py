from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np

# Load the trained model
with open('xgboost_model.pkl', 'rb') as f:
    model = pickle.load(f)

app = FastAPI()

# Ontario average base electricity price (CAD per kWh)
BASE_PRICE_CAD = 0.12

# Define the input format (feature vector from Flink)
class FeatureVector(BaseModel):
    station_id: str
    time_bin: str
    mean_kwh: float
    variance_kwh: float
    rate_of_change: float
    capacity_utilization_ratio: float
    hour_of_day: int
    day_of_week: int
    data_completeness: float
    anomaly_flag: int

def compute_price_multiplier(cur: float) -> float:
    """
    Compute dynamic price multiplier based on capacity utilization ratio.
    - Below 0.7: standard price (1.0x)
    - 0.7 to 0.9: linear scale from 1.0x to 2.0x (warning zone)
    - Above 0.9: fixed at 3.0x (critical zone, strong deterrent)
    """
    if cur < 0.7:
        return 1.0
    elif cur <= 0.9:
        return 1.0 + (cur - 0.7) / (0.9 - 0.7) * 1.0
    else:
        return 3.0

def compute_alert_level(cur: float) -> str:
    """
    Determine alert level based on capacity utilization ratio.
    - normal: CUR below 0.7
    - warning: CUR between 0.7 and 0.9
    - critical: CUR above 0.9
    """
    if cur < 0.7:
        return "normal"
    elif cur <= 0.9:
        return "warning"
    else:
        return "critical"

@app.post("/predict")
def predict(features: FeatureVector):
    # Prepare input array for model
    X = np.array([[
        features.mean_kwh,
        features.variance_kwh,
        features.rate_of_change,
        features.capacity_utilization_ratio,
        features.hour_of_day,
        features.day_of_week,
        features.data_completeness,
        features.anomaly_flag
    ]])

    # Run inference
    predicted_kwh = float(model.predict(X)[0])

    # Compute price multiplier and alert level based on current CUR
    price_multiplier = compute_price_multiplier(features.capacity_utilization_ratio)
    alert_level = compute_alert_level(features.capacity_utilization_ratio)

    # Compute final price in CAD
    final_price_cad = round(BASE_PRICE_CAD * price_multiplier, 4)

    return {
        "station_id": features.station_id,
        "time_bin": features.time_bin,
        "predicted_kwh": round(predicted_kwh, 4),
        "price_multiplier": round(price_multiplier, 4),
        "alert_level": alert_level,
        "base_price_cad": BASE_PRICE_CAD,
        "final_price_cad": final_price_cad
    }

@app.get("/health")
def health():
    return {"status": "ok"}