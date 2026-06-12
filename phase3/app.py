from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np
import json
import threading
from kafka import KafkaConsumer, KafkaProducer

# Load the trained model
with open('xgboost_model.pkl', 'rb') as f:
    model = pickle.load(f)

app = FastAPI()

# Ontario average base electricity price (CAD per kWh)
BASE_PRICE_CAD = 0.12

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'features'
OUTPUT_TOPIC = 'dynamic-prices'

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
    if cur < 0.7:
        return 1.0
    elif cur <= 0.9:
        return 1.0 + (cur - 0.7) / (0.9 - 0.7) * 1.0
    else:
        return 3.0

def compute_alert_level(cur: float) -> str:
    if cur < 0.7:
        return "normal"
    elif cur <= 0.9:
        return "warning"
    else:
        return "critical"

def run_inference(features: dict) -> dict:
    # Prepare input array for model
    X = np.array([[
        features['mean_kwh'],
        features['variance_kwh'],
        features['rate_of_change'],
        features['capacity_utilization_ratio'],
        features['hour_of_day'],
        features['day_of_week'],
        features['data_completeness'],
        features['anomaly_flag']
    ]])

    predicted_kwh = float(model.predict(X)[0])
    price_multiplier = compute_price_multiplier(features['capacity_utilization_ratio'])
    alert_level = compute_alert_level(features['capacity_utilization_ratio'])
    final_price_cad = round(BASE_PRICE_CAD * price_multiplier, 4)

    return {
        "station_id": features['station_id'],
        "time_bin": features['time_bin'],
        "predicted_kwh": round(predicted_kwh, 4),
        "price_multiplier": round(price_multiplier, 4),
        "alert_level": alert_level,
        "base_price_cad": BASE_PRICE_CAD,
        "final_price_cad": final_price_cad
    }

def kafka_consumer_loop():
    """
    Background thread: continuously reads feature vectors from
    the 'features' Kafka topic, runs inference, and publishes
    results to the 'dynamic-prices' topic.
    """
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='phase3-inference-group'
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    print(f"Kafka consumer started. Listening on topic: {INPUT_TOPIC}")

    for message in consumer:
        try:
            features = message.value
            result = run_inference(features)
            producer.send(OUTPUT_TOPIC, value=result)
            print(f"Inference done for station {result['station_id']} -> alert: {result['alert_level']}, price: {result['final_price_cad']} CAD")
        except Exception as e:
            print(f"Error processing message: {e}")

# Start Kafka consumer in background thread when app starts
@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    thread.start()
    print("Background Kafka consumer thread started.")

# Manual POST endpoint for testing
@app.post("/predict")
def predict(features: FeatureVector):
    result = run_inference(features.dict())
    return result

@app.get("/health")
def health():
    return {"status": "ok"}