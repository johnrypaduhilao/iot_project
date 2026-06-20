import json
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "dynamic-prices"

def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    dummy = {
        "station_id": "1000000001",
        "time_bin": "2023-06-01 14:00:00",
        "predicted_kwh": 20.0,
        "price_multiplier": 3.0,
        "base_price_cad": 0.12,
        "dynamic_price_cad": 0.36,
        "guaranteed_price_cad": 0.396,
        "session_status": "session_started",
        "day_ahead_price_cad": 0.12,
        "alert_level": "critical",
    }

    def delivery_report(err, msg):
        if err:
            print("Failed:", err)
        else:
            print(f"Sent to {msg.topic()} at offset {msg.offset()}")

    producer.produce(
        TOPIC,
        key=dummy["station_id"].encode("utf-8"),
        value=json.dumps(dummy).encode("utf-8"),
        callback=delivery_report,
    )
    producer.flush()
    print("Test message sent.")

if __name__ == "__main__":
    main()