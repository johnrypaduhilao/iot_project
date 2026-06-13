import json
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "data-quality"

def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed:", err)
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP
    }
    producer = Producer(producer_conf)

    dummy = {
        "station_id": "1000000001",
        "time_bin": "2023-06-01 14:00:00",
        "original_kwh": 25.0,
        "corrected_kwh": 10.5,
        "reason": "zscore>3"
    }

    data_str = json.dumps(dummy)
    producer.produce(
        TOPIC,
        key=dummy["station_id"].encode("utf-8"),
        value=data_str.encode("utf-8"),
        callback=delivery_report
    )

    producer.flush()
    print("Test data-quality message sent.")

if __name__ == "__main__":
    main()
