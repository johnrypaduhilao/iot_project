import json
import psycopg2
from confluent_kafka import Consumer, Producer

# ---------- PostgreSQL / TimescaleDB settings ----------
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "evdb"
DB_USER = "evuser"
DB_PASSWORD = "evpass"

# ---------- Kafka settings ----------
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_GROUP_ID = "phase4-service"  # consumer group name

DYNAMIC_PRICES_TOPIC = "dynamic-prices"  # Phase 3 outputs here
DATA_QUALITY_TOPIC = "data-quality"      # Phase 2 anomaly events
ALERTS_TOPIC = "alerts"                  # Phase 4 publishes alerts here


# ---------- DB helpers ----------

def get_db_connection():
conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True
    return conn, conn.cursor()


# ---------- Kafka helpers ----------

def create_consumer():
consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([DYNAMIC_PRICES_TOPIC, DATA_QUALITY_TOPIC])
    return consumer


def create_producer():
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP
    }
    return Producer(producer_conf)


# ---------- Handlers ----------

def send_station_alert(producer, data):
    station_id = data.get("station_id")
    time_bin = data.get("time_bin")
    predicted_kwh = data.get("predicted_kwh")
    price_multiplier = data.get("price_multiplier")
    alert_level = data.get("alert_level")

    alert_msg = {
        "type": "station",
        "station_id": station_id,
        "time_bin": time_bin,
        "alert_level": alert_level,
        "predicted_kwh": predicted_kwh,
        "price_multiplier": price_multiplier,
    }

    payload = json.dumps(alert_msg).encode("utf-8")
    key = (station_id or "unknown").encode("utf-8")

    producer.produce(
        ALERTS_TOPIC,
        key=key,
        value=payload,
    )
    producer.flush()
    print(f"[ALERT] Station-level alert sent for {station_id} at {time_bin}")


def handle_dynamic_price(cur, producer, data):
    station_id = data.get("station_id")
    time_bin = data.get("time_bin")
    predicted_kwh = data.get("predicted_kwh")
    price_multiplier = data.get("price_multiplier")
    alert_level = data.get("alert_level")

    if station_id is None or time_bin is None:
        print("Skipping message, missing station_id or time_bin:", data)
        return

    cur.execute(
        """
        INSERT INTO inference_results
        (station_id, time_bin, predicted_kwh, price_multiplier, alert_level)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (station_id, time_bin, predicted_kwh, price_multiplier, alert_level),
    )
    print(f"Inserted inference_result for station {station_id} at {time_bin}")
    if alert_level == "critical":
        send_station_alert(producer, data)


def handle_data_quality(cur, data):
    station_id = data.get("station_id")
    time_bin = data.get("time_bin")
    original_kwh = data.get("original_kwh")
    corrected_kwh = data.get("corrected_kwh")
    reason = data.get("reason", "anomaly_detected")

    raw_payload = json.dumps(data)

    cur.execute(
        """
        INSERT INTO data_quality_events
        (station_id, time_bin, original_kwh, corrected_kwh, reason, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (station_id, time_bin, original_kwh, corrected_kwh, reason, raw_payload)
    )

    print("[DATA-QUALITY] Logged anomaly event for station",
          station_id, "time_bin", time_bin, "reason", reason)


# ---------- Main loop ----------

def main():
    # DB connection
    conn, cur = get_db_connection()
    print("Connected to PostgreSQL.")

    # Kafka consumer + producer
    consumer = create_consumer()
    producer = create_producer()
    print(f"Subscribed to topics: {DYNAMIC_PRICES_TOPIC}, {DATA_QUALITY_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Kafka consumer error:", msg.error())
                continue

            topic = msg.topic()

            try:
                data_str = msg.value().decode("utf-8")
                data = json.loads(data_str)
            except Exception as e:
                print("Failed to parse message as JSON:", e, msg.value())
                continue

            if topic == DYNAMIC_PRICES_TOPIC:
                handle_dynamic_price(cur, producer, data)
            elif topic == DATA_QUALITY_TOPIC:
                handle_data_quality(cur, data)
            else:
                print(f"Received message on unexpected topic {topic}: {data}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        cur.close()
        conn.close()
        print("Closed DB connection and Kafka consumer.")

if __name__ == "__main__":
    main()
