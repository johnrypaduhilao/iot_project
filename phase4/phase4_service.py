import json
import psycopg2
from confluent_kafka import Consumer, Producer

DB_HOST = "localhost"
DB_PORT = 5900
DB_NAME = "evdb"
DB_USER = "evuser"
DB_PASSWORD = "evpass"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_GROUP_ID = "phase4-service"
DYNAMIC_PRICES_TOPIC = "dynamic-prices"
DATA_QUALITY_TOPIC = "data-quality"
ALERTS_TOPIC = "alerts"


def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = True
    return conn, conn.cursor()


def create_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([DYNAMIC_PRICES_TOPIC, DATA_QUALITY_TOPIC])
    return consumer


def create_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def send_station_alert(producer, data):
    station_id = data.get("station_id")
    time_bin = data.get("time_bin")
    alert_msg = {
        "type": "station",
        "station_id": station_id,
        "time_bin": time_bin,
        "alert_level": data.get("alert_level"),
        "predicted_kwh": data.get("predicted_kwh"),
        "price_multiplier": data.get("price_multiplier"),
        "dynamic_price_cad": data.get("dynamic_price_cad"),
        "guaranteed_price_cad": data.get("guaranteed_price_cad"),
    }
    producer.produce(
        ALERTS_TOPIC,
        key=(station_id or "unknown").encode("utf-8"),
        value=json.dumps(alert_msg).encode("utf-8"),
    )
    producer.flush()
    print(f"[ALERT] Station alert sent for {station_id} at {time_bin}")


def handle_dynamic_price(cur, producer, data):
    station_id           = data.get("station_id")
    time_bin             = data.get("time_bin")
    predicted_kwh        = data.get("predicted_kwh")
    price_multiplier     = data.get("price_multiplier")
    dynamic_price_cad    = data.get("dynamic_price_cad")
    guaranteed_price_cad = data.get("guaranteed_price_cad")
    day_ahead_price_cad  = data.get("day_ahead_price_cad")
    session_status       = data.get("session_status")
    alert_level          = data.get("alert_level")

    if station_id is None or time_bin is None:
        print("Skipping — missing station_id or time_bin:", data)
        return

    cur.execute(
        """
        INSERT INTO inference_results
        (station_id, time_bin, predicted_kwh, price_multiplier,
         dynamic_price_cad, guaranteed_price_cad, day_ahead_price_cad,
         session_status, alert_level)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (station_id, time_bin, predicted_kwh, price_multiplier,
         dynamic_price_cad, guaranteed_price_cad, day_ahead_price_cad,
         session_status, alert_level),
    )
    print(
        f"Saved → station={station_id} | "
        f"dynamic=${dynamic_price_cad} | "
        f"guaranteed=${guaranteed_price_cad} | "
        f"session={session_status} | "
        f"alert={alert_level}"
    )

    if alert_level == "critical":
        send_station_alert(producer, data)


def handle_data_quality(cur, data):
    station_id    = data.get("station_id")
    time_bin      = data.get("time_bin")
    original_kwh  = data.get("original_kwh")
    corrected_kwh = data.get("corrected_kwh")
    reason        = data.get("reason", "anomaly_detected")

    cur.execute(
        """
        INSERT INTO data_quality_events
        (station_id, time_bin, original_kwh, corrected_kwh, reason, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (station_id, time_bin, original_kwh, corrected_kwh,
         reason, json.dumps(data)),
    )
    print(f"[DATA-QUALITY] station={station_id} reason={reason}")


def main():
    conn, cur = get_db_connection()
    print("Connected to PostgreSQL.")
    consumer = create_consumer()
    producer = create_producer()
    print(f"Listening on: {DYNAMIC_PRICES_TOPIC}, {DATA_QUALITY_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Kafka error:", msg.error())
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print("JSON parse error:", e)
                continue

            topic = msg.topic()
            if topic == DYNAMIC_PRICES_TOPIC:
                handle_dynamic_price(cur, producer, data)
            elif topic == DATA_QUALITY_TOPIC:
                handle_data_quality(cur, data)

    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()
        cur.close()
        conn.close()
        print("Closed all connections.")


if __name__ == "__main__":
    main()