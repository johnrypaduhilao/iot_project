import json
from datetime import datetime

import psycopg2
import time
from confluent_kafka import Producer

# ---------- PostgreSQL / TimescaleDB settings ----------
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "evdb"
DB_USER = "evuser"
DB_PASSWORD = "evpass"

# ---------- Kafka settings ----------
KAFKA_BOOTSTRAP = "localhost:9092"
ALERTS_TOPIC = "alerts"

# capacity_utilization_ratio ~ predicted_kwh / 15.0
DEFAULT_CAPACITY_KWH_PER_15MIN = 15.0

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

def create_producer():
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP
    }
    return Producer(producer_conf)

def send_regional_alert(producer, time_bin, stress_score, station_count):
    alert_msg = {
        "type": "regional",
        "time_bin": time_bin.strftime("%Y-%m-%d %H:%M:%S"),
        "stress_score": stress_score,
        "station_count": station_count,
    }
    payload = json.dumps(alert_msg).encode("utf-8")

    producer.produce(
        ALERTS_TOPIC,
        key=b"regional",
        value=payload,
    )
    producer.flush()
    print(f"[GRID ALERT] Regional alert sent for {time_bin}, stress_score={stress_score:.3f}")

def compute_grid_stress():
    conn, cur = get_db_connection()
    producer = create_producer()

    cur.execute("""
        SELECT time_bin
        FROM inference_results
        ORDER BY time_bin DESC
        LIMIT 1;
    """)
    row = cur.fetchone()
    if not row:
        print("No inference_results rows found yet. Cannot compute grid stress.")
        cur.close()
        conn.close()
        return

    time_bin = row[0]
    print(f"Computing grid stress for time_bin = {time_bin}")

    cur.execute("""
        SELECT predicted_kwh
        FROM inference_results
        WHERE time_bin = %s;
    """, (time_bin,))
    rows = cur.fetchall()
    if not rows:
        print("No rows found for this time_bin in inference_results.")
        cur.close()
        conn.close()
        return

    curs = []
    for (predicted_kwh,) in rows:
        if predicted_kwh is None:
            continue
        cur_ratio = predicted_kwh / DEFAULT_CAPACITY_KWH_PER_15MIN
        curs.append(cur_ratio)

    if not curs:
        print("No valid predicted_kwh values to compute CUR.")
        cur.close()
        conn.close()
        return

    stress_score = sum(curs) / len(curs)
    station_count = len(curs)
    triggered_alert = stress_score > 0.8

    cur.execute("SELECT id FROM grid_stress WHERE time_bin = %s", (time_bin,))
    if cur.fetchone():
        print(f"Grid stress already computed for {time_bin}, skipping.")
        cur.close()
        conn.close()
        return
    
    cur.execute("""
        INSERT INTO grid_stress (time_bin, stress_score, station_count, triggered_alert)
        VALUES (%s, %s, %s, %s);
    """, (time_bin, stress_score, station_count, triggered_alert))

    print(f"[GRID] time_bin={time_bin}, stress_score={stress_score:.3f}, "
          f"station_count={station_count}, triggered_alert={triggered_alert}")

    if triggered_alert:
        send_regional_alert(producer, time_bin, stress_score, station_count)

    cur.close()
    conn.close()
    print("Done computing grid stress.")



if __name__ == "__main__":
    while True:
        compute_grid_stress()
        print("Next run in 15 minutes...")
        time.sleep(900)  # 900 seconds = 15 minutes
