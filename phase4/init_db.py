import psycopg2

DB_HOST = "localhost"
DB_PORT = 5900
DB_NAME = "evdb"
DB_USER = "evuser"
DB_PASSWORD = "evpass"

def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS inference_results (
            id                   SERIAL PRIMARY KEY,
            station_id           VARCHAR(20),
            time_bin             TIMESTAMP,
            predicted_kwh        FLOAT,
            price_multiplier     FLOAT,
            dynamic_price_cad    FLOAT,
            guaranteed_price_cad FLOAT,
            day_ahead_price_cad  FLOAT,
            session_status       VARCHAR(20),
            alert_level          VARCHAR(10),
            created_at           TIMESTAMP DEFAULT NOW()
        );
    """)

    # Migrate existing tables that predate the price columns
    for col, ctype in [
        ("dynamic_price_cad",    "FLOAT"),
        ("guaranteed_price_cad", "FLOAT"),
        ("day_ahead_price_cad",  "FLOAT"),
    ]:
        cur.execute(f"ALTER TABLE inference_results ADD COLUMN IF NOT EXISTS {col} {ctype};")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS grid_stress (
            id              SERIAL PRIMARY KEY,
            time_bin        TIMESTAMP,
            stress_score    FLOAT,
            station_count   INT,
            triggered_alert BOOLEAN,
            created_at      TIMESTAMP DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_events (
            id              SERIAL PRIMARY KEY,
            station_id      VARCHAR(20),
            time_bin        TIMESTAMP,
            original_kwh    FLOAT,
            corrected_kwh   FLOAT,
            reason          VARCHAR(100),
            raw_payload     JSONB,
            created_at      TIMESTAMP DEFAULT NOW()
        );
    """)

    print("Tables created successfully.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()