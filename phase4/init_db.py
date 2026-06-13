import psycopg2

# PostgreSQL / TimescaleDB connection settings
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "evdb"
DB_USER = "evuser"
DB_PASSWORD = "evpass"

def main():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Table 1: inference_results
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inference_results (
            id              SERIAL PRIMARY KEY,
            station_id      VARCHAR(20),
            time_bin        TIMESTAMP,
            predicted_kwh   FLOAT,
            price_multiplier FLOAT,
            alert_level     VARCHAR(10),
            created_at      TIMESTAMP DEFAULT NOW()
        );
    """)

    # Table 2: grid_stress
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

    # Table 3: data_quality_events
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
    
    print("Tables created (or already exist).")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
