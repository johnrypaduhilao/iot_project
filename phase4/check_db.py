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
    cur = conn.cursor()
    cur.execute("""
        SELECT id, station_id, time_bin, predicted_kwh,
               dynamic_price_cad, guaranteed_price_cad,
               day_ahead_price_cad, session_status, alert_level
        FROM inference_results
        ORDER BY id DESC
        LIMIT 10;
    """)
    rows = cur.fetchall()
    print("Last 10 rows from inference_results:")
    for row in rows:
        print(row)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()