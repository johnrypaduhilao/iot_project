import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
from confluent_kafka import Producer

TOPIC = "ev-telemetry"
CSV_PATH = os.path.join(os.path.dirname(__file__), "../datasets/LOA-5min/LOA.csv")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default=os.environ.get("CSV_PATH", CSV_PATH))
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--speed", type=float, default=float(os.environ.get("REPLAY_SPEED", 100.0)),
                   help="replay speed multiplier, 0 = send as fast as possible")
    p.add_argument("--max-stations", type=int, default=0, help="limit number of stations (0 = all)")
    p.add_argument("--max-rows", type=int, default=0, help="stop reading after N rows (0 = all), useful for testing")
    return p.parse_args()


def load_stations(csv_path, max_stations, max_rows):
    print(f"Loading {csv_path} ...")
    if max_rows > 0:
        print(f"  capped at {max_rows:,} rows")

    stations = {}

    chunks = pd.read_csv(
        csv_path,
        dtype={"station_id": str, "duration": float, "kwh": float},
        parse_dates=["time_new"],
        chunksize=100_000,
        low_memory=False,
    )

    rows_read = 0
    for chunk in chunks:
        # stop early if --max-rows was set
        if max_rows > 0:
            remaining = max_rows - rows_read
            if remaining <= 0:
                break
            chunk = chunk.iloc[:remaining]

        chunk = chunk.dropna(subset=["station_id", "time_new"])
        chunk["station_id"] = chunk["station_id"].astype(str)
        chunk["duration"] = chunk["duration"].fillna(0.0)
        chunk["kwh"] = chunk["kwh"].fillna(0.0)

        for sid, group in chunk.groupby("station_id", sort=False):
            if sid not in stations:
                if max_stations > 0 and len(stations) >= max_stations:
                    continue
                stations[sid] = []

            ts_series = group["time_new"]
            for ts_str, ts_raw, dur, kwh in zip(
                ts_series.dt.strftime("%Y-%m-%d %H:%M:%S"),
                ts_series,
                group["duration"],
                group["kwh"],
            ):
                stations[sid].append({
                    "station_id": sid,
                    "time_new": ts_str,
                    "duration": float(dur),
                    "kwh": float(kwh),
                    "_ts": ts_raw,
                })

        rows_read += len(chunk)
        print(f"  {rows_read:,} rows, {len(stations):,} stations ...", end="\r")

    print(f"\nDone loading: {rows_read:,} rows, {len(stations):,} stations")

    # sort each station by time so messages go out in order
    for sid in stations:
        stations[sid].sort(key=lambda r: r["_ts"])
        for r in stations[sid]:
            del r["_ts"]

    return stations


def produce_station(sid, records, bootstrap, speed):
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "queue.buffering.max.messages": 500_000,
        "queue.buffering.max.kbytes": 1_048_576,
        "batch.num.messages": 10_000,
    })

    prev_ts = None
    count = 0

    for record in records:
        ts = datetime.strptime(record["time_new"], "%Y-%m-%d %H:%M:%S")

        # sleep to simulate real-time gaps between records
        if speed > 0 and prev_ts is not None:
            gap = (ts - prev_ts).total_seconds()
            if gap > 0:
                time.sleep(gap / speed)

        prev_ts = ts
        producer.produce(
            topic=TOPIC,
            key=sid.encode(),
            value=json.dumps(record).encode(),
        )
        producer.poll(0)
        count += 1

    producer.flush()
    return count


def main():
    args = parse_args()
    stations = load_stations(args.csv, args.max_stations, args.max_rows)

    n = len(stations)
    print(f"\nStarting producer — {n} stations, speed={args.speed}x, broker={args.bootstrap}")

    total = 0
    done = 0

    with ThreadPoolExecutor(max_workers=min(n, 200)) as executor:
        futures = {
            executor.submit(produce_station, sid, records, args.bootstrap, args.speed): sid
            for sid, records in stations.items()
        }
        for f in as_completed(futures):
            total += f.result()
            done += 1
            if done % 50 == 0 or done == n:
                print(f"  {done}/{n} stations done, {total:,} messages sent")

    print(f"\nAll done. {total:,} messages sent.")


if __name__ == "__main__":
    main()
