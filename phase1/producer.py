import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue

import pandas as pd
from confluent_kafka import Producer

TOPIC = "ev-telemetry"
CSV_PATH = os.path.join(os.path.dirname(__file__), "../datasets/LOA-5min/LOA.csv")
_DONE = object()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default=os.environ.get("CSV_PATH", CSV_PATH))
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--speed", type=float, default=float(os.environ.get("REPLAY_SPEED", 100.0)),
                   help="replay speed multiplier, 0 = send as fast as possible")
    p.add_argument("--max-stations", type=int,
                   default=int(os.environ.get("MAX_STATIONS", 0)),
                   help="limit number of stations (0 = all)")
    p.add_argument("--max-rows", type=int,
                   default=int(os.environ.get("MAX_ROWS", 0)),
                   help="stop reading after N rows (0 = all)")
    return p.parse_args()


def produce_station(sid, q, bootstrap, speed):
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "queue.buffering.max.messages": 500_000,
        "queue.buffering.max.kbytes": 1_048_576,
        "batch.num.messages": 10_000,
    })

    prev_ts = None
    count = 0

    while True:
        record = q.get()
        if record is _DONE:
            break

        ts = datetime.strptime(record["time_new"], "%Y-%m-%d %H:%M:%S")

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

    print(f"Streaming {args.csv} ...")
    if args.max_rows > 0:
        print(f"  capped at {args.max_rows:,} rows")
    if args.max_stations > 0:
        print(f"  capped at {args.max_stations:,} stations")

    station_queues = {}
    skipped = set()
    rows_read = 0
    total = 0
    done_count = 0

    chunks = pd.read_csv(
        args.csv,
        dtype={"station_id": str, "duration": float, "kwh": float},
        parse_dates=["time_new"],
        chunksize=100_000,
        low_memory=False,
    )

    with ThreadPoolExecutor(max_workers=200) as executor:
        futures = {}

        for chunk in chunks:
            if args.max_rows > 0:
                remaining = args.max_rows - rows_read
                if remaining <= 0:
                    break
                chunk = chunk.iloc[:remaining]

            chunk = chunk.dropna(subset=["station_id", "time_new"])
            chunk["station_id"] = chunk["station_id"].astype(str)
            chunk["duration"] = chunk["duration"].fillna(0.0)
            chunk["kwh"] = chunk["kwh"].fillna(0.0)
            chunk = chunk.sort_values("time_new")

            for row in chunk.itertuples(index=False):
                sid = row.station_id
                if sid in skipped:
                    continue

                if sid not in station_queues:
                    if args.max_stations > 0 and len(station_queues) >= args.max_stations:
                        skipped.add(sid)
                        continue
                    q = Queue(maxsize=1000)
                    station_queues[sid] = q
                    futures[sid] = executor.submit(
                        produce_station, sid, q, args.bootstrap, args.speed
                    )

                station_queues[sid].put({
                    "station_id": sid,
                    "time_new": row.time_new.strftime("%Y-%m-%d %H:%M:%S"),
                    "duration": float(row.duration),
                    "kwh": float(row.kwh),
                })

            rows_read += len(chunk)
            print(f"  {rows_read:,} rows, {len(station_queues):,} stations ...", end="\r")

        for q in station_queues.values():
            q.put(_DONE)

        print(f"\nAll chunks read: {rows_read:,} rows, {len(station_queues):,} stations")
        print("Waiting for producers to flush ...")

        n = len(futures)
        for f in as_completed(futures.values()):
            total += f.result()
            done_count += 1
            if done_count % 50 == 0 or done_count == n:
                print(f"  {done_count}/{n} stations done, {total:,} messages sent")

    print(f"\nAll done. {total:,} messages sent.")


if __name__ == "__main__":
    main()
