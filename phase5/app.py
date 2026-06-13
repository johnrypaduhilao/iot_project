"""
Phase 5 - Live Dashboard
========================

Subscribes to the `dynamic-prices` and `alerts` Kafka topics in a background
thread, reads historical data from PostgreSQL (`inference_results`,
`grid_stress`, `data_quality_events`), and renders a live Streamlit dashboard
with:

  * per-station predicted load heatmap (last N windows)
  * current price multiplier per station
  * Grid Stress Indicator score (from grid_stress + live snapshot)
  * alerts feed (station + regional)
  * end-to-end latency, measured as `dashboard_arrival_time -
    kafka_message_timestamp` on the dynamic-prices topic
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List

import pandas as pd
import plotly.express as px
import psycopg2
import psycopg2.extras
import streamlit as st
from confluent_kafka import Consumer, KafkaException


# ----------------------------------------------------------------------------
# Configuration (env vars override defaults so the same image works locally
# and inside docker-compose)
# ----------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
DYNAMIC_PRICES_TOPIC = os.environ.get("DYNAMIC_PRICES_TOPIC", "dynamic-prices")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "alerts")
CONSUMER_GROUP = os.environ.get("KAFKA_GROUP_ID", "phase5-dashboard")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "evdb")
DB_USER = os.environ.get("DB_USER", "evuser")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "evpass")

REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "3"))
PRICE_BUFFER_SIZE = 5_000
ALERT_BUFFER_SIZE = 500
HEATMAP_WINDOWS = 16  # last 16 fifteen-minute bins ~ 4h of history


# ----------------------------------------------------------------------------
# Background Kafka consumer
# ----------------------------------------------------------------------------
class LiveKafkaState:
    """Thread-safe in-memory store shared between the consumer thread and
    Streamlit's render thread."""

    def __init__(self) -> None:
        self.prices: Deque[Dict] = deque(maxlen=PRICE_BUFFER_SIZE)
        self.alerts: Deque[Dict] = deque(maxlen=ALERT_BUFFER_SIZE)
        self.latencies_ms: Deque[float] = deque(maxlen=PRICE_BUFFER_SIZE)
        self.lock = threading.Lock()
        self.started_at = datetime.utcnow()
        self.messages_seen = 0
        self.last_error: str | None = None

    def add_price(self, payload: Dict, kafka_ts_ms: int | None) -> None:
        arrival_ms = time.time() * 1000.0
        with self.lock:
            self.prices.append(payload)
            self.messages_seen += 1
            if kafka_ts_ms and kafka_ts_ms > 0:
                latency = arrival_ms - kafka_ts_ms
                # Filter unreasonable values (clock skew / replayed history)
                if -1_000 < latency < 600_000:
                    self.latencies_ms.append(latency)

    def add_alert(self, payload: Dict) -> None:
        payload = {**payload, "_received_at": datetime.utcnow().isoformat()}
        with self.lock:
            self.alerts.append(payload)

    def snapshot(self) -> Dict:
        with self.lock:
            return {
                "prices": list(self.prices),
                "alerts": list(self.alerts),
                "latencies_ms": list(self.latencies_ms),
                "messages_seen": self.messages_seen,
                "started_at": self.started_at,
                "last_error": self.last_error,
            }


def _consumer_loop(state: LiveKafkaState) -> None:
    while True:
        try:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": CONSUMER_GROUP,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            })
            consumer.subscribe([DYNAMIC_PRICES_TOPIC, ALERTS_TOPIC])
            state.last_error = None

            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    state.last_error = str(msg.error())
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except Exception as exc:
                    state.last_error = f"decode: {exc}"
                    continue

                topic = msg.topic()
                ts_type, ts_ms = msg.timestamp()
                if topic == DYNAMIC_PRICES_TOPIC:
                    state.add_price(payload, ts_ms if ts_ms > 0 else None)
                elif topic == ALERTS_TOPIC:
                    state.add_alert(payload)
        except KafkaException as exc:
            state.last_error = f"kafka: {exc}"
            time.sleep(3)
        except Exception as exc:
            state.last_error = f"loop: {exc}"
            time.sleep(3)


@st.cache_resource
def get_live_state() -> LiveKafkaState:
    state = LiveKafkaState()
    thread = threading.Thread(
        target=_consumer_loop,
        args=(state,),
        daemon=True,
        name="phase5-kafka-consumer",
    )
    thread.start()
    return state


# ----------------------------------------------------------------------------
# PostgreSQL helpers
# ----------------------------------------------------------------------------
@st.cache_resource
def get_db_pool():
    # Single connection is fine for a dashboard with one Streamlit session;
    # we keep autocommit + reconnect on failure.
    return _connect()


def _connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=5,
    )


def query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        conn = get_db_pool()
        if conn.closed:
            get_db_pool.clear()
            conn = get_db_pool()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        conn.commit()
        return pd.DataFrame(rows)
    except Exception as exc:
        st.session_state["_db_error"] = str(exc)
        # Force a reconnect next call
        try:
            get_db_pool.clear()
        except Exception:
            pass
        return pd.DataFrame()


# ----------------------------------------------------------------------------
# Page setup
# ----------------------------------------------------------------------------
st.set_page_config(
    page_title="EV Charging Load Forecasting Dashboard",
    layout="wide",
)

st.title("Real-Time EV Charging Load Forecasting Dashboard")
st.caption(
    "Phase 5 - live Kafka stream + PostgreSQL history for the "
    "ENGR 5785G EV Charging Load Forecasting and Dynamic Pricing pipeline."
)

# Auto-refresh: rerun the script every REFRESH_SECONDS to pick up new
# messages from the consumer thread.
st.markdown(
    f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>",
    unsafe_allow_html=True,
)

state = get_live_state()
snapshot = state.snapshot()

prices_buffer = snapshot["prices"]
alerts_buffer = snapshot["alerts"]
latencies_ms = snapshot["latencies_ms"]


# ----------------------------------------------------------------------------
# Build the working DataFrames
# ----------------------------------------------------------------------------
live_df = pd.DataFrame(prices_buffer) if prices_buffer else pd.DataFrame()

# Historical data from Postgres (last 4 hours, capped at HEATMAP_WINDOWS bins)
hist_df = query_df(
    """
    SELECT station_id, time_bin, predicted_kwh, price_multiplier, alert_level
    FROM inference_results
    WHERE created_at >= NOW() - INTERVAL '12 hours'
    ORDER BY time_bin DESC
    LIMIT 5000
    """
)

grid_df = query_df(
    """
    SELECT time_bin, stress_score, station_count, triggered_alert
    FROM grid_stress
    ORDER BY time_bin DESC
    LIMIT 50
    """
)

# Merge live + history for visualizations
combined = pd.concat([live_df, hist_df], ignore_index=True) if not live_df.empty else hist_df

if not combined.empty:
    combined["station_id"] = combined["station_id"].astype(str)
    combined["time_bin"] = pd.to_datetime(combined["time_bin"], errors="coerce")
    combined = combined.dropna(subset=["time_bin"])
    combined = combined.sort_values("time_bin")
    # Deduplicate (live message and DB row for same station/time_bin)
    combined = combined.drop_duplicates(
        subset=["station_id", "time_bin"], keep="last"
    )


# ----------------------------------------------------------------------------
# KPI row
# ----------------------------------------------------------------------------
latest_df = pd.DataFrame()
if not combined.empty:
    latest_df = (
        combined.sort_values("time_bin")
        .groupby("station_id")
        .tail(1)
        .reset_index(drop=True)
    )

avg_load = latest_df["predicted_kwh"].mean() if not latest_df.empty else 0.0
avg_multiplier = latest_df["price_multiplier"].mean() if not latest_df.empty else 1.0
critical_count = (
    int((latest_df["alert_level"] == "critical").sum())
    if not latest_df.empty
    else 0
)

if not grid_df.empty:
    grid_stress_score = float(grid_df.iloc[0]["stress_score"])
elif not latest_df.empty:
    grid_stress_score = float(latest_df["predicted_kwh"].mean() / 15.0)
else:
    grid_stress_score = 0.0

avg_latency_ms = (
    sum(latencies_ms) / len(latencies_ms) if latencies_ms else None
)
p95_latency_ms = (
    sorted(latencies_ms)[int(0.95 * len(latencies_ms)) - 1]
    if len(latencies_ms) >= 20
    else None
)

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Active Stations", latest_df["station_id"].nunique() if not latest_df.empty else 0)
col2.metric("Avg Predicted Load", f"{avg_load:.2f} kWh")
col3.metric("Avg Price Multiplier", f"{avg_multiplier:.2f}x")
col4.metric("Critical Alerts", critical_count)
col5.metric(
    "Avg E2E Latency",
    f"{avg_latency_ms:.0f} ms" if avg_latency_ms is not None else "-",
    delta=f"p95 {p95_latency_ms:.0f} ms" if p95_latency_ms is not None else None,
)
col6.metric("Grid Stress", f"{grid_stress_score:.2f}")

st.divider()


# ----------------------------------------------------------------------------
# Heatmap + Grid Stress
# ----------------------------------------------------------------------------
left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("Per-Station Load Heatmap")
    if combined.empty:
        st.info(
            "Waiting for predictions on `dynamic-prices` ... "
            "make sure phases 1-4 are running."
        )
    else:
        heatmap_source = combined.copy()
        last_bins = (
            heatmap_source["time_bin"]
            .drop_duplicates()
            .sort_values(ascending=False)
            .head(HEATMAP_WINDOWS)
        )
        heatmap_source = heatmap_source[heatmap_source["time_bin"].isin(last_bins)]
        heatmap_source["time_label"] = heatmap_source["time_bin"].dt.strftime("%H:%M")
        heatmap_df = heatmap_source.pivot_table(
            index="station_id",
            columns="time_label",
            values="predicted_kwh",
            aggfunc="mean",
        )
        fig = px.imshow(
            heatmap_df,
            color_continuous_scale="RdYlGn_r",
            labels={"x": "Time Bin", "y": "Station ID", "color": "Predicted kWh"},
            aspect="auto",
        )
        fig.update_yaxes(type="category")
        fig.update_xaxes(type="category")
        st.plotly_chart(fig, use_container_width=True)

with right_col:
    st.subheader("Grid Stress Indicator")
    st.metric(
        label="Current Grid Stress Score",
        value=f"{grid_stress_score:.2f}",
        delta="Triggered" if grid_stress_score > 0.8 else "Normal",
        delta_color="inverse",
    )
    if grid_stress_score > 0.8:
        st.error("Regional grid stress alert active.")
    elif grid_stress_score > 0.6:
        st.warning("Grid stress is increasing.")
    else:
        st.success("Grid stress is normal.")

    if not grid_df.empty:
        gd = grid_df.copy()
        gd["time_bin"] = pd.to_datetime(gd["time_bin"])
        gd = gd.sort_values("time_bin")
        line = px.line(
            gd,
            x="time_bin",
            y="stress_score",
            markers=True,
            labels={"time_bin": "Time", "stress_score": "Stress"},
        )
        line.add_hline(y=0.8, line_dash="dash", line_color="red")
        st.plotly_chart(line, use_container_width=True)


# ----------------------------------------------------------------------------
# Price multiplier bar + status table
# ----------------------------------------------------------------------------
st.subheader("Current Price Multiplier per Station")

if latest_df.empty:
    st.info("No predictions yet.")
else:
    price_fig = px.bar(
        latest_df.sort_values("price_multiplier", ascending=False),
        x="station_id",
        y="price_multiplier",
        text="price_multiplier",
        color="alert_level",
        color_discrete_map={
            "normal": "#2ecc71",
            "warning": "#f39c12",
            "critical": "#e74c3c",
        },
        labels={
            "station_id": "Station ID",
            "price_multiplier": "Price Multiplier",
            "alert_level": "Alert",
        },
    )
    price_fig.update_xaxes(type="category")
    price_fig.add_hline(y=1.0, line_dash="dot")
    st.plotly_chart(price_fig, use_container_width=True)

    st.subheader("Current Station Status")
    display_cols = [
        c
        for c in ["station_id", "time_bin", "predicted_kwh", "price_multiplier", "alert_level"]
        if c in latest_df.columns
    ]
    st.dataframe(
        latest_df[display_cols].sort_values("station_id"),
        use_container_width=True,
        hide_index=True,
    )


# ----------------------------------------------------------------------------
# Alerts feed (Kafka live)
# ----------------------------------------------------------------------------
st.subheader("Live Alerts Feed (Kafka `alerts` topic)")

if not alerts_buffer:
    st.success("No alerts received yet.")
else:
    recent_alerts = list(reversed(alerts_buffer))[:20]
    for alert in recent_alerts:
        alert_type = alert.get("type", "station")
        if alert_type == "regional":
            st.error(
                f"REGIONAL stress {alert.get('stress_score', 0):.2f} "
                f"across {alert.get('station_count', '?')} stations "
                f"at {alert.get('time_bin')}"
            )
        else:
            level = alert.get("alert_level", "warning")
            line = (
                f"Station {alert.get('station_id')} -> {level.upper()} "
                f"(load {alert.get('predicted_kwh')} kWh, "
                f"multiplier {alert.get('price_multiplier')}x) "
                f"at {alert.get('time_bin')}"
            )
            if level == "critical":
                st.error(line)
            else:
                st.warning(line)


# ----------------------------------------------------------------------------
# Footer / health
# ----------------------------------------------------------------------------
with st.expander("Pipeline health"):
    st.write(
        {
            "kafka_bootstrap": KAFKA_BOOTSTRAP,
            "topics": [DYNAMIC_PRICES_TOPIC, ALERTS_TOPIC],
            "db_host": DB_HOST,
            "db_name": DB_NAME,
            "messages_seen_since_start": snapshot["messages_seen"],
            "consumer_started_at_utc": snapshot["started_at"].isoformat(),
            "last_kafka_error": snapshot["last_error"],
            "last_db_error": st.session_state.get("_db_error"),
            "refresh_seconds": REFRESH_SECONDS,
        }
    )
