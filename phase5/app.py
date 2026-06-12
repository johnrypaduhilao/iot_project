import random
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(
    page_title="EV Charging Load Forecasting Dashboard",
    layout="wide"
)


def generate_mock_data():
    stations = [
        "1000604065",
        "1000604066",
        "1000604067",
        "1000604068",
        "1000604069",
    ]

    current_time = datetime.now().replace(second=0, microsecond=0)
    rounded_minute = (current_time.minute // 15) * 15
    now = current_time.replace(minute=rounded_minute)

    rows = []

    for station in stations:
        for i in range(8):
            time_bin = now - timedelta(minutes=15 * (7 - i))
            predicted_kwh = round(random.uniform(0.2, 1.5), 2)

            if predicted_kwh < 0.7:
                alert_level = "normal"
                price_multiplier = 1.0
            elif predicted_kwh < 1.1:
                alert_level = "warning"
                price_multiplier = round(random.uniform(1.1, 1.5), 2)
            else:
                alert_level = "critical"
                price_multiplier = 2.0

            rows.append({
                "station_id": station,
                "time_bin": time_bin.strftime("%Y-%m-%d %H:%M:%S"),
                "predicted_kwh": predicted_kwh,
                "price_multiplier": price_multiplier,
                "alert_level": alert_level,
                "latency_ms": random.randint(120, 900)
            })

    return pd.DataFrame(rows)


st.title("Real-Time EV Charging Load Forecasting Dashboard")

st.caption(
    "Phase 5 - Docker Compose + Live Dashboard for EV Charging Load Forecasting and Dynamic Pricing"
)

df = generate_mock_data()
df["station_id"] = df["station_id"].astype(str)
df["time_bin"] = df["time_bin"].astype(str)

latest_df = (
    df.sort_values("time_bin")
    .groupby("station_id")
    .tail(1)
    .reset_index(drop=True)
)

avg_load = latest_df["predicted_kwh"].mean()
avg_multiplier = latest_df["price_multiplier"].mean()
critical_count = len(latest_df[latest_df["alert_level"] == "critical"])
grid_stress_score = round(latest_df["predicted_kwh"].mean() / 1.5, 2)
avg_latency = latest_df["latency_ms"].mean()

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Active Stations", latest_df["station_id"].nunique())
col2.metric("Avg Predicted Load", f"{avg_load:.2f} kWh")
col3.metric("Avg Price Multiplier", f"{avg_multiplier:.2f}x")
col4.metric("Critical Alerts", critical_count)
col5.metric("Avg Latency", f"{avg_latency:.0f} ms")

st.divider()

left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("Per-Station Load Heatmap")

    heatmap_df = df.pivot_table(
        index="station_id",
        columns="time_bin",
        values="predicted_kwh",
        aggfunc="mean"
    )

    fig = px.imshow(
        heatmap_df,
        labels={
            "x": "Time Bin",
            "y": "Station ID",
            "color": "Predicted kWh"
        },
        aspect="auto"
    )

    fig.update_yaxes(type="category")
    fig.update_xaxes(type="category")

    st.plotly_chart(fig, use_container_width=True)

with right_col:
    st.subheader("Grid Stress Indicator")

    st.metric(
        label="Current Grid Stress Score",
        value=f"{grid_stress_score:.2f}",
        delta="Triggered" if grid_stress_score > 0.8 else "Normal"
    )

    if grid_stress_score > 0.8:
        st.error("Regional grid stress alert triggered.")
    elif grid_stress_score > 0.6:
        st.warning("Grid stress is increasing.")
    else:
        st.success("Grid stress is normal.")

st.subheader("Current Price Multiplier per Station")

price_fig = px.bar(
    latest_df,
    x="station_id",
    y="price_multiplier",
    text="price_multiplier",
    labels={
        "station_id": "Station ID",
        "price_multiplier": "Price Multiplier"
    }
)

price_fig.update_xaxes(type="category")

st.plotly_chart(price_fig, use_container_width=True)

st.subheader("Current Station Status")

st.dataframe(
    latest_df[
        [
            "station_id",
            "time_bin",
            "predicted_kwh",
            "price_multiplier",
            "alert_level",
            "latency_ms"
        ]
    ],
    use_container_width=True
)

st.subheader("Alerts Feed")

alerts_df = latest_df[latest_df["alert_level"].isin(["warning", "critical"])]

if alerts_df.empty:
    st.success("No active alerts.")
else:
    for _, row in alerts_df.iterrows():
        if row["alert_level"] == "critical":
            st.error(
                f"Station {row['station_id']} is CRITICAL. "
                f"Predicted load: {row['predicted_kwh']} kWh. "
                f"Multiplier: {row['price_multiplier']}x."
            )
        else:
            st.warning(
                f"Station {row['station_id']} is WARNING. "
                f"Predicted load: {row['predicted_kwh']} kWh. "
                f"Multiplier: {row['price_multiplier']}x."
            )