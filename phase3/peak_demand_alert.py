import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pickle

print("Loading data...")
df = pd.read_csv('C:/Users/linfangyyu/Downloads/LOA-5min/LOA.csv',
                 nrows=5000000)

# Convert time and create 15-minute bins
df['time_new'] = pd.to_datetime(df['time_new'])
df['time_bin'] = df['time_new'].dt.floor('15min')

# Aggregate into 15-minute windows
print("Aggregating into 15-minute windows...")
agg = df.groupby(['station_id', 'time_bin']).agg(
    mean_kwh=('kwh', 'mean'),
    variance_kwh=('kwh', 'var'),
    data_completeness=('kwh', lambda x: (x > 0).sum() / len(x))
).reset_index()

agg['variance_kwh'] = agg['variance_kwh'].fillna(0)
agg = agg.sort_values(['station_id', 'time_bin'])
agg['rate_of_change'] = agg.groupby('station_id')['mean_kwh'].diff().fillna(0)
agg['capacity_utilization_ratio'] = agg['mean_kwh'] / 15.0
agg['hour_of_day'] = agg['time_bin'].dt.hour
agg['day_of_week'] = agg['time_bin'].dt.dayofweek
agg['anomaly_flag'] = 0

feature_cols = [
    'mean_kwh',
    'variance_kwh',
    'rate_of_change',
    'capacity_utilization_ratio',
    'hour_of_day',
    'day_of_week',
    'data_completeness',
    'anomaly_flag'
]

# Test three prediction horizons
# 1 window ahead = 15 minutes
# 2 windows ahead = 30 minutes (not used, skip to keep it simple)
# 1 window = 5 minutes in original data but we use 15-min bins
# So: horizon 1 = 15 min, horizon 2 = 30 min, horizon 3 = 45 min
horizons = {
    '5-min':  1,   # 1 window ahead (each window = 15 min, but we label as nearest)
    '15-min': 1,   # 1 window ahead = 15 minutes
    '30-min': 2,   # 2 windows ahead = 30 minutes
    '45-min': 3,   # 3 windows ahead = 45 minutes
}

# We use shift values 1, 2, 3 to simulate different prediction horizons
results = {}

for label, shift in [('15-min ahead', 1), ('30-min ahead', 2), ('45-min ahead', 3)]:
    print(f"\nTraining model for {label} prediction horizon...")

    # Create target for this horizon
    agg_h = agg.copy()
    agg_h['target'] = agg_h.groupby('station_id')['mean_kwh'].shift(-shift)
    agg_h = agg_h.dropna(subset=['target'])

    # Chronological split: 80% train, 20% validation
    split_idx = int(len(agg_h) * 0.8)
    train = agg_h.iloc[:split_idx]
    val = agg_h.iloc[split_idx:]

    X_train = train[feature_cols]
    y_train = train['target']
    X_val = val[feature_cols]
    y_val = val['target']

    # Train XGBoost
    model = XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        random_state=42
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_val)
    mae = mean_absolute_error(y_val, y_pred)
    rmse = mean_squared_error(y_val, y_pred) ** 0.5

    results[label] = {'MAE': mae, 'RMSE': rmse}
    print(f"  MAE:  {mae:.4f} kWh")
    print(f"  RMSE: {rmse:.4f} kWh")

# Summary table
print("\n" + "="*50)
print("Peak Demand Alert Experiment Summary")
print("="*50)
print(f"{'Horizon':<20} {'MAE (kWh)':<15} {'RMSE (kWh)':<15}")
print("-"*50)
for label, metrics in results.items():
    print(f"{label:<20} {metrics['MAE']:<15.4f} {metrics['RMSE']:<15.4f}")
print("="*50)
print("\nConclusion: The horizon with the lowest MAE while still")
print("providing actionable lead time is the recommended alert window.")