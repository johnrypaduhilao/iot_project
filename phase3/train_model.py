import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pickle

# Load LOA dataset (first 5 million rows to keep memory manageable)
print("Loading data...")
df = pd.read_csv('C:/Users/linfangyyu/Downloads/LOA-5min/LOA.csv',
                 nrows=5000000)

# Convert time column to datetime format
df['time_new'] = pd.to_datetime(df['time_new'])

# Round each timestamp down to the nearest 15-minute interval
df['time_bin'] = df['time_new'].dt.floor('15min')

# Aggregate raw 5-minute records into 15-minute windows per station
print("Aggregating into 15-minute windows...")
agg = df.groupby(['station_id', 'time_bin']).agg(
    mean_kwh=('kwh', 'mean'),
    variance_kwh=('kwh', 'var'),
    data_completeness=('kwh', lambda x: (x > 0).sum() / len(x))
).reset_index()

# Fill NaN variance values with 0 (happens when window has only one record)
agg['variance_kwh'] = agg['variance_kwh'].fillna(0)

# Sort by station and time before computing rate of change
agg = agg.sort_values(['station_id', 'time_bin'])

# Rate of change: difference between current and previous window mean
agg['rate_of_change'] = agg.groupby('station_id')['mean_kwh'].diff().fillna(0)

# Capacity utilization ratio: mean_kwh divided by default capacity of 15.0 kWh
agg['capacity_utilization_ratio'] = agg['mean_kwh'] / 15.0

# Time-based features
agg['hour_of_day'] = agg['time_bin'].dt.hour
agg['day_of_week'] = agg['time_bin'].dt.dayofweek

# Anomaly flag: set to 0 for offline training (no real-time detection here)
agg['anomaly_flag'] = 0

# Target variable: mean_kwh of the next 15-minute window
agg['target'] = agg.groupby('station_id')['mean_kwh'].shift(-1)

# Drop the last row per station (no target available)
agg = agg.dropna(subset=['target'])

print(f"Total rows after aggregation: {len(agg)}")

# Define feature columns used for training
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

# Chronological train/validation split — no random shuffle to prevent data leakage
split_idx = int(len(agg) * 0.8)
train = agg.iloc[:split_idx]
val = agg.iloc[split_idx:]

X_train = train[feature_cols]
y_train = train['target']
X_val = val[feature_cols]
y_val = val['target']

print(f"Training set:   {len(train)} rows")
print(f"Validation set: {len(val)} rows")

# Train XGBoost regressor
print("Training XGBoost model...")
model = XGBRegressor(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.1,
    random_state=42
)
model.fit(X_train, y_train)

# Evaluate on validation set
y_pred = model.predict(X_val)
mae = mean_absolute_error(y_val, y_pred)
rmse = mean_squared_error(y_val, y_pred) ** 0.5

print(f"MAE:  {mae:.4f} kWh")
print(f"RMSE: {rmse:.4f} kWh")

# Save the trained model to disk
with open('xgboost_model.pkl', 'wb') as f:
    pickle.dump(model, f)

print("Model saved to xgboost_model.pkl")