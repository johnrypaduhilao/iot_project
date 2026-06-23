import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pickle

print("Loading data...")
df = pd.read_csv('C:/Users/linfangyyu/Downloads/LOA-5min/LOA.csv',
                 nrows=5000000)

df['time_new'] = pd.to_datetime(df['time_new'])
df['time_bin'] = df['time_new'].dt.floor('15min')

print("Aggregating into 15-minute windows...")
agg = df.groupby(['station_id', 'time_bin']).agg(
    mean_kwh=('kwh', 'mean'),
    variance_kwh=('kwh', 'var'),
    data_completeness=('kwh', lambda x: (x > 0).sum() / len(x))
).reset_index()

# single-record windows have no variance
agg['variance_kwh'] = agg['variance_kwh'].fillna(0)

agg = agg.sort_values(['station_id', 'time_bin'])
agg['rate_of_change'] = agg.groupby('station_id')['mean_kwh'].diff().fillna(0)
agg['capacity_utilization_ratio'] = agg['mean_kwh'] / 15.0
agg['hour_of_day'] = agg['time_bin'].dt.hour
agg['day_of_week'] = agg['time_bin'].dt.dayofweek
# anomaly detection is real-time only; offline training gets a constant zero
agg['anomaly_flag'] = 0
agg['target'] = agg.groupby('station_id')['mean_kwh'].shift(-1)
agg = agg.dropna(subset=['target'])  # last window per station has no next target

print(f"Total rows after aggregation: {len(agg)}")

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

# chronological split — shuffling would leak future data into training
split_idx = int(len(agg) * 0.8)
train = agg.iloc[:split_idx]
val = agg.iloc[split_idx:]

X_train = train[feature_cols]
y_train = train['target']
X_val = val[feature_cols]
y_val = val['target']

print(f"Training set:   {len(train)} rows")
print(f"Validation set: {len(val)} rows")

print("Training XGBoost model...")
model = XGBRegressor(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.1,
    random_state=42
)
model.fit(X_train, y_train)

y_pred = model.predict(X_val)
mae = mean_absolute_error(y_val, y_pred)
rmse = mean_squared_error(y_val, y_pred) ** 0.5

print(f"MAE:  {mae:.4f} kWh")
print(f"RMSE: {rmse:.4f} kWh")

with open('xgboost_model.pkl', 'wb') as f:
    pickle.dump(model, f)

print("Model saved to xgboost_model.pkl")