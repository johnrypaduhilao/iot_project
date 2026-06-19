"""
One-window-ahead load forecaster.

Reproduces phase3/train_model.py: same 8 features, same target (next
window's mean_kwh), same chronological 80/20 split, same XGBoost
hyperparameters. Trained fresh on each scenario's own history so the
"forecast" detectors use a real prediction, not an oracle.
"""

import numpy as np
from xgboost import XGBRegressor

import config

FEATURE_ORDER = [
    "mean_kwh", "variance_kwh", "rate_of_change", "capacity_utilization_ratio",
    "hour_of_day", "day_of_week", "data_completeness", "anomaly_flag",
]

def _features(scn: dict) -> np.ndarray:
    """Build the (T, n, 8) feature tensor from a scenario's realised CUR."""
    cur = scn["cur"]
    cap = scn["capacity"]
    T, n = cur.shape
    mean_kwh = cur * cap
    variance = (0.1 * mean_kwh) ** 2
    roc = np.vstack([np.zeros((1, n)), np.diff(mean_kwh, axis=0)])
    hour = np.repeat(scn["hour"][:, None], n, axis=1)
    dow = np.repeat(scn["dow"][:, None], n, axis=1)
    completeness = np.ones((T, n))
    anomaly = np.zeros((T, n))
    feats = np.stack(
        [mean_kwh, variance, roc, cur, hour, dow, completeness, anomaly], axis=-1
    )
    return feats, mean_kwh


def forecast_cur(scn: dict) -> np.ndarray:
    """Return (T, n) predicted CUR, where row t is the forecast FOR window t
    made from information available at window t-1."""
    feats, mean_kwh = _features(scn)
    T, n, _ = feats.shape

    # Pooled training rows: features of window t -> mean_kwh of window t+1
    split = int(T * config.TRAIN_FRACTION)
    X_rows, y_rows = [], []
    for t in range(split - 1):
        X_rows.append(feats[t])
        y_rows.append(mean_kwh[t + 1])
    X_train = np.vstack(X_rows)
    y_train = np.concatenate(y_rows)

    model = XGBRegressor(
        n_estimators=200, max_depth=6, learning_rate=0.1,
        random_state=config.RANDOM_SEED, verbosity=0,
    )
    model.fit(X_train, y_train)

    # pred_next[t] = predicted mean_kwh for window t+1 (from features at t)
    pred_next = model.predict(feats.reshape(T * n, -1)).reshape(T, n)

    # Align so row t holds the forecast FOR window t (made at t-1).
    pred_mean = np.vstack([mean_kwh[0:1], pred_next[:-1]])
    return pred_mean / scn["capacity"]
