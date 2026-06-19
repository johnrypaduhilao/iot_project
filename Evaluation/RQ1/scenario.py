"""
Scenario generator.

Builds a multi-station regional load trace whose daily shape comes from the
real LOA data, then layers on two patterns:

  1. Coordinated regional events - many stations rise together, region exceeds
     capacity, no single station looks critical.
  2. Localised spikes - one station overloads alone; region stays healthy.
     Used to check for false regional alarms.

Ground truth label is computed here from realised regional load only.
"""

import numpy as np
import pandas as pd

import config


def diurnal_shape(csv_path) -> np.ndarray:
    """Length-24 normalised hour-of-day load shape from real LOA data.
    Falls back to a flat shape if the file is missing."""
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        return np.ones(24)
    df["time_new"] = pd.to_datetime(df["time_new"])
    df["time_bin"] = df["time_new"].dt.floor("15min")
    agg = df.groupby(["station_id", "time_bin"]).agg(mean_kwh=("kwh", "mean")).reset_index()
    agg["hour"] = agg["time_bin"].dt.hour
    shape = agg.groupby("hour")["mean_kwh"].mean().reindex(range(24)).fillna(0.0).to_numpy()
    # circular 3-point smoothing
    shape = (np.roll(shape, 1) + 2 * shape + np.roll(shape, -1)) / 4.0
    if shape.max() > 0:
        shape = shape / shape.max()
    return 0.3 + 0.7 * shape  # floor so demand never fully drops to zero


def _ar1(n: int, phi: float, sd: float, rng: np.random.Generator) -> np.ndarray:
    """A single AR(1) noise series."""
    x = np.zeros(n)
    for t in range(1, n):
        x[t] = phi * x[t - 1] + rng.normal(0, sd)
    return x


def generate_scenario(n_stations: int, rho: float, seed: int,
                      participation: float = None) -> dict:
    """Generate one regional scenario.

    `participation` is the fraction of stations joining each coordinated
    event (1.0 = fully distributed; lower = same overload carried by fewer,
    hotter-running stations).

    Returns a dict with:
      cur        (T, n) realised capacity-utilisation ratio per station/window
      capacity   (n,)   per-station capacity (kWh / 15 min)
      hour       (T,)   hour of day
      dow        (T,)   day of week
      overload   (T,)   ground-truth regional overload label (bool)
      region_u   (T,)   realised regional utilisation = total load / total capacity
    """
    rng = np.random.default_rng(seed)
    T = config.N_WINDOWS
    shape = diurnal_shape(config.LOA_CSV)

    hour = np.array([(w // (config.WINDOWS_PER_DAY // 24)) % 24 for w in range(T)])
    dow = np.array([(w // config.WINDOWS_PER_DAY) % 7 for w in range(T)])
    shape_t = shape[hour]

    capacity = rng.choice(config.STATION_SIZES, size=n_stations)
    base_level = rng.uniform(*config.BASE_LEVEL_RANGE, size=n_stations)

    # Shared regional factor plus idiosyncratic noise per station
    common = _ar1(T, config.NOISE_AR, config.NOISE_SD, rng)
    cur = np.zeros((T, n_stations))
    for i in range(n_stations):
        idio = _ar1(T, config.NOISE_AR, config.NOISE_SD, rng)
        factor = 1.0 + rho * common + (1.0 - rho) * idio
        cur[:, i] = base_level[i] * shape_t * np.clip(factor, 0.0, None)

    # Coordinated regional events: ramp participating stations toward a
    # sub-critical target band (~0.80-0.90).
    event_window = np.zeros(T, dtype=bool)
    p = config.EVENT_PARTICIPATION if participation is None else participation
    for d in range(config.N_DAYS):
        n_events = rng.integers(*config.EVENTS_PER_DAY_RANGE, endpoint=True)
        for _ in range(n_events):
            start_hour = rng.integers(*config.EVENT_HOURS)
            peak_w = d * config.WINDOWS_PER_DAY + start_hour * (config.WINDOWS_PER_DAY // 24)
            peak_w = min(peak_w, T - 1)
            ramp = config.EVENT_RAMP_WINDOWS
            target = rng.uniform(*config.EVENT_TARGET_BAND, size=n_stations) / p
            target = np.clip(target, 0.0, config.EVENT_CUR_CAP)
            joining = rng.random(n_stations) < p
            for k in range(-ramp, ramp + 1):
                w = peak_w + k
                if 0 <= w < T:
                    g = max(0.0, 1.0 - abs(k) / (ramp + 1))   # triangular bump
                    lifted = cur[w] + g * np.clip(target - cur[w], 0.0, None)
                    cur[w, joining] = lifted[joining]
                    event_window[w] = True

    # Independent single-station bursts, suppressed during coordinated events
    burst = (rng.random((T, n_stations)) < config.LOCAL_BURST_PROB) & ~event_window[:, None]
    burst_add = rng.uniform(*config.LOCAL_BURST_ADD_RANGE, size=(T, n_stations))
    cur += burst * burst_add

    cur = np.clip(cur, 0.0, None)

    # Realised regional utilisation and ground-truth label
    load = cur * capacity                       # (T, n) kWh per 15 min
    region_u = load.sum(axis=1) / capacity.sum()
    overload = region_u >= config.L_TRUE

    return {
        "cur": cur,
        "capacity": capacity,
        "hour": hour,
        "dow": dow,
        "overload": overload,
        "region_u": region_u,
    }
