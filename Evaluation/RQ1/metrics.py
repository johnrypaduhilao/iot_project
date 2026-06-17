"""
Detectors and metrics.

Four detectors, all thresholding a per-window statistic of the per-station CUR:

    AGG-CURRENT   mean_i CUR_observed_i(t)   > theta
    AGG-FORECAST  mean_i CUR_forecast_i(t)   > theta
    IND-CURRENT   max_i  CUR_observed_i(t)   > phi
    IND-FORECAST  max_i  CUR_forecast_i(t)   > phi

Crossing {current, forecast} x {mean, max} isolates how much earliness comes
from forecasting versus from spatial aggregation.

Early-warning scoring separates two things a plain confusion matrix conflates:
an alarm fired in the run-up to a real overload is a *timely warning*, not a
false alarm. We define a warning zone of [onset - lookback, event end] around
each true event; alarms inside it score as warnings (with a lead time), alarms
outside any zone score as false alarms.
"""

import numpy as np

import config


def detector_statistics(cur_obs: np.ndarray, cur_fc: np.ndarray) -> dict:
    """Map each detector name to its per-window scalar statistic."""
    return {
        "AGG-CURRENT": cur_obs.mean(axis=1),
        "AGG-FORECAST": cur_fc.mean(axis=1),
        "IND-CURRENT": cur_obs.max(axis=1),
        "IND-FORECAST": cur_fc.max(axis=1),
    }


def find_events(overload: np.ndarray, lo: int, hi: int):
    """Contiguous overload runs within [lo, hi). Returns list of (onset, end)."""
    events = []
    t = lo
    while t < hi:
        if overload[t]:
            onset = t
            while t < hi and overload[t]:
                t += 1
            events.append((onset, t - 1))
        else:
            t += 1
    return events


def warning_zone_mask(events, lo: int, hi: int) -> np.ndarray:
    """Boolean mask over [lo, hi): True inside any event's warning zone."""
    mask = np.zeros(hi, dtype=bool)
    for onset, end in events:
        start = max(lo, onset - config.LEAD_LOOKBACK_WINDOWS)
        mask[start:end + 1] = True
    return mask


def evaluate_detector(stat: np.ndarray, overload: np.ndarray, threshold: float,
                      lo: int, hi: int) -> dict:
    """Score one detector at one threshold over the test region [lo, hi)."""
    alarm = stat > threshold
    events = find_events(overload, lo, hi)
    zone = warning_zone_mask(events, lo, hi)

    test = slice(lo, hi)
    label = overload[test]
    fired = alarm[test]

    tp = int(np.sum(fired & label))
    fp = int(np.sum(fired & ~label))
    fn = int(np.sum(~fired & label))
    tn = int(np.sum(~fired & ~label))

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    fpr = fp / (fp + tn) if fp + tn else 0.0

    # Early-warning view: false alarms only outside warning zones.
    outside = ~zone
    outside[:lo] = False
    far = float(np.sum(alarm & outside) / np.sum(outside)) if np.sum(outside) else 0.0

    leads, detected = [], 0
    for onset, end in events:
        start = max(lo, onset - config.LEAD_LOOKBACK_WINDOWS)
        fired_idx = np.where(alarm[start:end + 1])[0]
        if fired_idx.size:
            first = start + fired_idx[0]
            leads.append(max(0, onset - first))
            detected += 1
        else:
            leads.append(0)            # missed: no advance warning
    mean_lead = float(np.mean(leads)) if leads else 0.0
    det_rate = detected / len(events) if events else 0.0

    return {
        "precision": precision, "recall": recall, "f1": f1, "fpr": fpr,
        "far": far, "mean_lead_windows": mean_lead, "detection_rate": det_rate,
        "n_events": len(events),
    }


def roc_points(stat: np.ndarray, overload: np.ndarray, lo: int, hi: int):
    """(fpr, recall) pairs across the threshold sweep, for an ROC-style curve."""
    pts = []
    for thr in config.THRESHOLDS:
        m = evaluate_detector(stat, overload, thr, lo, hi)
        pts.append((m["fpr"], m["recall"]))
    return pts


def operating_points(stat: np.ndarray, overload: np.ndarray, lo: int, hi: int):
    """(false_alarm_rate, mean_lead_minutes) pairs -- the joint earliness vs
    false-alarm trade-off curve that answers 'earlier AND more accurate'."""
    pts = []
    for thr in config.THRESHOLDS:
        m = evaluate_detector(stat, overload, thr, lo, hi)
        pts.append((m["far"], m["mean_lead_windows"] * 15.0))  # windows -> minutes
    return pts


def pick_threshold_for_far(stat, overload, lo, hi, far_budget: float) -> float:
    """Lowest threshold whose false-alarm rate is within the budget -- i.e. the
    most sensitive setting that still respects the operator's false-alarm cap."""
    best = config.THRESHOLDS[-1]
    for thr in config.THRESHOLDS:
        m = evaluate_detector(stat, overload, thr, lo, hi)
        if m["far"] <= far_budget:
            best = thr
            break
    return best
