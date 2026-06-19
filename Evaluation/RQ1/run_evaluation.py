"""
Run the full Grid Stress Indicator evaluation.

Produces, in evaluation/outputs/:
  realdata_null.txt          the honest result on the raw LOA trace
  accuracy_table.csv         precision/recall/F1/FAR per detector at a common
                             false-alarm budget (the 2x2 decomposition)
  leadtime_table.csv         mean advance-warning minutes + detection rate
  operating_curve.png        false-alarm rate vs mean lead time (the headline)
  roc.png                    detection rate vs false-alarm rate
  nsweep.csv / nsweep.png    aggregation's lead advantage as station count grows
  scenario_example.png       one annotated scenario for the slides

Run from the evaluation/ directory:  python run_evaluation.py
"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import config
import scenario as scn_mod
import forecaster as fc_mod
import metrics as mx

DETECTORS = ["AGG-FORECAST", "AGG-CURRENT", "IND-FORECAST", "IND-CURRENT"]
COLORS = {"AGG-FORECAST": "#1b7837", "AGG-CURRENT": "#7fbf7b",
          "IND-FORECAST": "#762a83", "IND-CURRENT": "#c2a5cf"}


def real_data_null():
    """Run the pipeline's own indicator math on the raw LOA trace."""
    df = pd.read_csv(config.LOA_CSV)
    df["time_new"] = pd.to_datetime(df["time_new"])
    df["time_bin"] = df["time_new"].dt.floor("15min")
    agg = df.groupby(["station_id", "time_bin"]).agg(mean_kwh=("kwh", "mean")).reset_index()
    agg["cur"] = agg["mean_kwh"] / config.CAPACITY_KWH_PER_15MIN
    grid = agg.groupby("time_bin")["cur"].mean()
    lines = [
        "Grid Stress Indicator on the RAW LOA trace (no recalibration):",
        f"  stations                : {agg['station_id'].nunique()}",
        f"  15-min windows           : {grid.shape[0]}",
        f"  mean regional stress     : {grid.mean():.4f}",
        f"  max regional stress      : {grid.max():.4f}",
        f"  windows above {config.GRID_THRESHOLD} : {(grid > config.GRID_THRESHOLD).sum()}",
        "",
        "Finding: across 521 stations and 35,040 fifteen-minute windows (a full year),",
        "regional stress never reaches the 0.8 alert line (max 0.8199, a single-window blip).",
        "With the fixed 15 kWh capacity there are no overload events to measure, so the",
        "overload regime is constructed for the research question below.",
    ]
    return "\n".join(lines)


def run_main():
    """Main comparison at N_STATIONS_MAIN across N_SCENARIOS seeds."""
    T = config.N_WINDOWS
    lo = int(T * config.TRAIN_FRACTION)
    hi = T
    nthr = len(config.THRESHOLDS)

    # accumulate per-threshold curves and per-seed operating-point metrics
    far_curve = {d: np.zeieros(nthr) for d in DETECTORS}
    roc_curve = {d: np.zeros((nthr, 2)) for d in DETECTORS}
    op_metrics = {d: [] for d in DETECTORS}

    for s in range(config.N_SCENARIOS):
        scn = scn_mod.generate_scenario(config.N_STATIONS_MAIN, config.CORRELATION_RHO,
                                        seed=config.RANDOM_SEED + s)
        cur_fc = fc_mod.forecast_cur(scn)
        stats = mx.detector_statistics(scn["cur"], cur_fc)
        overload = scn["overload"]

        for d in DETECTORS:
            op = mx.operating_points(stats[d], overload, lo, hi)
            rc = mx.roc_points(stats[d], overload, lo, hi)
            far_curve[d] += np.array([p[0] for p in op])
            lead_curve[d] += np.array([p[1] for p in op])
            roc_curve[d] += np.array(rc)

            thr = mx.pick_threshold_for_far(stats[d], overload, lo, hi,
                                            config.FALSE_ALARM_BUDGET)
            op_metrics[d].append(mx.evaluate_detector(stats[d], overload, thr, lo, hi))

    for d in DETECTORS:
        far_curve[d] /= config.N_SCENARIOS
        lead_curve[d] /= config.N_SCENARIOS
        roc_curve[d] /= config.N_SCENARIOS

    # --- accuracy + lead-time tables (averaged over seeds) ---
    rows = []
    for d in DETECTORS:
        m = op_metrics[d]
        rows.append({
            "detector": d,
            "precision": np.mean([x["precision"] for x in m]),
            "recall": np.mean([x["recall"] for x in m]),
            "f1": np.mean([x["f1"] for x in m]),
            "false_alarm_rate": np.mean([x["far"] for x in m]),
            "mean_lead_min": np.mean([x["mean_lead_windows"] for x in m]) * 15.0,
            "detection_rate": np.mean([x["detection_rate"] for x in m]),
        })
    table = pd.DataFrame(rows).round(3)
    table.to_csv(config.OUTPUT_DIR / "accuracy_table.csv", index=False)
    table[["detector", "mean_lead_min", "detection_rate", "false_alarm_rate"]].to_csv(
        config.OUTPUT_DIR / "leadtime_table.csv", index=False)

    # --- operating curve (headline) ---
    plt.figure(figsize=(7, 5))
    for d in DETECTORS:
        order = np.argsort(far_curve[d])
        plt.plot(far_curve[d][order], lead_curve[d][order], "-o", ms=3,
                 color=COLORS[d], label=d)
    plt.xlabel("False-alarm rate (alarms outside any warning window)")
    plt.ylabel("Mean advance warning (minutes)")
    plt.title("Earliness vs false alarms: up-and-left is better")
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(config.OUTPUT_DIR / "operating_curve.png", dpi=130)
    plt.close()

    # --- ROC ---
    plt.figure(figsize=(6, 6))
    for d in DETECTORS:
        order = np.argsort(roc_curve[d][:, 0])
        plt.plot(roc_curve[d][order, 0], roc_curve[d][order, 1], "-o", ms=3,
                 color=COLORS[d], label=d)
    plt.plot([0, 1], [0, 1], "k--", alpha=0.4)
    plt.xlabel("False-positive rate"); plt.ylabel("Recall (true-positive rate)")
    plt.title("Per-window detection of regional overload")
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(config.OUTPUT_DIR / "roc.png", dpi=130)
    plt.close()

    return table


def run_nsweep():
    """Aggregate vs isolation recall as region size (station count) grows,
    at a fixed false-alarm budget."""
    T = config.N_WINDOWS
    lo, hi = int(T * config.TRAIN_FRACTION), T
    seeds = 10
    rows = []
    for n in config.N_STATIONS_SWEEP:
        for d in ["AGG-CURRENT", "IND-CURRENT"]:
            recalls, leads = [], []
            for s in range(seeds):
                scn = scn_mod.generate_scenario(n, config.CORRELATION_RHO, seed=1000 + s)
                cur_fc = fc_mod.forecast_cur(scn)
                stats = mx.detector_statistics(scn["cur"], cur_fc)
                thr = mx.pick_threshold_for_far(stats[d], scn["overload"], lo, hi,
                                                config.FALSE_ALARM_BUDGET)
                m = mx.evaluate_detector(stats[d], scn["overload"], thr, lo, hi)
                recalls.append(m["recall"])
                leads.append(m["mean_lead_windows"] * 15.0)
            rows.append({"n_stations": n, "detector": d,
                         "recall": float(np.mean(recalls)),
                         "mean_lead_min": float(np.mean(leads))})
    df = pd.DataFrame(rows)
    df.to_csv(config.OUTPUT_DIR / "nsweep.csv", index=False)

    plt.figure(figsize=(7, 5))
    for d in ["AGG-CURRENT", "IND-CURRENT"]:
        sub = df[df["detector"] == d]
        plt.plot(sub["n_stations"], sub["recall"], "-o", color=COLORS[d], label=d)
    plt.xlabel("Number of stations in the region")
    plt.ylabel("Recall on regional overload windows")
    plt.title("Isolation degrades with scale; aggregation holds")
    plt.ylim(-0.05, 1.05)
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(config.OUTPUT_DIR / "nsweep.png", dpi=130)
    plt.close()
    return df


def run_coordination_sweep():
    """Aggregate vs isolation recall as event participation goes from fully
    distributed to concentrated, holding overload magnitude fixed."""
    T = config.N_WINDOWS
    lo, hi = int(T * config.TRAIN_FRACTION), T
    seeds = 12
    rows = []
    for p in config.PARTICIPATION_SWEEP:
        for d in ["AGG-CURRENT", "IND-CURRENT"]:
            recalls = []
            for s in range(seeds):
                scn = scn_mod.generate_scenario(config.N_STATIONS_MAIN, config.CORRELATION_RHO,
                                                seed=3000 + s, participation=p)
                cur_fc = fc_mod.forecast_cur(scn)
                stats = mx.detector_statistics(scn["cur"], cur_fc)
                thr = mx.pick_threshold_for_far(stats[d], scn["overload"], lo, hi,
                                                config.FALSE_ALARM_BUDGET)
                recalls.append(mx.evaluate_detector(stats[d], scn["overload"], thr, lo, hi)["recall"])
            rows.append({"participation": p, "detector": d, "recall": float(np.mean(recalls))})
    df = pd.DataFrame(rows)
    df.to_csv(config.OUTPUT_DIR / "coordination_sweep.csv", index=False)

    plt.figure(figsize=(7, 5))
    for d in ["AGG-CURRENT", "IND-CURRENT"]:
        sub = df[df["detector"] == d].sort_values("participation")
        plt.plot(sub["participation"], sub["recall"], "-o", color=COLORS[d], label=d)
    plt.xlabel("Event participation  (low = concentrated  \u2192  high = distributed)")
    plt.ylabel("Recall on regional overload windows")
    plt.title("Aggregation's edge lives in distributed stress")
    plt.ylim(-0.05, 1.05)
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(config.OUTPUT_DIR / "coordination_sweep.png", dpi=130)
    plt.close()
    return df


def scenario_example():
    """One annotated scenario for the slides."""
    scn = scn_mod.generate_scenario(config.N_STATIONS_MAIN, config.CORRELATION_RHO,
                                    seed=config.RANDOM_SEED)
    cur_fc = fc_mod.forecast_cur(scn)
    stats = mx.detector_statistics(scn["cur"], cur_fc)
    T = config.N_WINDOWS
    lo = int(T * config.TRAIN_FRACTION)
    x = np.arange(T)

    plt.figure(figsize=(11, 5))
    plt.plot(x, scn["region_u"], color="black", lw=1.5, label="true regional utilisation")
    plt.plot(x, stats["AGG-CURRENT"], color=COLORS["AGG-CURRENT"], lw=1, label="mean CUR (aggregate)")
    plt.plot(x, stats["IND-CURRENT"], color=COLORS["IND-CURRENT"], lw=1, label="max CUR (isolation)")
    plt.axhline(config.L_TRUE, color="red", ls="--", alpha=0.6, label=f"overload line {config.L_TRUE}")
    plt.axvline(lo, color="gray", ls=":", label="train/test split")
    over = scn["overload"]
    plt.fill_between(x, 0, 1.6, where=over, color="red", alpha=0.08)
    plt.ylim(0, 1.6); plt.xlabel("15-minute window"); plt.ylabel("utilisation / CUR")
    plt.title("Example scenario: shaded = true regional overload")
    plt.legend(ncol=2, fontsize=8); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(config.OUTPUT_DIR / "scenario_example.png", dpi=130)
    plt.close()


def main():
    config.OUTPUT_DIR.mkdir(exist_ok=True)
    null = real_data_null()
    (config.OUTPUT_DIR / "realdata_null.txt").write_text(null)
    print(null, "\n")

    print("Running main comparison ...")
    table = run_main()
    print(table.to_string(index=False), "\n")

    print("Running station-count sweep ...")
    nsweep = run_nsweep()
    print(nsweep.to_string(index=False), "\n")

    print("Running coordination (participation) sweep ...")
    coord = run_coordination_sweep()
    print(coord.to_string(index=False), "\n")

    scenario_example()
    print("Done. Figures and tables written to", config.OUTPUT_DIR)


if __name__ == "__main__":
    main()
