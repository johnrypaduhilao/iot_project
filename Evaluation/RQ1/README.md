# Grid Stress Indicator — Offline Evaluation

This harness answers the project's research question:

> Does multi-station aggregation (the Grid Stress Indicator = mean
> capacity-utilisation ratio across stations, alert if mean CUR > 0.8) provide
> **earlier** and **more accurate** regional grid-overload warnings than
> monitoring individual stations **in isolation**?

It is a standalone, read-only analysis. It reproduces the pipeline's indicator
math but does **not** modify any of phases 1–5, and it changes no Kafka topic,
feature field, or runtime behaviour. Mirroring the deployed system rather than
editing it is deliberate: the evaluation measures the system as built.

## How to run

```bash
cd evaluation
pip install numpy pandas matplotlib scikit-learn xgboost
python run_evaluation.py
```

Everything lands in `outputs/`. One run takes ~1–2 minutes.

## Why a constructed scenario is necessary (the real-data null)

`outputs/realdata_null.txt` runs the indicator on the raw LOA trace. With the
pipeline's fixed 15 kWh/15-min capacity, regional stress peaks at **0.037** and
**never** approaches the 0.8 line — there are zero overload events. The question
is unanswerable on the raw trace, so the overload regime is constructed:
per-station capacity is recalibrated and coordinated demand is injected. These
are labelled analysis parameters in `config.py`, not edits to the pipeline.

## What the design defends against

- **Circularity.** Ground truth is *physical*: a true regional overload is when
  observed total load reaches `L_TRUE` (0.80) of total regional capacity. It is
  computed from realised load, never from a forecast a detector uses. Because
  station capacities are heterogeneous, the indicator's *mean of per-station
  ratios* is not identical to true *ratio of sums*, so even the aggregate
  detector is an approximation rather than a relabelling of the ground truth.
- **Cherry-picked thresholds.** Nothing rests on a single threshold. Every
  detector is swept across thresholds and compared with operating curves
  (`operating_curve.png`, `roc.png`). Head-to-head tables are reported at a
  common false-alarm budget so all detectors are equally cautious.
- **Confusing early warnings with false alarms.** An alarm in the run-up to a
  real overload is a *timely warning*, not a false positive. Each event gets a
  warning zone of `[onset − lookback, end]`; alarms inside score as warnings
  (with a lead time), alarms outside score as false alarms.
- **A rigged baseline.** Isolation isn't handicapped by hand. Frequent, normal
  single-station busy-ness forces any-station monitoring to keep its threshold
  high (or drown in false alarms); the overloads are *distributed*, so no single
  station looks critical. This is the realistic case where the two approaches
  genuinely differ.

## The four detectors (the 2×2 decomposition)

|              | aggregate (mean CUR) | isolation (max CUR) |
|--------------|----------------------|---------------------|
| **current**  | AGG-CURRENT          | IND-CURRENT         |
| **forecast** | AGG-FORECAST         | IND-FORECAST        |

Crossing {current, forecast} × {mean, max} separates how much earliness comes
from *forecasting* versus from *spatial aggregation*. The forecaster is the
system's own model, reproduced from `phase3/train_model.py` (same 8 features,
same next-window target, same chronological 80/20 split, same hyper-parameters).

## Outputs

- `realdata_null.txt` — indicator on the raw trace (the calibration finding).
- `accuracy_table.csv` — precision / recall / F1 / false-alarm rate / lead /
  detection rate per detector, at the common false-alarm budget.
- `leadtime_table.csv` — the earliness slice of the same table.
- `operating_curve.png` — false-alarm rate vs mean advance warning. Up-and-left
  is better; this is the headline figure.
- `roc.png` — per-window detection of regional overload.
- `nsweep.csv` / `nsweep.png` — recall vs number of stations for the two
  reactive detectors.
- `coordination_sweep.csv` / `coordination_sweep.png` — recall vs how
  distributed each overload is (event participation). Shows the aggregate's
  advantage is specific to distributed stress and disappears when overloads
  concentrate into a few stations.
- `scenario_example.png` — one annotated scenario for orientation.

## Reading the numbers

Per-window **precision is intentionally pessimistic**: ramp-up alarms before the
labelled onset count as false positives even though they are exactly the early
warnings we want. The **false-alarm rate** column is the early-warning-aware
metric and is the one to quote. Recall and detection rate measure whether real
overloads are caught at all.

## Knobs worth sweeping if you want more

In `config.py`: `PARTICIPATION_SWEEP` (how distributed each overload is — the
lever that decides whether aggregation helps; the `coordination_sweep` output
is built from it), `EVENT_TARGET_BAND` (how stressed the region gets),
`LOCAL_BURST_PROB` (how much normal single-station busy-ness isolation must
tolerate), and `N_STATIONS_*`.

Note: `CORRELATION_RHO` controls only the *baseline* load correlation and has
almost no effect on the result — the mechanism lives in how distributed the
overload *events* are (participation), not in baseline noise correlation. Sweep
participation, not rho.

## Caveats to state plainly

- Results are on a **constructed** overload regime, because the real trace has
  no overloads. The scenario is grounded in the real LOA diurnal shape, but the
  stress events are synthetic.
- The question is specifically about **regional** overload. Isolation remains
  necessary for **single-station** faults — a localized failure is invisible to
  a regional mean. The two approaches are complementary; this evaluation only
  shows aggregation wins for the regional early-warning task.
