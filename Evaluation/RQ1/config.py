"""
Central configuration for the Grid Stress Indicator evaluation.

Everything that defines the experiment lives here so the methodology is
auditable in one place and a reviewer can see exactly which numbers are
fixed, which are swept, and which mirror the production pipeline.

Production constants (mirrored, never changed):
  - CAPACITY_KWH_PER_15MIN ... the pipeline's fixed 15.0 default
  - GRID_THRESHOLD ........... grid_stress_job.py fires on mean CUR > 0.8
  - STATION_CRITICAL_CUR ..... phase3/app.py marks a station critical at CUR > 0.9

Recalibration knobs (analysis parameters, NOT pipeline edits):
  - TARGET_NORMAL_CUR, STATION_SIZES, event magnitudes. These exist only
    because the raw LOA trace peaks at CUR 0.037 and never enters the stress
    regime, so the overload regime has to be constructed to be studied at all.
"""

from pathlib import Path

# --- paths -----------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
#LOA_CSV = REPO_ROOT / "iot_project-main" / "datasets" / "LOA-5min" / "LOA_demo.csv"
LOA_CSV = REPO_ROOT / "../datasets" / "LOA-5min" / "LOA.csv"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"

# --- production constants we mirror (do not change to game results) --------
CAPACITY_KWH_PER_15MIN = 15.0   # phase3 / phase4 default station capacity
GRID_THRESHOLD = 0.80           # grid_stress_job.py: mean CUR > 0.8 -> regional alert
STATION_CRITICAL_CUR = 0.90     # phase3/app.py: CUR > 0.9 -> "critical"

# --- ground truth (physical, independent of any detector) ------------------
# A true regional overload is observed total load within L_TRUE of total
# regional capacity. Defined on REALISED load, never on a forecast, so no
# detector can be circular with the label.
L_TRUE = 0.80

# --- scenario shape --------------------------------------------------------
WINDOWS_PER_DAY = 96            # 24h / 15min
N_DAYS = 7
N_WINDOWS = WINDOWS_PER_DAY * N_DAYS
TRAIN_FRACTION = 0.80           # chronological split, mirrors train_model.py

# Heterogeneous station capacities (kWh / 15 min). Mixed sizes make the
# indicator's mean-of-ratios differ from true ratio-of-sums utilisation,
# which is what keeps the aggregate detector from being identical to the label.
STATION_SIZES = [7.5, 15.0, 30.0]

BASE_LEVEL_RANGE = (0.28, 0.40)  # per-station baseline utilisation scale
NOISE_AR = 0.6                   # AR(1) coefficient for load noise
NOISE_SD = 0.06                  # idiosyncratic noise scale (kept small so noise
                                 # alone never manufactures a regional overload)

# Coordinated regional events: a correlated ramp that lifts MANY stations by a
# moderate, sub-critical amount. Each station stays below its own 0.9 critical
# line, but because most stations rise together the region crosses capacity.
# This is the distributed-stress regime where aggregation is meant to help and
# an any-station rule is structurally blind.
EVENTS_PER_DAY_RANGE = (1, 3)
EVENT_HOURS = (16, 22)          # evening peak window events can start in
EVENT_RAMP_WINDOWS = 4          # ramp-up length before peak
EVENT_TARGET_BAND = (0.80, 0.92)   # per-station utilisation at event peak
EVENT_PARTICIPATION = 1.00      # fraction of stations joining a coordinated event
EVENT_CUR_CAP = 2.00            # ceiling on a single station's CUR during an event
PARTICIPATION_SWEEP = [1.0, 0.85, 0.7, 0.55, 0.4]   # distributed -> concentrated

# Normal single-station busy-ness: independent bursts that push ONE station to
# high utilisation without stressing the region. Frequent enough that an
# any-station rule must keep its threshold high to avoid constant false alarms.
LOCAL_BURST_PROB = 0.02         # per station, per window
LOCAL_BURST_ADD_RANGE = (0.60, 1.10)

# --- sweeps ----------------------------------------------------------------
THRESHOLDS = [round(0.05 * i, 3) for i in range(2, 31)]   # 0.10 .. 1.50
N_STATIONS_MAIN = 12
N_STATIONS_SWEEP = [4, 8, 16, 32, 64]
CORRELATION_RHO = 0.7           # shared-factor weight in the main scenario
N_SCENARIOS = 25                # independent seeds for stable distributions
FALSE_ALARM_BUDGET = 0.05       # operating point for the head-to-head lead-time table

# Lead time is only credited if an alarm precedes onset by at most this many
# windows (a warning 3 hours early about a different event is not a warning).
LEAD_LOOKBACK_WINDOWS = 8

RANDOM_SEED = 42
