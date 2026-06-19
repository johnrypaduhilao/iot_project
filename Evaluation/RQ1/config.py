"""Central configuration for the Grid Stress Indicator evaluation."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
LOA_CSV = REPO_ROOT / "../datasets" / "LOA-5min" / "LOA.csv"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"

# Mirrors production constants
CAPACITY_KWH_PER_15MIN = 15.0   # phase3 / phase4 default station capacity
GRID_THRESHOLD = 0.80           # grid_stress_job.py: mean CUR > 0.8 -> regional alert
STATION_CRITICAL_CUR = 0.90     # phase3/app.py: CUR > 0.9 -> "critical"

# True regional overload: realised load >= L_TRUE of total regional capacity
L_TRUE = 0.80

# Scenario shape
WINDOWS_PER_DAY = 96            # 24h / 15min
N_DAYS = 7
N_WINDOWS = WINDOWS_PER_DAY * N_DAYS
TRAIN_FRACTION = 0.80           # chronological split, mirrors train_model.py

STATION_SIZES = [7.5, 15.0, 30.0]   # heterogeneous per-station capacity (kWh/15min)

BASE_LEVEL_RANGE = (0.28, 0.40)  # per-station baseline utilisation scale
NOISE_AR = 0.6                   # AR(1) coefficient for load noise
NOISE_SD = 0.06                  # idiosyncratic noise scale

# Coordinated regional events: correlated ramp lifting many stations toward a
# sub-critical target so the region crosses capacity without any single
# station looking critical.
EVENTS_PER_DAY_RANGE = (1, 3)
EVENT_HOURS = (16, 22)
EVENT_RAMP_WINDOWS = 4
EVENT_TARGET_BAND = (0.80, 0.92)
EVENT_PARTICIPATION = 1.00      # fraction of stations joining a coordinated event
EVENT_CUR_CAP = 2.00
PARTICIPATION_SWEEP = [1.0, 0.85, 0.7, 0.55, 0.4]   # distributed -> concentrated

# Local single-station bursts (independent, suppressed during events)
LOCAL_BURST_PROB = 0.02
LOCAL_BURST_ADD_RANGE = (0.60, 1.10)

# Sweeps
THRESHOLDS = [round(0.05 * i, 3) for i in range(2, 31)]   # 0.10 .. 1.50
N_STATIONS_MAIN = 12
N_STATIONS_SWEEP = [4, 8, 16, 32, 64]
CORRELATION_RHO = 0.7
N_SCENARIOS = 25
FALSE_ALARM_BUDGET = 0.05

LEAD_LOOKBACK_WINDOWS = 8   # max windows an alarm may precede onset and still count as a warning

RANDOM_SEED = 42
