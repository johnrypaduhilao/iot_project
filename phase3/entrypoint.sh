#!/usr/bin/env bash
set -euo pipefail

MODEL_FILE="${MODEL_PATH:-/models/xgboost_model.pkl}"
MODEL_DIR="$(dirname "$MODEL_FILE")"
mkdir -p "$MODEL_DIR"

# Phase 3's app.py expects the model at ./xgboost_model.pkl.
# Either reuse the trained pickle on the shared volume, or train it once.
if [[ ! -f "$MODEL_FILE" ]]; then
  echo "[phase3] No model at $MODEL_FILE - training from LOA dataset ..."
  python train_model.py
  cp xgboost_model.pkl "$MODEL_FILE"
else
  echo "[phase3] Reusing cached model from $MODEL_FILE"
  cp "$MODEL_FILE" xgboost_model.pkl
fi

exec uvicorn app:app --host 0.0.0.0 --port 8000
