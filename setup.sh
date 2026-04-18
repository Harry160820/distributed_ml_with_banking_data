#!/bin/bash
# Run once from WSL: bash setup.sh
# Creates the full project skeleton and verifies env.

set -e

PROJECT_ROOT="$HOME/projects/distributed_ml_project"

echo "[setup] Creating project directories..."
mkdir -p \
  "$PROJECT_ROOT/data" \
  "$PROJECT_ROOT/scripts" \
  "$PROJECT_ROOT/artifacts/eda" \
  "$PROJECT_ROOT/artifacts/models" \
  "$PROJECT_ROOT/artifacts/streaming" \
  "$PROJECT_ROOT/artifacts/reports" \
  "$PROJECT_ROOT/logs"

echo "[setup] Checking Python packages..."
pip install pyspark pandas matplotlib seaborn scikit-learn --quiet

echo "[setup] Done. Place bank.csv in $PROJECT_ROOT/data/"
echo "[setup] Then run: python $PROJECT_ROOT/config.py  (to verify paths)"