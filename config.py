"""
Import this in EVERY script:
    from config import PATHS, verify_paths
It builds all paths relative to this file's location,
so the project works regardless of where you cd from.
"""

import os, sys
from pathlib import Path

# Root = directory containing this file
ROOT = Path(__file__).resolve().parent

PATHS = {
    "root":             ROOT,
    "data":             ROOT / "data",
    "bank_csv":         ROOT / "data" / "bank.csv",
    "transactions_csv": ROOT / "data" / "transactions_sim.csv",
    "artifacts":        ROOT / "artifacts",
    "eda":              ROOT / "artifacts" / "eda",
    "models":           ROOT / "artifacts" / "models",
    "streaming":        ROOT / "artifacts" / "streaming",
    "reports":          ROOT / "artifacts" / "reports",
    "logs":             ROOT / "logs",
}

def verify_paths():
    for key, path in PATHS.items():
        if key in ["bank_csv", "transactions_csv"]:
            continue
        path.mkdir(parents=True, exist_ok=True)
    if not PATHS["bank_csv"].exists():
        print(f"[WARNING] bank.csv missing — place it at: {PATHS['bank_csv']}")
    else:
        print(f"[OK] bank.csv found at {PATHS['bank_csv']}")

if __name__ == "__main__":
    verify_paths()
    print("All paths:")
    for k, v in PATHS.items():
        print(f"  {k:20s} → {v}")