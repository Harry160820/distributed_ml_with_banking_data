"""
Generates synthetic banking transactions for Spark Streaming.
Reads bank.csv to derive realistic account IDs and balances.
Output is data/transactions_sim.csv
"""
import sys, random, csv
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS, verify_paths

verify_paths()

random.seed(42)
types = ["TRANSFER", "WITHDRAWAL", "DEPOSIT", "PAYMENT"]
base_time = datetime(2024, 1, 1, 9, 0, 0)

rows = []
for i in range(2000):
    ts = base_time + timedelta(seconds=i * 30)
    amount = round(random.expovariate(1/500), 2)  # realistic skew
    is_fraud = amount > 4000 and random.random() < 0.3

    rows.append(
        {
            "transaction_id": f"TXN{i:06d}",
            "account_id":     f"ACC{random.randint(1000,9999)}",
            "timestamp":      ts.isoformat(),
            "amount":         amount,
            "type":           random.choice(types),
            "is_fraud_flag":  int(is_fraud)
        }
    )

out = PATHS["transactions_csv"]
with open(out, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader(); writer.writerows(rows)

print(f"[OK] Wrote {len(rows)} transactions → {out}")