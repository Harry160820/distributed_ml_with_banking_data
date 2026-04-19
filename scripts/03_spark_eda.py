"""
Exploratory Data Analysis with PySpark.
Outputs: artifacts/eda/ (plots + summary CSVs)
Run: python scripts/03_spark_eda.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS, verify_paths

verify_paths()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

spark = (
    SparkSession.builder
    .appName("BankingEDA")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Read from HDFS
csv_path = PATHS["bank_csv_hdfs"]

df = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .option("inferSchema", True)
    .csv(csv_path)
)

print(f"[EDA] Rows: {df.count()}, Cols: {len(df.columns)}")
print("[EDA] Columns:", df.columns)

# Schema + null counts
df.printSchema()
null_counts = df.select(
    [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
)
null_counts.show()

# Job subscription counts
job_sub = df.groupBy("job", "y").count().orderBy("job")
job_sub.toPandas().to_csv(PATHS["eda"] / "job_subscription.csv", index=False)

# Age distribution
age_pd = df.select("age").toPandas()

fig, ax = plt.subplots(figsize=(8, 5))
ax.hist(age_pd["age"], bins=20, edgecolor="white")
ax.set_title("Age Distribution of Bank Customers")
ax.set_xlabel("Age")
ax.set_ylabel("Count")
fig.tight_layout()
fig.savefig(PATHS["eda"] / "age_distribution.png", dpi=120)
plt.close()

# Balance by subscription
bal_pd = df.select("balance", "y").toPandas()

fig, ax = plt.subplots(figsize=(8, 4))
for label, grp in bal_pd.groupby("y"):
    ax.hist(grp["balance"].clip(-1000, 10000), bins=40, alpha=0.6, label=f"y={label}")
ax.set_title("Balance Distribution by Subscription")
ax.set_xlabel("Balance")
ax.legend()
fig.tight_layout()
fig.savefig(PATHS["eda"] / "balance_by_subscription.png", dpi=120)
plt.close()

# Correlation heatmap
num_cols = ["age", "balance", "duration", "campaign", "pdays", "previous"]
corr_pd = df.select(num_cols).toPandas().corr()

fig, ax = plt.subplots(figsize=(8, 6))
sns.heatmap(corr_pd, annot=True, fmt=".2f", cmap="Blues", ax=ax)
ax.set_title("Correlation Heatmap of Numeric Features")
fig.tight_layout()
fig.savefig(PATHS["eda"] / "correlation_heatmap.png", dpi=120)
plt.close()

# Summary stats
df.describe(num_cols).toPandas().to_csv(PATHS["eda"] / "summary_stats.csv", index=False)

print(f"[EDA] All outputs saved to: {PATHS['eda']}")
spark.stop()