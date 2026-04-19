"""
Structured Streaming fraud detection.
Watches: data/stream_input/  (drop CSVs here to simulate live feed)
Writes: artifacts/streaming/alerts/  (flagged transactions)
Run: spark-submit scripts/05_spark_streaming.py
Stop with Ctrl+C after testing.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS, verify_paths
verify_paths()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField,
                                  StringType, DoubleType, IntegerType)

# Input/output paths
stream_input = (PATHS["data"] / "stream_input")
stream_input.mkdir(parents=True, exist_ok=True)

checkpoint = (PATHS["streaming"] / "checkpoint")
checkpoint.mkdir(parents=True, exist_ok=True)

alerts_out = (PATHS["streaming"] / "alerts")
alerts_out.mkdir(parents=True, exist_ok=True)

stream_input_uri = stream_input.resolve().as_uri()
checkpoint_uri = checkpoint.resolve().as_uri()
alerts_out_uri = alerts_out.resolve().as_uri()

spark = SparkSession.builder \
    .appName("BankingStreaming") \
    .master("local[2]") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Explicit schema (avoids inference issues)
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("account_id",     StringType()),
    StructField("timestamp",      StringType()),
    StructField("amount",         DoubleType()),
    StructField("type",           StringType()),
    StructField("is_fraud_flag",  IntegerType()),
])

# read stream 
raw_stream = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", "1") \
    .csv(stream_input_uri)

# Fraud detection logic

flagged = raw_stream \
    .withColumn("parsed_ts",
                F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss")) \
    .withColumn("fraud_score",
                F.when(F.col("amount") > 3000, 0.8)
                 .when(F.col("amount") > 1000, 0.4)
                 .otherwise(0.1)) \
    .filter((F.col("fraud_score") >= 0.8) | (F.col("is_fraud_flag") == 1)) \
    .select("transaction_id", "account_id", "parsed_ts",
            "amount", "type", "fraud_score")

# Write alerts to file
query = flagged.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", alerts_out_uri) \
    .option("checkpointLocation", checkpoint_uri) \
    .option("header", "true") \
    .trigger(processingTime="10 seconds") \
    .start()


print(f"[Stream] Watching: {stream_input}")
print(f"[Stream] Alerts -> {alerts_out}")
print("[Stream] Drop CSVs into stream_input/ to test. Ctrl+C to stop.")

# Copy a batch to test:
import shutil
shutil.copy(str(PATHS["transactions_csv"]),
            str(stream_input / "batch_01.csv"))
print("[Stream] Copied batch_01.csv -> stream_input/")

query.awaitTermination()