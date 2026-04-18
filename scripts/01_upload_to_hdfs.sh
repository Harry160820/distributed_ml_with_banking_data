#!/bin/bash
# Uploads bank.csv and transactions_sim.csv to HDFS.
# Assumes Hadoop is running in WSL (start-all.sh already called).

set -e
PROJECT_ROOT="$HOME/projects/distributed_ml_project"

echo "[hdfs] Creating HDFS directories..."
hdfs dfs -mkdir -p /banking/raw/bank
hdfs dfs -mkdir -p /banking/raw/transactions

echo "[hdfs] Uploading bank.csv..."
hdfs dfs -put -f "$PROJECT_ROOT/data/bank.csv" /banking/raw/bank/

echo "[hdfs] Uploading transactions_sim.csv..."
hdfs dfs -put -f "$PROJECT_ROOT/data/transactions_sim.csv" /banking/raw/transactions/

echo "[hdfs] Verifying uploads..."
hdfs dfs -ls /banking/raw/bank/
hdfs dfs -ls /banking/raw/transactions/
echo "[hdfs] Done."