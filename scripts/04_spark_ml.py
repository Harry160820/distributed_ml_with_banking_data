"""
Distributed ML with Spark MLlib.
Target: predict if customer subscribes to term deposit (y).
Output: artifacts/models/rf_model/  +  artifacts/reports/ml_report.txt
Run: spark-submit scripts/04_spark_ml.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS, verify_paths
verify_paths()


from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql import functions as F

from pyspark.ml.feature import (StringIndexer, OneHotEncoder,
                                  VectorAssembler, StandardScaler)

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

spark = SparkSession.builder \
    .appName("BankingML") \
    .master("local[*]") \
    .config("spark.driver.memory", "3g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# load and clean

df = spark.read.csv(str(PATHS["bank_csv_hdfs"]),
                    header=True, sep=",", inferSchema=True)
df = df.withColumn("label", (F.col("y") == "yes").cast("int"))

# Feature definition

cat_cols = ["job", "marital", "education", "default",
            "housing", "loan", "contact", "month", "poutcome"]

num_cols = ["age", "balance", "day", "duration",
            "campaign", "pdays", "previous"]


# Pipeline stages
indexers = [StringIndexer(inputCol=c, outputCol=c+"_idx",
             handleInvalid="keep") for c in cat_cols]

encoders = [OneHotEncoder(inputCol=c+"_idx", outputCol=c+"_ohe")
             for c in cat_cols]

assembler =  VectorAssembler(
    inputCols=[c+"_ohe" for c in cat_cols] + num_cols,
    outputCol="features_raw")

scaler    = StandardScaler(inputCol="features_raw",
                            outputCol="features")
rf        = RandomForestClassifier(labelCol="label", numTrees=100,
                                    maxDepth=6, seed=42)


pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, rf])

# Train / test split
train, test = df.randomSplit([0.8, 0.2], seed=42)
print(f"[ML] Train: {train.count()} | Test: {test.count()}")

# Fit & evaluate
model = pipeline.fit(train)
preds = model.transform(test)

auc_eval = BinaryClassificationEvaluator(labelCol="label")
acc_eval = MulticlassClassificationEvaluator(labelCol="label",
                metricName="accuracy")
f1_eval  = MulticlassClassificationEvaluator(labelCol="label",
                metricName="f1")
auc = auc_eval.evaluate(preds)
acc = acc_eval.evaluate(preds)
f1  = f1_eval.evaluate(preds)

# Save model

model_path = str(PATHS["models"] / "rf_model")
model.write().overwrite().save(model_path)

# Save report
report = PATHS["reports"] / "ml_report.txt"
report.write_text(f"""Banking ML Report
--------------------
Model  : Random Forest (100 trees, maxDepth=6)
Target : term deposit subscription (y)
AUC-ROC: {auc:.4f}
Accuracy: {acc:.4f}
F1 Score: {f1:.4f}
Model saved: {model_path}
""")
print(report.read_text())
spark.stop()
