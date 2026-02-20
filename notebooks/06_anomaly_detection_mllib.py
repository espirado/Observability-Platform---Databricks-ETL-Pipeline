# Databricks notebook source
# MAGIC %md
# MAGIC # 06: Anomaly Detection with MLlib (Week 6)
# MAGIC 
# MAGIC **Purpose**: Use Spark MLlib to detect anomalies in service flow patterns
# MAGIC 
# MAGIC **Input**: Gold service flow edges (`/observability-data/gold/service_flow_edges`)
# MAGIC 
# MAGIC **Output**: 
# MAGIC - Trained anomaly detection model
# MAGIC - Anomaly scores for each edge
# MAGIC - Flagged anomalous edges
# MAGIC 
# MAGIC **ML Concepts Covered**:
# MAGIC - Feature engineering from logs
# MAGIC - Isolation Forest (via VectorAssembler + KMeans approximation)
# MAGIC - Model training and evaluation
# MAGIC - Prediction on new data
# MAGIC 
# MAGIC **Real-World Use Case**: SRE teams need to detect unusual service behavior
# MAGIC - High error rates
# MAGIC - Unusual latency patterns
# MAGIC - Traffic spikes

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from datetime import datetime, timedelta

# Configuration
GOLD_PATH = "/observability-data/gold/service_flow_edges"
MODEL_PATH = "/observability-data/models/anomaly_detector"
ANOMALY_OUTPUT_PATH = "/observability-data/analytics/anomalies"

# Get processing date
try:
    input_date = dbutils.widgets.get("input_date")
except:
    input_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    dbutils.widgets.text("input_date", input_date, "Input Date (YYYY-MM-DD)")

print(f"🤖 Training anomaly detector for date: {input_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold Service Flow Data

# COMMAND ----------

gold_df = (spark.read
    .format("delta")
    .load(GOLD_PATH)
    .filter(F.col("partition_date") == input_date))

record_count = gold_df.count()
print(f"📊 Loaded {record_count:,} service flow edges")

if record_count == 0:
    print("⚠️  No data found. Make sure notebook 03 has run successfully.")
    dbutils.notebook.exit("NO_DATA")

# Show sample
print("\n📄 Sample edges:")
gold_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering
# MAGIC 
# MAGIC **Key Features for Anomaly Detection**:
# MAGIC 1. Error rate (normalized)
# MAGIC 2. Request count (log-scaled)
# MAGIC 3. Latency percentiles (p95, p99)
# MAGIC 4. Latency variance (max - min)

# COMMAND ----------

# Engineer features
features_df = (gold_df
    # Numeric features
    .withColumn("log_request_count", F.log1p(F.col("request_count")))
    .withColumn("latency_variance", F.col("max_latency") - F.col("min_latency"))
    .withColumn("latency_p95_p50_ratio", 
        F.when(F.col("p50_latency") > 0, F.col("p95_latency") / F.col("p50_latency"))
         .otherwise(1.0))
    
    # Select feature columns
    .select(
        "hour",
        "source_service",
        "target_service", 
        "endpoint",
        "request_count",
        "error_rate",
        "log_request_count",
        "avg_latency",
        "p50_latency",
        "p95_latency",
        "p99_latency",
        "latency_variance",
        "latency_p95_p50_ratio",
        "partition_date"
    )
    
    # Handle nulls
    .fillna({
        "error_rate": 0.0,
        "avg_latency": 0.0,
        "p50_latency": 0.0,
        "p95_latency": 0.0,
        "p99_latency": 0.0,
        "latency_variance": 0.0,
        "latency_p95_p50_ratio": 1.0
    })
)

print(f"✅ Engineered features for {features_df.count():,} edges")
print("\nFeature schema:")
features_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Anomaly Detection Model
# MAGIC 
# MAGIC **Approach**: Use K-Means clustering to find "normal" clusters
# MAGIC - Edges far from cluster centers = anomalies
# MAGIC - Simple but effective for SRE use cases

# COMMAND ----------

# Define feature columns for ML
feature_cols = [
    "error_rate",
    "log_request_count",
    "avg_latency",
    "p95_latency",
    "latency_variance",
    "latency_p95_p50_ratio"
]

# Build ML Pipeline
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="raw_features"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=True
)

# Use 3 clusters (normal, degraded, failing)
kmeans = KMeans(
    featuresCol="features",
    predictionCol="cluster",
    k=3,
    seed=42,
    maxIter=50
)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])

# Train model
print("🔄 Training K-Means model...")
model = pipeline.fit(features_df)
print("✅ Model trained!")

# Save model
model.write().overwrite().save(MODEL_PATH)
print(f"💾 Model saved to {MODEL_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Predictions

# COMMAND ----------

# Apply model to data
predictions_df = model.transform(features_df)

# Calculate distance to cluster center (anomaly score)
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import DoubleType

def euclidean_distance(features, center):
    """Calculate distance between feature vector and cluster center"""
    import numpy as np
    f = np.array(features.toArray())
    c = np.array(center)
    return float(np.linalg.norm(f - c))

# Get cluster centers from trained model
kmeans_model = model.stages[-1]
centers = kmeans_model.clusterCenters()

print(f"📍 Cluster centers: {len(centers)}")

# Broadcast centers
centers_bc = spark.sparkContext.broadcast(centers)

# UDF to calculate anomaly score
@F.udf(DoubleType())
def anomaly_score_udf(features, cluster):
    centers = centers_bc.value
    center = centers[int(cluster)]
    return euclidean_distance(features, center)

predictions_with_scores = predictions_df.withColumn(
    "anomaly_score",
    anomaly_score_udf(F.col("features"), F.col("cluster"))
)

print("✅ Anomaly scores calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Anomalies
# MAGIC 
# MAGIC **Threshold**: 95th percentile of anomaly scores

# COMMAND ----------

# Calculate threshold (95th percentile)
threshold = predictions_with_scores.approxQuantile("anomaly_score", [0.95], 0.01)[0]
print(f"🎯 Anomaly threshold (95th percentile): {threshold:.4f}")

# Flag anomalies
anomalies_df = (predictions_with_scores
    .withColumn("is_anomaly", F.col("anomaly_score") > threshold)
    .withColumn("anomaly_severity", 
        F.when(F.col("anomaly_score") > threshold * 1.5, "high")
         .when(F.col("anomaly_score") > threshold, "medium")
         .otherwise("normal"))
)

# Count anomalies
anomaly_count = anomalies_df.filter(F.col("is_anomaly")).count()
anomaly_pct = (anomaly_count / record_count) * 100

print(f"\n🚨 Anomaly Detection Results:")
print(f"   Total edges: {record_count:,}")
print(f"   Anomalies detected: {anomaly_count:,} ({anomaly_pct:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Anomalies

# COMMAND ----------

print("🔍 Top 10 Anomalous Service Edges:")
top_anomalies = (anomalies_df
    .filter(F.col("is_anomaly"))
    .orderBy(F.col("anomaly_score").desc())
    .select(
        "hour",
        "source_service",
        "target_service",
        "endpoint",
        "request_count",
        "error_rate",
        "p95_latency",
        "anomaly_score",
        "anomaly_severity"
    ))

top_anomalies.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Root Cause Analysis

# COMMAND ----------

print("📊 Anomaly Breakdown by Root Cause:")

# Classify anomalies by probable root cause
anomalies_with_cause = (anomalies_df
    .filter(F.col("is_anomaly"))
    .withColumn("root_cause",
        F.when(F.col("error_rate") > 0.1, "high_error_rate")
         .when(F.col("p95_latency") > F.col("avg_latency") * 3, "high_latency")
         .when(F.col("latency_variance") > 1000, "latency_instability")
         .when(F.col("log_request_count") > 10, "traffic_spike")
         .otherwise("unknown"))
)

anomalies_with_cause.groupBy("root_cause").count().orderBy(F.col("count").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Anomaly Results

# COMMAND ----------

# Select output columns
output_df = (anomalies_df
    .select(
        "hour",
        "source_service",
        "target_service",
        "endpoint",
        "request_count",
        "error_rate",
        "avg_latency",
        "p50_latency",
        "p95_latency",
        "p99_latency",
        "cluster",
        "anomaly_score",
        "is_anomaly",
        "anomaly_severity",
        "partition_date"
    ))

# Write to Delta
(output_df.write
    .format("delta")
    .mode("append")
    .partitionBy("partition_date")
    .option("mergeSchema", "true")
    .save(ANOMALY_OUTPUT_PATH))

print(f"✅ Wrote {output_df.count():,} records to {ANOMALY_OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Evaluation Metrics

# COMMAND ----------

from pyspark.ml.evaluation import ClusteringEvaluator

evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="cluster",
    metricName="silhouette"
)

silhouette = evaluator.evaluate(predictions_df)

print(f"\n📈 Model Quality Metrics:")
print(f"   Silhouette Score: {silhouette:.4f}")
print(f"   Number of clusters: {kmeans_model.getK()}")
print(f"   Anomaly detection rate: {anomaly_pct:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Impact Summary

# COMMAND ----------

# Calculate potential impact
impact_summary = (anomalies_with_cause
    .filter(F.col("is_anomaly"))
    .agg(
        F.sum("request_count").alias("affected_requests"),
        F.avg("error_rate").alias("avg_error_rate_anomalies"),
        F.avg("p95_latency").alias("avg_p95_latency_anomalies"),
        F.countDistinct("source_service").alias("affected_services")
    ))

print("\n💼 Business Impact:")
impact_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = {
    "input_date": input_date,
    "total_edges": record_count,
    "anomalies_detected": anomaly_count,
    "anomaly_rate": f"{anomaly_pct:.1f}%",
    "model_silhouette_score": f"{silhouette:.4f}",
    "num_clusters": kmeans_model.getK(),
    "anomaly_threshold": f"{threshold:.4f}",
    "model_path": MODEL_PATH,
    "output_path": ANOMALY_OUTPUT_PATH,
    "status": "SUCCESS"
}

print("\n" + "="*60)
print("MLLIB ANOMALY DETECTION COMPLETE")
print("="*60)
for key, value in summary.items():
    print(f"{key:30s}: {value}")
print("="*60)

print("\n✅ Week 6 (MLlib) complete!")
print("📊 Trained K-Means model for anomaly detection")
print(f"🚨 Detected {anomaly_count:,} anomalous edges")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Detected {anomaly_count:,} anomalies from {record_count:,} edges")
