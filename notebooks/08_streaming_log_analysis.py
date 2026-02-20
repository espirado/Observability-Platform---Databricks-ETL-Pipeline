# Databricks notebook source
# MAGIC %md
# MAGIC # 08: Streaming Log Analysis with Structured Streaming (Week 8)
# MAGIC 
# MAGIC **Purpose**: Process logs in real-time using Spark Structured Streaming
# MAGIC 
# MAGIC **Input**: Simulated streaming logs (Auto Loader from file source)
# MAGIC 
# MAGIC **Output**: 
# MAGIC - Real-time service flow metrics
# MAGIC - Live anomaly alerts
# MAGIC - Streaming dashboard data
# MAGIC 
# MAGIC **Streaming Concepts Covered**:
# MAGIC - Structured Streaming basics
# MAGIC - Micro-batch processing
# MAGIC - Windowed aggregations (tumbling, sliding)
# MAGIC - Watermarks for late data handling
# MAGIC - Event time vs processing time
# MAGIC - Streaming sinks (Delta, console, memory)
# MAGIC 
# MAGIC **Real-World Use Case**: SRE teams need real-time incident detection
# MAGIC - Detect errors as they happen
# MAGIC - Alert on SLA breaches immediately
# MAGIC - Track live service health

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json

# Configuration
STREAM_SOURCE_PATH = "/observability-data/streaming/input"
STREAM_CHECKPOINT_PATH = "/observability-data/streaming/checkpoints"
STREAM_OUTPUT_PATH = "/observability-data/streaming/output"

# Create streaming input directory if it doesn't exist
dbutils.fs.mkdirs(STREAM_SOURCE_PATH)

print(f"📡 Streaming Analytics Setup")
print(f"   Source: {STREAM_SOURCE_PATH}")
print(f"   Checkpoint: {STREAM_CHECKPOINT_PATH}")
print(f"   Output: {STREAM_OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Simulated Streaming Data
# MAGIC 
# MAGIC **Approach**: Write logs to landing zone, simulate real-time ingestion

# COMMAND ----------

import random
from datetime import datetime, timedelta

def generate_streaming_log_batch(batch_id, num_logs=100):
    """Generate a batch of simulated streaming logs"""
    services = ["api-gateway", "auth-service", "payment-service", "order-service", "inventory-service"]
    endpoints = ["/api/v1/health", "/api/v1/users", "/api/v1/orders", "/api/v1/payments"]
    levels = ["INFO", "INFO", "INFO", "WARN", "ERROR"]  # 60% INFO, 20% WARN, 20% ERROR
    
    logs = []
    base_time = datetime.now()
    
    for i in range(num_logs):
        # Simulate 10% error rate
        is_error = random.random() < 0.10
        
        log = {
            "timestamp": (base_time + timedelta(seconds=random.randint(0, 60))).isoformat() + "Z",
            "level": "ERROR" if is_error else random.choice(["INFO", "WARN"]),
            "source_service": random.choice(services),
            "target_service": random.choice(services),
            "endpoint": random.choice(endpoints),
            "http_status": random.choice([500, 502, 503]) if is_error else 200,
            "latency_ms": random.randint(500, 5000) if is_error else random.randint(10, 500),
            "trace_id": f"trace_{batch_id}_{i:04d}",
            "is_error": is_error,
            "batch_id": batch_id
        }
        logs.append(log)
    
    return logs

# Generate first batch
print("🔄 Generating initial streaming batch...")
batch_1 = generate_streaming_log_batch(batch_id=1, num_logs=200)

# Write to streaming source
with open(f"/dbfs{STREAM_SOURCE_PATH}/batch_001.jsonl", "w") as f:
    for log in batch_1:
        f.write(json.dumps(log) + "\n")

print(f"✅ Generated {len(batch_1)} streaming logs")
print("\n📄 Sample log:")
print(json.dumps(batch_1[0], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Streaming Schema

# COMMAND ----------

streaming_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("level", StringType(), True),
    StructField("source_service", StringType(), True),
    StructField("target_service", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("http_status", IntegerType(), True),
    StructField("latency_ms", IntegerType(), True),
    StructField("trace_id", StringType(), True),
    StructField("is_error", BooleanType(), True),
    StructField("batch_id", IntegerType(), True)
])

print("✅ Streaming schema defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Streaming DataFrame
# MAGIC 
# MAGIC **Structured Streaming**: Treat stream as unbounded table

# COMMAND ----------

# Read streaming data
streaming_df = (spark.readStream
    .schema(streaming_schema)
    .format("json")
    .option("maxFilesPerTrigger", 1)  # Process 1 file per micro-batch
    .load(STREAM_SOURCE_PATH))

# Parse timestamp
streaming_df = streaming_df.withColumn(
    "event_time", 
    F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
)

print("✅ Streaming DataFrame created")
print("\nIs streaming:", streaming_df.isStreaming)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Query 1: Tumbling Window Aggregations
# MAGIC 
# MAGIC **Tumbling Window**: Non-overlapping 1-minute windows

# COMMAND ----------

# Aggregate per 1-minute window
windowed_metrics = (streaming_df
    .withWatermark("event_time", "2 minutes")  # Handle up to 2min late data
    .groupBy(
        F.window("event_time", "1 minute"),  # Tumbling window
        "source_service",
        "target_service"
    )
    .agg(
        F.count("*").alias("request_count"),
        F.sum(F.when(F.col("is_error"), 1).otherwise(0)).alias("error_count"),
        F.avg("latency_ms").alias("avg_latency"),
        F.max("latency_ms").alias("max_latency"),
        F.approx_count_distinct("trace_id").alias("unique_traces")
    )
    .withColumn("error_rate", 
        F.when(F.col("request_count") > 0, F.col("error_count") / F.col("request_count"))
         .otherwise(0.0))
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end", F.col("window.end"))
    .drop("window")
)

print("✅ Tumbling window aggregations defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Query 2: Real-Time Anomaly Detection
# MAGIC 
# MAGIC **Alert Logic**: Flag edges with high error rate or latency

# COMMAND ----------

# Define alert thresholds
ERROR_RATE_THRESHOLD = 0.15  # 15%
LATENCY_THRESHOLD = 1000     # 1 second

# Filter for anomalies
streaming_anomalies = (windowed_metrics
    .filter(
        (F.col("error_rate") > ERROR_RATE_THRESHOLD) | 
        (F.col("avg_latency") > LATENCY_THRESHOLD)
    )
    .withColumn("alert_type",
        F.when(F.col("error_rate") > ERROR_RATE_THRESHOLD, "HIGH_ERROR_RATE")
         .when(F.col("avg_latency") > LATENCY_THRESHOLD, "HIGH_LATENCY")
         .otherwise("UNKNOWN"))
    .withColumn("severity",
        F.when(F.col("error_rate") > 0.5, "CRITICAL")
         .when(F.col("error_rate") > 0.3, "HIGH")
         .when(F.col("avg_latency") > 2000, "HIGH")
         .otherwise("MEDIUM"))
)

print("✅ Streaming anomaly detection defined")
print(f"   Alert if error_rate > {ERROR_RATE_THRESHOLD} OR avg_latency > {LATENCY_THRESHOLD}ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Sink 1: Write Metrics to Delta (Append Mode)

# COMMAND ----------

# Start streaming query to write metrics
metrics_query = (windowed_metrics
    .writeStream
    .format("delta")
    .outputMode("append")  # Append completed windows only
    .option("checkpointLocation", f"{STREAM_CHECKPOINT_PATH}/metrics")
    .trigger(processingTime="30 seconds")  # Micro-batch every 30s
    .start(f"{STREAM_OUTPUT_PATH}/realtime_metrics"))

print(f"✅ Started streaming query: {metrics_query.id}")
print(f"   Status: {metrics_query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Sink 2: Write Anomalies to Delta

# COMMAND ----------

anomalies_query = (streaming_anomalies
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{STREAM_CHECKPOINT_PATH}/anomalies")
    .trigger(processingTime="30 seconds")
    .start(f"{STREAM_OUTPUT_PATH}/realtime_anomalies"))

print(f"✅ Started anomaly streaming query: {anomalies_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate More Streaming Batches

# COMMAND ----------

import time

# Generate and write 5 more batches (simulating 5 minutes of data)
for batch_id in range(2, 7):
    print(f"⏱️  Generating batch {batch_id}...")
    
    batch_logs = generate_streaming_log_batch(batch_id=batch_id, num_logs=150)
    
    # Write batch
    with open(f"/dbfs{STREAM_SOURCE_PATH}/batch_{batch_id:03d}.jsonl", "w") as f:
        for log in batch_logs:
            f.write(json.dumps(log) + "\n")
    
    print(f"   ✅ Wrote {len(batch_logs)} logs to batch_{batch_id:03d}.jsonl")
    
    # Wait for streaming query to process
    time.sleep(10)

print("\n✅ All batches generated!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Streaming Progress

# COMMAND ----------

# Wait for streams to process
time.sleep(30)

print("📊 Streaming Query Status:")
print(f"\n   Metrics Query: {metrics_query.status['message']}")
print(f"   Batches processed: {metrics_query.lastProgress['batchId'] if metrics_query.lastProgress else 'N/A'}")

print(f"\n   Anomalies Query: {anomalies_query.status['message']}")
print(f"   Batches processed: {anomalies_query.lastProgress['batchId'] if anomalies_query.lastProgress else 'N/A'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Streaming Results

# COMMAND ----------

# Read back the streaming output
metrics_result = spark.read.format("delta").load(f"{STREAM_OUTPUT_PATH}/realtime_metrics")
anomalies_result = spark.read.format("delta").load(f"{STREAM_OUTPUT_PATH}/realtime_anomalies")

metrics_count = metrics_result.count()
anomalies_count = anomalies_result.count()

print(f"📊 Streaming Results:")
print(f"   Metrics windows: {metrics_count:,}")
print(f"   Anomalies detected: {anomalies_count:,}")

print("\n📈 Sample metrics:")
metrics_result.orderBy("window_start").show(10, truncate=False)

print("\n🚨 Sample anomalies:")
if anomalies_count > 0:
    anomalies_result.orderBy("window_start").show(10, truncate=False)
else:
    print("   No anomalies detected (good!)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sliding Window Analysis
# MAGIC 
# MAGIC **Sliding Window**: Overlapping 2-minute windows, sliding every 1 minute

# COMMAND ----------

# Stop previous queries first
metrics_query.stop()
anomalies_query.stop()

print("⏸️  Stopped previous queries")

# Create sliding window aggregation
sliding_window_df = (streaming_df
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        F.window("event_time", "2 minutes", "1 minute"),  # 2-min window, 1-min slide
        "source_service"
    )
    .agg(
        F.count("*").alias("request_count"),
        F.avg("latency_ms").alias("avg_latency")
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end", F.col("window.end"))
    .drop("window")
)

# Start sliding window query
sliding_query = (sliding_window_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{STREAM_CHECKPOINT_PATH}/sliding")
    .trigger(processingTime="30 seconds")
    .start(f"{STREAM_OUTPUT_PATH}/sliding_metrics"))

print(f"✅ Started sliding window query")

# Wait for processing
time.sleep(40)

# Read results
sliding_result = spark.read.format("delta").load(f"{STREAM_OUTPUT_PATH}/sliding_metrics")

print(f"\n📊 Sliding Window Results: {sliding_result.count():,} windows")
sliding_result.orderBy("window_start", "source_service").show(15, truncate=False)

# Stop query
sliding_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Time vs Processing Time
# MAGIC 
# MAGIC **Demonstration**: Show difference between when event happened vs when it was processed

# COMMAND ----------

# Add processing time
processing_time_df = (streaming_df
    .withColumn("processing_time", F.current_timestamp())
    .withColumn("latency_seconds", 
        F.unix_timestamp("processing_time") - F.unix_timestamp("event_time"))
    .select(
        "event_time",
        "processing_time",
        "latency_seconds",
        "source_service",
        "endpoint"
    )
)

# Write with memory sink (for quick inspection)
timing_query = (processing_time_df
    .writeStream
    .format("memory")
    .queryName("timing_analysis")
    .outputMode("append")
    .start())

# Wait for processing
time.sleep(30)

# Query the memory sink
timing_df = spark.sql("SELECT * FROM timing_analysis ORDER BY event_time DESC LIMIT 20")

print("⏰ Event Time vs Processing Time:")
timing_df.show(truncate=False)

print("\n📊 Processing Latency Statistics:")
spark.sql("""
    SELECT 
        avg(latency_seconds) as avg_latency_sec,
        min(latency_seconds) as min_latency_sec,
        max(latency_seconds) as max_latency_sec
    FROM timing_analysis
""").show()

# Stop query
timing_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = {
    "batches_processed": 6,
    "total_logs_streamed": 6 * 150,
    "metrics_windows_created": metrics_count,
    "anomalies_detected": anomalies_count,
    "watermark": "2 minutes",
    "micro_batch_interval": "30 seconds",
    "tumbling_window_size": "1 minute",
    "sliding_window_size": "2 minutes",
    "sliding_window_slide": "1 minute",
    "output_path": STREAM_OUTPUT_PATH,
    "status": "SUCCESS"
}

print("\n" + "="*60)
print("STREAMING LOG ANALYSIS COMPLETE")
print("="*60)
for key, value in summary.items():
    print(f"{key:30s}: {value}")
print("="*60)

print("\n✅ Week 8 (Structured Streaming) complete!")
print("📡 Demonstrated: windowed aggregations, watermarks, micro-batches")
print(f"⚡ Processed {summary['total_logs_streamed']} logs in real-time")
print(f"🚨 Detected {anomalies_count} anomalies in streaming data")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Processed {summary['total_logs_streamed']} streaming logs, detected {anomalies_count} anomalies")
