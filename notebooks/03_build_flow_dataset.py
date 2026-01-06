# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Build Service Flow Dataset (Gold Layer)
# MAGIC 
# MAGIC **Purpose**: Aggregate Silver events into hourly service flow metrics
# MAGIC 
# MAGIC **Input**: Delta Silver table (`/mnt/observability/silver/events`)
# MAGIC 
# MAGIC **Output**: Delta Gold table (`/mnt/observability/gold/service_flow_edges`)
# MAGIC 
# MAGIC **Aggregations**:
# MAGIC - Request counts per service edge
# MAGIC - Error rates and counts
# MAGIC - Latency percentiles (p50, p95, p99)
# MAGIC - First/last seen timestamps

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# Configuration
SILVER_PATH = "/mnt/observability/silver/events"
GOLD_PATH = "/mnt/observability/gold/service_flow_edges"

# Get processing date
try:
    input_date = dbutils.widgets.get("input_date")
except:
    input_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    dbutils.widgets.text("input_date", input_date, "Input Date (YYYY-MM-DD)")

print(f"Building Gold dataset for date: {input_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Events

# COMMAND ----------

silver_df = (spark.read
    .format("delta")
    .load(SILVER_PATH)
    .filter(F.col("partition_date") == input_date))

initial_count = silver_df.count()
print(f"Read {initial_count:,} Silver events for {input_date}")

if initial_count == 0:
    print("⚠️  No Silver events found. Make sure notebook 02 has run successfully.")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Hourly Service Flow Edges
# MAGIC 
# MAGIC This is the core transformation for Week 4 (Apache Spark aggregations)

# COMMAND ----------

# Aggregate by hour, source, target, endpoint
hourly_edges = (silver_df
    # Add hour window
    .withColumn("hour", F.date_trunc("hour", F.col("timestamp")))
    
    # Group by key dimensions
    .groupBy(
        "hour",
        "source_service", 
        "target_service",
        "endpoint"
    )
    
    # Aggregate metrics
    .agg(
        # Request counts
        F.count("*").alias("request_count"),
        F.sum(F.when(F.col("is_error"), 1).otherwise(0)).alias("error_count"),
        F.sum(F.when(~F.col("is_error"), 1).otherwise(0)).alias("success_count"),
        
        # Latency statistics
        F.avg("latency_ms").alias("avg_latency"),
        F.min("latency_ms").alias("min_latency"),
        F.max("latency_ms").alias("max_latency"),
        F.expr("percentile_approx(latency_ms, 0.50)").alias("p50_latency"),
        F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency"),
        F.expr("percentile_approx(latency_ms, 0.99)").alias("p99_latency"),
        
        # Timestamps
        F.min("timestamp").alias("first_seen"),
        F.max("timestamp").alias("last_seen"),
        
        # Sample trace IDs (for context preservation!)
        F.collect_set("trace_id").alias("sample_trace_ids")
    )
    
    # Calculate derived metrics
    .withColumn("error_rate", 
        F.when(F.col("request_count") > 0, 
               F.col("error_count") / F.col("request_count"))
         .otherwise(0.0))
    
    .withColumn("success_rate", 
        F.when(F.col("request_count") > 0,
               F.col("success_count") / F.col("request_count"))
         .otherwise(0.0))
    
    # Add partition column
    .withColumn("partition_date", F.to_date("hour"))
)

edge_count = hourly_edges.count()
print(f"Aggregated into {edge_count:,} hourly service edges")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show Sample Aggregations

# COMMAND ----------

print("📊 Top Service Edges by Request Count:")
hourly_edges.orderBy(F.col("request_count").desc()).show(10, truncate=False)

print("\n📊 Top Service Edges by Error Rate:")
hourly_edges.filter(F.col("error_count") > 0) \
    .orderBy(F.col("error_rate").desc()) \
    .show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context Preservation Analysis
# MAGIC 
# MAGIC **Key Innovation**: Even though we're aggregating, we keep sample trace IDs
# MAGIC This allows us to drill back to Silver/Bronze for full context when needed!

# COMMAND ----------

print("🔍 Context Preservation Check:")
print(f"Total edges: {edge_count:,}")
print(f"Edges with trace samples: {hourly_edges.filter(F.size('sample_trace_ids') > 0).count():,}")

# Show how many traces we kept per edge
trace_sample_stats = hourly_edges.withColumn(
    "num_traces_sampled", 
    F.size("sample_trace_ids")
).agg(
    F.avg("num_traces_sampled").alias("avg_traces_per_edge"),
    F.min("num_traces_sampled").alias("min_traces"),
    F.max("num_traces_sampled").alias("max_traces")
).collect()[0]

print(f"Avg traces sampled per edge: {trace_sample_stats['avg_traces_per_edge']:.1f}")
print(f"Min traces sampled: {trace_sample_stats['min_traces']}")
print(f"Max traces sampled: {trace_sample_stats['max_traces']}")

print("\n✅ Context preserved! Can reconstruct incidents from trace IDs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reduction Analysis

# COMMAND ----------

print("💾 Data Reduction Analysis:")
print(f"Silver events: {initial_count:,}")
print(f"Gold edges: {edge_count:,}")
print(f"Reduction ratio: {initial_count / edge_count:.1f}x")
print(f"Storage savings: {(1 - edge_count/initial_count)*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Gold

# COMMAND ----------

(hourly_edges.write
    .format("delta")
    .mode("append")
    .partitionBy("partition_date")
    .option("mergeSchema", "true")
    .save(GOLD_PATH))

print(f"✅ Successfully wrote {edge_count:,} edges to Gold layer")
print(f"   Path: {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold Table

# COMMAND ----------

# Optimize files
spark.sql(f"OPTIMIZE delta.`{GOLD_PATH}` WHERE partition_date = '{input_date}'")

# Z-order by frequently filtered columns
spark.sql(f"""
    OPTIMIZE delta.`{GOLD_PATH}` 
    WHERE partition_date = '{input_date}'
    ZORDER BY (source_service, target_service, error_rate)
""")

print("✅ Optimization complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Performance Test
# MAGIC 
# MAGIC Compare query speed: Silver (raw events) vs Gold (aggregated)

# COMMAND ----------

import time

# Query 1: Error rate by service (from Gold - should be fast)
start = time.time()
gold_result = spark.read.format("delta").load(GOLD_PATH) \
    .filter(F.col("partition_date") == input_date) \
    .groupBy("source_service") \
    .agg(F.avg("error_rate").alias("avg_error_rate")) \
    .count()
gold_time = time.time() - start

# Query 2: Same query from Silver (should be slower)
start = time.time()
silver_result = silver_df \
    .groupBy("source_service") \
    .agg(
        (F.sum(F.when(F.col("is_error"), 1).otherwise(0)) / F.count("*")).alias("error_rate")
    ) \
    .count()
silver_time = time.time() - start

print("\n⚡ Query Performance Comparison:")
print(f"Gold query time:   {gold_time:.2f}s")
print(f"Silver query time: {silver_time:.2f}s")
print(f"Speedup: {silver_time/gold_time:.1f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

gold_table = spark.read.format("delta").load(GOLD_PATH)

summary = (gold_table
    .filter(F.col("partition_date") == input_date)
    .agg(
        F.count("*").alias("total_edges"),
        F.sum("request_count").alias("total_requests"),
        F.sum("error_count").alias("total_errors"),
        F.avg("error_rate").alias("avg_error_rate"),
        F.avg("avg_latency").alias("overall_avg_latency"),
        F.countDistinct("source_service").alias("unique_sources"),
        F.countDistinct("target_service").alias("unique_targets")
    ))

print(f"\n📊 Gold Dataset Summary for {input_date}:")
summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Aggregations
# MAGIC 
# MAGIC Ensure our aggregations are correct by comparing totals

# COMMAND ----------

# Validate: Sum of edge request_counts should equal total Silver events
gold_total_requests = gold_table \
    .filter(F.col("partition_date") == input_date) \
    .agg(F.sum("request_count")).collect()[0][0]

silver_total_requests = initial_count

print("\n✅ Validation Check:")
print(f"Silver total events:      {silver_total_requests:,}")
print(f"Gold total requests (sum): {gold_total_requests:,}")

if gold_total_requests == silver_total_requests:
    print("✅ PASS: Aggregation preserved all events!")
else:
    diff = abs(gold_total_requests - silver_total_requests)
    diff_pct = (diff / silver_total_requests) * 100
    print(f"⚠️  Difference: {diff:,} events ({diff_pct:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Metrics

# COMMAND ----------

metrics = {
    "input_date": input_date,
    "silver_events": initial_count,
    "gold_edges": edge_count,
    "reduction_ratio": f"{initial_count / edge_count:.1f}x",
    "storage_savings": f"{(1 - edge_count/initial_count)*100:.1f}%",
    "query_speedup": f"{silver_time/gold_time:.1f}x",
    "processing_time": datetime.now().isoformat(),
    "status": "SUCCESS"
}

print("\n📈 Job Metrics:")
for key, value in metrics.items():
    print(f"   {key}: {value}")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Built {edge_count:,} edges for {input_date}")

