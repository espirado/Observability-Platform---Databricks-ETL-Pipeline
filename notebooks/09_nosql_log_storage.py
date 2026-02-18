# Databricks notebook source
# MAGIC %md
# MAGIC # 09: NoSQL Log Storage with Delta Lake (Week 9)
# MAGIC 
# MAGIC **Purpose**: Demonstrate NoSQL concepts using Delta Lake (HBase-like patterns)
# MAGIC 
# MAGIC **Input**: Gold service flow edges and anomaly results
# MAGIC 
# MAGIC **Output**: 
# MAGIC - Row-key based Delta tables
# MAGIC - Time-series optimized storage
# MAGIC - Fast point lookups and scans
# MAGIC 
# MAGIC **NoSQL Concepts Covered**:
# MAGIC - Key-value storage patterns
# MAGIC - Row key design for fast lookups
# MAGIC - Column families (simulated)
# MAGIC - Time-series data modeling
# MAGIC - CAP theorem trade-offs
# MAGIC - Scan patterns vs SQL queries
# MAGIC 
# MAGIC **HBase-Like Patterns in Delta**:
# MAGIC - Composite row keys (service + timestamp)
# MAGIC - Z-ordering for scan optimization
# MAGIC - Time-based partitioning
# MAGIC - Bloom filters (via data skipping)
# MAGIC - Compaction (OPTIMIZE)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import hashlib

# Configuration
GOLD_PATH = "/mnt/observability/gold/service_flow_edges"
ANOMALY_PATH = "/mnt/observability/analytics/anomalies"
NOSQL_PATH = "/mnt/observability/nosql"

print("🗄️  NoSQL Storage with Delta Lake")
print(f"   Output: {NOSQL_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## HBase Concepts Review
# MAGIC 
# MAGIC **HBase Architecture**:
# MAGIC - Row Key: Primary access path (must be designed carefully!)
# MAGIC - Column Families: Group related columns
# MAGIC - Versioning: Multiple versions of same cell
# MAGIC - Region Servers: Distributed storage
# MAGIC 
# MAGIC **Common Patterns**:
# MAGIC - Time-series: `row_key = service + reverse_timestamp`
# MAGIC - Scan efficiency: Keep related data together in row key
# MAGIC - Bloom filters: Skip unnecessary files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data

# COMMAND ----------

# Get latest date
latest_date = spark.read.format("delta").load(GOLD_PATH) \
    .select(F.max("partition_date")).collect()[0][0]

print(f"📅 Latest data date: {latest_date}")

# Load Gold edges
gold_df = spark.read.format("delta").load(GOLD_PATH) \
    .filter(F.col("partition_date") == latest_date)

# Load anomalies
anomaly_df = spark.read.format("delta").load(ANOMALY_PATH) \
    .filter(F.col("partition_date") == latest_date)

print(f"📊 Loaded {gold_df.count():,} edges and {anomaly_df.count():,} anomalies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Row Key (HBase-Style)
# MAGIC 
# MAGIC **Row Key Design**: `{service}#{reverse_timestamp}#{target_service}`
# MAGIC 
# MAGIC **Benefits**:
# MAGIC - All data for a service is co-located
# MAGIC - Reverse timestamp enables scanning recent data first
# MAGIC - Target service adds uniqueness

# COMMAND ----------

# Build row key using native Spark SQL expressions (avoids Python UDF serialization issues)
# Row key format: {service}#{reverse_timestamp}#{target_service}
# Reverse timestamp (9999999999999 - unix_ts_ms) ensures recent-first scans
MAX_TS = 9999999999999

# Add row key to data
nosql_edges = (gold_df
    .withColumn("reverse_ts",
        F.lpad(
            (F.lit(MAX_TS) - (F.unix_timestamp(F.col("hour")) * 1000)).cast("string"),
            13, "0"
        ))
    .withColumn("row_key",
        F.concat_ws("#",
            F.col("source_service"),
            F.col("reverse_ts"),
            F.col("target_service")
        ))
    .drop("reverse_ts")
    .withColumn("row_key_hash", F.md5(F.col("row_key")))  # For bucketing
)

print("✅ Row keys created (using native Spark SQL - no UDF serialization issues)")
    .withColumn("row_key_hash", F.md5(F.col("row_key")))  # For bucketing
)

print("✅ Row keys created")
print("\n📋 Sample row keys:")
nosql_edges.select("row_key", "source_service", "target_service", "hour").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Column Families (Simulated)
# MAGIC 
# MAGIC **Column Families in HBase**:
# MAGIC - `cf:metadata` - Service, endpoint info
# MAGIC - `cf:metrics` - Request counts, latency
# MAGIC - `cf:quality` - Error rates, anomalies

# COMMAND ----------

# Restructure data into column family-like structure
nosql_table = (nosql_edges
    .select(
        # Row key (primary key)
        "row_key",
        "row_key_hash",
        
        # cf:metadata
        F.struct(
            F.col("source_service").alias("source"),
            F.col("target_service").alias("target"),
            F.col("endpoint"),
            F.col("hour").alias("timestamp")
        ).alias("metadata"),
        
        # cf:metrics
        F.struct(
            F.col("request_count"),
            F.col("success_count"),
            F.col("error_count"),
            F.col("avg_latency"),
            F.col("p50_latency"),
            F.col("p95_latency"),
            F.col("p99_latency")
        ).alias("metrics"),
        
        # cf:quality
        F.struct(
            F.col("error_rate"),
            F.col("success_rate"),
            F.col("first_seen"),
            F.col("last_seen")
        ).alias("quality"),
        
        # Partition column
        "partition_date"
    )
)

print("✅ Column families created")
print("\nSchema:")
nosql_table.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta with HBase-Like Optimizations

# COMMAND ----------

# Write with optimizations
(nosql_table
    .repartition(10, "row_key_hash")  # Distribute by hash (like HBase regions)
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("partition_date")
    .option("overwriteSchema", "true")
    .save(f"{NOSQL_PATH}/service_edges_kv"))

print(f"✅ Written to {NOSQL_PATH}/service_edges_kv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize with Z-Ordering (Scan Optimization)
# MAGIC 
# MAGIC **Z-Order**: Co-locate data by multiple dimensions (like HBase row key ordering)

# COMMAND ----------

# Optimize and Z-order by row_key
spark.sql(f"""
    OPTIMIZE delta.`{NOSQL_PATH}/service_edges_kv`
    ZORDER BY (row_key)
""")

print("✅ Z-ordering complete (similar to HBase row key sorting)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Pattern 1: Point Lookup (Get Row by Key)

# COMMAND ----------

import time

# Read table
kv_table = spark.read.format("delta").load(f"{NOSQL_PATH}/service_edges_kv")

# Pick a sample row key
sample_row_key = kv_table.select("row_key").first()[0]

print(f"🔍 Point Lookup Test")
print(f"   Row Key: {sample_row_key}")

# Time the lookup
start = time.time()
result = kv_table.filter(F.col("row_key") == sample_row_key).collect()
lookup_time = time.time() - start

print(f"\n✅ Found {len(result)} record(s)")
print(f"⚡ Lookup time: {lookup_time*1000:.2f}ms")

# Show result
if result:
    print("\n📄 Result:")
    for row in result:
        print(f"   Row Key: {row['row_key']}")
        print(f"   Metadata: {row['metadata']}")
        print(f"   Metrics: {row['metrics']}")
        print(f"   Quality: {row['quality']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Pattern 2: Range Scan (Get All Rows for Service)

# COMMAND ----------

# Scan all rows for a specific service
target_service = "api-gateway"

print(f"📊 Range Scan: All edges from '{target_service}'")

start = time.time()

# Scan using row key prefix (HBase-style)
scan_result = (kv_table
    .filter(F.col("row_key").startswith(f"{target_service}#"))
    .orderBy("row_key")  # Already sorted by Z-order
    .limit(100))

scan_count = scan_result.count()
scan_time = time.time() - start

print(f"\n✅ Scanned {scan_count} rows")
print(f"⚡ Scan time: {scan_time*1000:.2f}ms")

print("\n📋 Sample results:")
scan_result.select(
    "row_key",
    "metadata.target",
    "metrics.request_count",
    "metrics.avg_latency"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Pattern 3: Time-Range Scan (Recent Data)

# COMMAND ----------

# Get most recent data for a service (reverse timestamp design helps!)
print(f"⏰ Time-Range Scan: Recent 1 hour of data for '{target_service}'")

start = time.time()

recent_data = (kv_table
    .filter(F.col("row_key").startswith(f"{target_service}#"))
    .orderBy("row_key")  # Reverse timestamp means recent first!
    .limit(10))

recent_count = recent_data.count()
recent_time = time.time() - start

print(f"\n✅ Retrieved {recent_count} recent rows")
print(f"⚡ Query time: {recent_time*1000:.2f}ms")

recent_data.select(
    "metadata.timestamp",
    "metadata.target",
    "metrics.request_count",
    "quality.error_rate"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CAP Theorem Discussion
# MAGIC 
# MAGIC **CAP Theorem**: You can only have 2 of 3:
# MAGIC - **C**onsistency: All nodes see the same data
# MAGIC - **A**vailability: System always responds
# MAGIC - **P**artition Tolerance: Works despite network splits
# MAGIC 
# MAGIC **HBase Trade-offs**:
# MAGIC - CP system (Consistency + Partition Tolerance)
# MAGIC - May sacrifice availability during region server failures
# MAGIC 
# MAGIC **Delta Lake Trade-offs**:
# MAGIC - CP-like with strong consistency (ACID transactions)
# MAGIC - Cloud storage provides partition tolerance
# MAGIC - Availability depends on cloud storage SLA

# COMMAND ----------

print("📚 CAP Theorem Analysis:")
print("\nHBase:")
print("   ✅ Consistency: Strong (via WAL and MemStore)")
print("   ❌ Availability: May be unavailable during region moves")
print("   ✅ Partition Tolerance: Handles network partitions")
print("   → CP System")

print("\nDelta Lake:")
print("   ✅ Consistency: ACID transactions")
print("   ✅ Availability: High (cloud storage redundancy)")
print("   ✅ Partition Tolerance: Distributed storage")
print("   → Effectively CAP (with cloud storage guarantees)")

print("\n💡 Trade-off: Delta Lake is better for analytics, HBase for low-latency operational queries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Comparison: NoSQL vs SQL Queries

# COMMAND ----------

# Test 1: NoSQL-style row key lookup
start = time.time()
nosql_query = kv_table.filter(F.col("row_key").startswith("api-gateway#")).count()
nosql_time = time.time() - start

# Test 2: SQL-style filter
start = time.time()
sql_query = gold_df.filter(F.col("source_service") == "api-gateway").count()
sql_time = time.time() - start

print("⚡ Performance Comparison:")
print(f"\n   NoSQL-style (row key prefix):")
print(f"      Time: {nosql_time:.3f}s")
print(f"      Result: {nosql_query:,} rows")

print(f"\n   SQL-style (column filter):")
print(f"      Time: {sql_time:.3f}s")
print(f"      Result: {sql_query:,} rows")

print(f"\n   Speedup: {sql_time/nosql_time:.2f}x faster with row key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Compaction (Like HBase Major Compaction)

# COMMAND ----------

print("🔄 Running compaction (like HBase major compaction)...")

# Compact files
spark.sql(f"OPTIMIZE delta.`{NOSQL_PATH}/service_edges_kv`")

# Get table stats
table_stats = spark.sql(f"DESCRIBE DETAIL delta.`{NOSQL_PATH}/service_edges_kv`").collect()[0]

print(f"\n✅ Compaction complete")
print(f"   Num files: {table_stats['numFiles']}")
print(f"   Size: {table_stats['sizeInBytes'] / 1024 / 1024:.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-Series Table (Versioned Data)
# MAGIC 
# MAGIC **HBase Versioning**: Keep multiple versions of same cell

# COMMAND ----------

# Create versioned table (simulating HBase cell versioning)
versioned_metrics = (gold_df
    .select(
        F.concat_ws(":", F.col("source_service"), F.col("target_service"), F.col("endpoint")).alias("row_key"),
        F.col("hour").alias("version_timestamp"),
        F.col("request_count"),
        F.col("error_rate"),
        F.col("avg_latency"),
        F.col("partition_date")
    )
)

# Write versioned data
(versioned_metrics
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("partition_date")
    .save(f"{NOSQL_PATH}/versioned_metrics"))

print(f"✅ Created versioned metrics table")

# Query: Get all versions for a specific row key
sample_key = versioned_metrics.select("row_key").first()[0]

versions = spark.read.format("delta").load(f"{NOSQL_PATH}/versioned_metrics") \
    .filter(F.col("row_key") == sample_key) \
    .orderBy("version_timestamp", ascending=False)

print(f"\n📊 Versions for row key: {sample_key}")
versions.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = {
    "nosql_pattern": "HBase-like row key design",
    "row_key_format": "service#reverse_timestamp#target",
    "column_families": 3,
    "total_rows": kv_table.count(),
    "table_size_mb": f"{table_stats['sizeInBytes'] / 1024 / 1024:.2f}",
    "num_files": table_stats['numFiles'],
    "point_lookup_time_ms": f"{lookup_time*1000:.2f}",
    "range_scan_time_ms": f"{scan_time*1000:.2f}",
    "cap_tradeoff": "CP (Consistency + Partition Tolerance)",
    "optimization": "Z-ordering on row_key",
    "output_path": NOSQL_PATH,
    "status": "SUCCESS"
}

print("\n" + "="*60)
print("NOSQL LOG STORAGE COMPLETE")
print("="*60)
for key, value in summary.items():
    print(f"{key:30s}: {value}")
print("="*60)

print("\n✅ Week 9 (NoSQL Databases) complete!")
print("🗄️  Demonstrated: HBase patterns with Delta Lake")
print("🔑 Row key design: service#reverse_timestamp#target")
print("⚡ Point lookups, range scans, time-series queries")
print("📚 CAP theorem: CP system (like HBase)")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Created NoSQL storage with {kv_table.count():,} rows using HBase patterns")
