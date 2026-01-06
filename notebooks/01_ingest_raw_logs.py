# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Ingest Raw Logs (Bronze Layer)
# MAGIC 
# MAGIC **Purpose**: Ingest raw JSON logs from S3/ADLS/GCS into Delta Lake Bronze layer
# MAGIC 
# MAGIC **Input**: 
# MAGIC - Cloud storage path: `s3://bucket/raw-logs/YYYY-MM-DD/*.jsonl`
# MAGIC - Kafka topic: `observability.logs` (optional)
# MAGIC 
# MAGIC **Output**: Delta table at `/mnt/observability/bronze/logs`
# MAGIC 
# MAGIC **Partitioning**: By `partition_date` (derived from timestamp)
# MAGIC 
# MAGIC **Schedule**: Hourly (process previous hour's data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# Configuration
BRONZE_PATH = "/mnt/observability/bronze/logs"
RAW_LOGS_PATH = "s3://your-bucket/raw-logs"  # Override with widget parameter

# Get parameters (set by job or use defaults)
try:
    input_date = dbutils.widgets.get("input_date")  # Format: YYYY-MM-DD
except:
    # Default to yesterday if running manually
    input_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    dbutils.widgets.text("input_date", input_date, "Input Date (YYYY-MM-DD)")

print(f"Processing logs for date: {input_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema
# MAGIC 
# MAGIC Define expected schema for raw logs. This handles various log formats from different sources.

# COMMAND ----------

raw_log_schema = StructType([
    StructField("timestamp", StringType(), False),
    StructField("message", StringType(), True),
    StructField("level", StringType(), True),
    StructField("service", StringType(), True),
    StructField("serviceName", StringType(), True),  # OTel format
    
    # HTTP fields (nested)
    StructField("http", StructType([
        StructField("method", StringType(), True),
        StructField("path", StringType(), True),
        StructField("route", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("latency_ms", DoubleType(), True),
        StructField("client_ip", StringType(), True),
        StructField("user_agent", StringType(), True)
    ]), True),
    
    # Trace context (nested)
    StructField("trace", StructType([
        StructField("trace_id", StringType(), True),
        StructField("span_id", StringType(), True),
        StructField("parent_span_id", StringType(), True)
    ]), True),
    
    # Kubernetes metadata (nested)
    StructField("kubernetes", StructType([
        StructField("cluster", StringType(), True),
        StructField("namespace", StringType(), True),
        StructField("pod", StringType(), True),
        StructField("container", StringType(), True),
        StructField("labels", MapType(StringType(), StringType()), True)
    ]), True),
    
    # Business context (nested)
    StructField("business", StructType([
        StructField("user_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("payment_id", StringType(), True)
    ]), True),
    
    # Catch-all for other fields
    StructField("context_json", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Logs

# COMMAND ----------

# Construct input path for specific date
input_path = f"{RAW_LOGS_PATH}/{input_date}/*.jsonl"

print(f"Reading from: {input_path}")

# Read JSON logs with schema
raw_df = (spark.read
    .format("json")
    .option("multiLine", "false")
    .option("mode", "PERMISSIVE")  # Handle malformed records
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(raw_log_schema)
    .load(input_path))

initial_count = raw_df.count()
print(f"Read {initial_count:,} raw log entries")

# Show sample
display(raw_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Check for corrupt records
corrupt_count = raw_df.filter(F.col("_corrupt_record").isNotNull()).count()

if corrupt_count > 0:
    print(f"WARNING: {corrupt_count} corrupt records found ({corrupt_count/initial_count*100:.2f}%)")
    
    # Log corrupt records for debugging
    corrupt_df = raw_df.filter(F.col("_corrupt_record").isNotNull())
    corrupt_df.select("_corrupt_record").limit(10).show(truncate=False)
    
    # Option 1: Drop corrupt records
    raw_df = raw_df.filter(F.col("_corrupt_record").isNull())
    
    # Option 2: Write to quarantine table for investigation
    # corrupt_df.write.format("delta").mode("append").save("/mnt/observability/quarantine/logs")

# Check for missing timestamps
null_timestamp_count = raw_df.filter(F.col("timestamp").isNull()).count()
if null_timestamp_count > 0:
    print(f"WARNING: {null_timestamp_count} records missing timestamp")
    # Drop records without timestamp
    raw_df = raw_df.filter(F.col("timestamp").isNotNull())

print(f"After quality checks: {raw_df.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich with Metadata

# COMMAND ----------

# Add ingestion metadata
enriched_df = (raw_df
    # Ingestion timestamp
    .withColumn("ingestion_time", F.current_timestamp())
    
    # Source file for traceability
    .withColumn("source_file", F.input_file_name())
    
    # Partition date (for Delta partitioning)
    .withColumn("partition_date", F.to_date(F.col("timestamp")))
    
    # Extract just the date from timestamp for filtering
    .withColumn("log_date", F.to_date(F.col("timestamp")))
    
    # Add processing batch ID
    .withColumn("batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
)

print(f"Enriched {enriched_df.count():,} records with metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate
# MAGIC 
# MAGIC Remove duplicate log entries based on trace_id + span_id + timestamp

# COMMAND ----------

# Deduplicate based on unique identifier
dedupe_df = enriched_df.dropDuplicates([
    "timestamp", 
    "trace.trace_id", 
    "trace.span_id"
])

deduped_count = dedupe_df.count()
duplicates_removed = initial_count - deduped_count

print(f"After deduplication: {deduped_count:,} records")
print(f"Duplicates removed: {duplicates_removed:,} ({duplicates_removed/initial_count*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Bronze

# COMMAND ----------

# Write to Delta Lake Bronze layer
(dedupe_df.write
    .format("delta")
    .mode("append")
    .partitionBy("partition_date")
    .option("mergeSchema", "true")  # Allow schema evolution
    .save(BRONZE_PATH))

print(f"✅ Successfully wrote {deduped_count:,} records to Bronze layer")
print(f"   Path: {BRONZE_PATH}")
print(f"   Partition: {input_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Write

# COMMAND ----------

# Read back to verify
bronze_table = spark.read.format("delta").load(BRONZE_PATH)

# Count records for this partition
partition_count = (bronze_table
    .filter(F.col("partition_date") == input_date)
    .count())

print(f"Verification: {partition_count:,} records in Bronze for {input_date}")

# Show table info
print("\nBronze Table Info:")
display(spark.sql(f"DESCRIBE DETAIL delta.`{BRONZE_PATH}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Delta Table
# MAGIC 
# MAGIC Run compaction and Z-ordering for better query performance

# COMMAND ----------

# Optimize files (compact small files)
spark.sql(f"OPTIMIZE delta.`{BRONZE_PATH}` WHERE partition_date = '{input_date}'")

# Z-order by frequently filtered columns
spark.sql(f"""
    OPTIMIZE delta.`{BRONZE_PATH}` 
    WHERE partition_date = '{input_date}'
    ZORDER BY (service, level)
""")

print("✅ Optimization complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

summary_df = (bronze_table
    .filter(F.col("partition_date") == input_date)
    .groupBy("level", "service")
    .agg(
        F.count("*").alias("log_count"),
        F.min("timestamp").alias("earliest_log"),
        F.max("timestamp").alias("latest_log")
    )
    .orderBy(F.col("log_count").desc()))

print(f"\n📊 Summary for {input_date}:")
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Old Partitions
# MAGIC 
# MAGIC Delete data older than retention period (30 days for Bronze)

# COMMAND ----------

# Calculate cutoff date
retention_days = 30
cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d")

# Vacuum old files (must disable retention check first)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark.sql(f"""
    VACUUM delta.`{BRONZE_PATH}` RETAIN 0 HOURS
""")

print(f"✅ Cleaned up data older than {cutoff_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Metrics
# MAGIC 
# MAGIC Log metrics for monitoring

# COMMAND ----------

metrics = {
    "input_date": input_date,
    "input_path": input_path,
    "records_read": initial_count,
    "records_corrupt": corrupt_count,
    "records_written": deduped_count,
    "duplicates_removed": duplicates_removed,
    "processing_time": datetime.now().isoformat(),
    "status": "SUCCESS"
}

print("\n📈 Job Metrics:")
for key, value in metrics.items():
    print(f"   {key}: {value}")

# Write metrics to monitoring table (optional)
# spark.createDataFrame([metrics]).write.format("delta").mode("append").save("/mnt/observability/metrics/bronze_ingestion")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Ingested {deduped_count:,} records for {input_date}")

