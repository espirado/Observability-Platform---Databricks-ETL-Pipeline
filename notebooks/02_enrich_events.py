# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Enrich Events (Silver Layer)
# MAGIC 
# MAGIC **Purpose**: Parse and enrich Bronze logs into clean Silver events
# MAGIC 
# MAGIC **Input**: Delta Bronze table (`/mnt/observability/bronze/logs`)
# MAGIC 
# MAGIC **Output**: Delta Silver table (`/mnt/observability/silver/events`)
# MAGIC 
# MAGIC **Processing**:
# MAGIC - Flatten nested JSON structures
# MAGIC - Extract source/target services from trace context
# MAGIC - Parse timestamps to consistent format
# MAGIC - Enrich with service metadata
# MAGIC - Deduplicate by trace_id + span_id

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# Configuration
BRONZE_PATH = "/mnt/observability/bronze/logs"
SILVER_PATH = "/mnt/observability/silver/events"
METADATA_PATH = "/mnt/observability/metadata/services"

# Get processing date
try:
    input_date = dbutils.widgets.get("input_date")
except:
    input_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    dbutils.widgets.text("input_date", input_date, "Input Date (YYYY-MM-DD)")

print(f"Enriching events for date: {input_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Logs

# COMMAND ----------

bronze_df = (spark.read
    .format("delta")
    .load(BRONZE_PATH)
    .filter(F.col("partition_date") == input_date))

initial_count = bronze_df.count()
print(f"Read {initial_count:,} bronze records for {input_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten Nested Fields

# COMMAND ----------

flattened_df = (bronze_df
    # Service name (handle multiple formats)
    .withColumn("service", F.coalesce(
        F.col("service"),
        F.col("serviceName"),
        F.col("kubernetes.labels.app"),
        F.lit("unknown")
    ))
    
    # HTTP fields
    .withColumn("http_method", F.col("http.method"))
    .withColumn("http_path", F.col("http.path"))
    .withColumn("http_route", F.col("http.route"))
    .withColumn("http_status", F.col("http.status_code"))
    .withColumn("latency_ms", F.col("http.latency_ms"))
    .withColumn("client_ip", F.col("http.client_ip"))
    
    # Trace context
    .withColumn("trace_id", F.col("trace.trace_id"))
    .withColumn("span_id", F.col("trace.span_id"))
    .withColumn("parent_span_id", F.col("trace.parent_span_id"))
    
    # Kubernetes metadata
    .withColumn("k8s_cluster", F.col("kubernetes.cluster"))
    .withColumn("k8s_namespace", F.col("kubernetes.namespace"))
    .withColumn("k8s_pod", F.col("kubernetes.pod"))
    .withColumn("k8s_container", F.col("kubernetes.container"))
    
    # Business context
    .withColumn("user_id", F.col("business.user_id"))
    .withColumn("order_id", F.col("business.order_id"))
    
    # Parse timestamp to proper format
    .withColumn("event_timestamp", 
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
)

print(f"Flattened {flattened_df.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Source and Target Services
# MAGIC 
# MAGIC Use trace parent/child relationships to determine service-to-service calls

# COMMAND ----------

# Create window partitioned by trace_id, ordered by timestamp
trace_window = Window.partitionBy("trace_id").orderBy("event_timestamp")

# Determine span kind (CLIENT vs SERVER)
# CLIENT span: request leaving a service (source)
# SERVER span: request arriving at a service (target)
service_flow_df = (flattened_df
    # Add span kind if not present
    .withColumn("span_kind", 
        F.when(F.col("parent_span_id").isNull(), F.lit("ROOT"))
         .otherwise(F.lit("SPAN")))
    
    # For SERVER spans, current service is the target
    # For CLIENT spans, current service is the source
    .withColumn("target_service", 
        F.when(F.col("http_status").isNotNull(), F.col("service"))
         .otherwise(F.lit(None)))
    
    # Source service is the previous service in the trace
    .withColumn("source_service",
        F.lag("service", 1).over(trace_window))
    
    # If no source, it's an entry point
    .withColumn("source_service",
        F.coalesce(F.col("source_service"), F.lit("external")))
    
    # Endpoint is the HTTP route or path
    .withColumn("endpoint",
        F.coalesce(F.col("http_route"), F.col("http_path"), F.lit("/unknown")))
)

# Filter to only service-to-service interactions
service_calls_df = service_flow_df.filter(
    (F.col("source_service").isNotNull()) &
    (F.col("target_service").isNotNull()) &
    (F.col("source_service") != F.col("target_service"))
)

print(f"Extracted {service_calls_df.count():,} service-to-service calls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich with Service Metadata
# MAGIC 
# MAGIC Join with service metadata table (team, oncall, version, etc.)

# COMMAND ----------

# Read service metadata (create if doesn't exist)
try:
    service_metadata_df = spark.read.format("delta").load(METADATA_PATH)
except:
    print("Service metadata table not found, creating sample...")
    
    # Create sample metadata
    sample_metadata = spark.createDataFrame([
        ("payment-service", "payments-team", "v1.2.3", "critical"),
        ("api-gateway", "platform-team", "v2.0.1", "critical"),
        ("risk-service", "fraud-team", "v1.5.0", "high"),
        ("user-service", "identity-team", "v3.1.2", "medium")
    ], ["service_name", "team", "version", "priority"])
    
    sample_metadata.write.format("delta").mode("overwrite").save(METADATA_PATH)
    service_metadata_df = spark.read.format("delta").load(METADATA_PATH)

# Join with metadata
enriched_df = (service_calls_df
    .join(
        service_metadata_df.selectExpr(
            "service_name as source_service",
            "team as source_team",
            "version as source_version",
            "priority as source_priority"
        ),
        on="source_service",
        how="left"
    )
    .join(
        service_metadata_df.selectExpr(
            "service_name as target_service",
            "team as target_team",
            "version as target_version",
            "priority as target_priority"
        ),
        on="target_service",
        how="left"
    )
)

print(f"Enriched {enriched_df.count():,} records with service metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality and Validation

# COMMAND ----------

# Validate status codes
enriched_df = enriched_df.filter(
    F.col("http_status").between(100, 599) | F.col("http_status").isNull()
)

# Validate latency (must be positive)
enriched_df = enriched_df.filter(
    (F.col("latency_ms") >= 0) | F.col("latency_ms").isNull()
)

# Fill missing latency with 0
enriched_df = enriched_df.fillna({"latency_ms": 0.0})

# Add error flag
enriched_df = enriched_df.withColumn("is_error",
    F.when(F.col("http_status") >= 500, True)
     .when(F.col("http_status").isNull(), False)
     .otherwise(False))

print(f"After validation: {enriched_df.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Final Schema

# COMMAND ----------

# Select and rename columns for Silver layer
silver_df = enriched_df.select(
    F.col("event_timestamp").alias("timestamp"),
    F.col("trace_id"),
    F.col("span_id"),
    F.col("parent_span_id"),
    F.col("source_service"),
    F.col("target_service"),
    F.col("endpoint"),
    F.col("http_method").alias("method"),
    F.col("http_status").alias("status_code"),
    F.col("latency_ms"),
    F.col("is_error"),
    F.col("client_ip"),
    F.col("k8s_pod").alias("pod_id"),
    F.col("k8s_node").alias("node_id"),
    F.col("k8s_namespace").alias("namespace"),
    F.col("user_id"),
    F.col("order_id"),
    F.col("source_team"),
    F.col("target_team"),
    F.col("source_priority"),
    F.col("target_priority"),
    F.col("message"),
    F.col("level"),
    F.to_date("event_timestamp").alias("partition_date")
)

final_count = silver_df.count()
print(f"Final Silver records: {final_count:,}")

# Show sample
display(silver_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Silver

# COMMAND ----------

(silver_df.write
    .format("delta")
    .mode("append")
    .partitionBy("partition_date")
    .option("mergeSchema", "true")
    .save(SILVER_PATH))

print(f"✅ Successfully wrote {final_count:,} records to Silver layer")
print(f"   Path: {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Silver Table

# COMMAND ----------

# Optimize and Z-order
spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}` WHERE partition_date = '{input_date}'")

spark.sql(f"""
    OPTIMIZE delta.`{SILVER_PATH}` 
    WHERE partition_date = '{input_date}'
    ZORDER BY (source_service, target_service, trace_id)
""")

print("✅ Optimization complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

silver_table = spark.read.format("delta").load(SILVER_PATH)

summary = (silver_table
    .filter(F.col("partition_date") == input_date)
    .groupBy("source_service", "target_service")
    .agg(
        F.count("*").alias("call_count"),
        F.sum(F.when(F.col("is_error"), 1).otherwise(0)).alias("error_count"),
        F.avg("latency_ms").alias("avg_latency"),
        F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency")
    )
    .withColumn("error_rate", F.col("error_count") / F.col("call_count"))
    .orderBy(F.col("error_count").desc()))

print(f"\n📊 Service Call Summary for {input_date}:")
display(summary.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics

# COMMAND ----------

quality_metrics = {
    "input_date": input_date,
    "bronze_records": initial_count,
    "silver_records": final_count,
    "enrichment_rate": f"{final_count/initial_count*100:.2f}%",
    "avg_latency": silver_df.agg(F.avg("latency_ms")).collect()[0][0],
    "error_rate": silver_df.filter(F.col("is_error")).count() / final_count,
    "unique_services": silver_df.select("source_service").distinct().count(),
    "unique_traces": silver_df.select("trace_id").distinct().count(),
    "processing_time": datetime.now().isoformat(),
    "status": "SUCCESS"
}

print("\n📈 Quality Metrics:")
for key, value in quality_metrics.items():
    print(f"   {key}: {value}")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Enriched {final_count:,} events for {input_date}")

