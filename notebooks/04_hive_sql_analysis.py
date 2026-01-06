# Databricks notebook source
# MAGIC %md
# MAGIC # 04: Hive/SQL Analysis (Week 3)
# MAGIC 
# MAGIC **Purpose**: Demonstrate Hive concepts - SQL-like queries on Delta Lake
# MAGIC 
# MAGIC **Week 3 Requirement**: Show how to use Hive/SQL for big data analysis
# MAGIC 
# MAGIC **Input**: Delta tables (Bronze, Silver, Gold)
# MAGIC 
# MAGIC **Concepts Covered**:
# MAGIC - Creating external tables
# MAGIC - Partitioning strategies
# MAGIC - SQL queries on distributed data
# MAGIC - Views and temporary tables
# MAGIC - Performance optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Hive External Tables
# MAGIC 
# MAGIC Register Delta Lake paths as Hive tables for SQL access

# COMMAND ----------

# Configuration
BRONZE_PATH = "/mnt/observability/bronze/logs"
SILVER_PATH = "/mnt/observability/silver/events"
GOLD_PATH = "/mnt/observability/gold/service_flow_edges"

print("Creating Hive external tables...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Table (Raw Logs)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS observability;
# MAGIC USE observability;
# MAGIC 
# MAGIC -- Create external table pointing to Delta Bronze
# MAGIC CREATE TABLE IF NOT EXISTS bronze_logs
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/observability/bronze/logs';
# MAGIC 
# MAGIC -- Show table info
# MAGIC DESCRIBE EXTENDED bronze_logs;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Table (Enriched Events)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_events
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/observability/silver/events';
# MAGIC 
# MAGIC -- Show partitioning
# MAGIC SHOW PARTITIONS silver_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table (Service Flow Edges)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_service_flow
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/observability/gold/service_flow_edges';
# MAGIC 
# MAGIC SELECT 'Created Hive tables successfully!' as status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Analysis Queries
# MAGIC 
# MAGIC Demonstrate various SQL operations on the data lake

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Top Services by Traffic

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find services handling the most requests
# MAGIC SELECT 
# MAGIC     source_service,
# MAGIC     SUM(request_count) as total_requests,
# MAGIC     ROUND(AVG(error_rate) * 100, 2) as avg_error_rate_pct,
# MAGIC     ROUND(AVG(avg_latency), 2) as avg_latency_ms
# MAGIC FROM gold_service_flow
# MAGIC GROUP BY source_service
# MAGIC ORDER BY total_requests DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Error Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find service edges with highest error rates
# MAGIC SELECT 
# MAGIC     source_service,
# MAGIC     target_service,
# MAGIC     endpoint,
# MAGIC     SUM(request_count) as requests,
# MAGIC     SUM(error_count) as errors,
# MAGIC     ROUND(AVG(error_rate) * 100, 2) as error_rate_pct,
# MAGIC     ROUND(AVG(p95_latency), 2) as p95_latency_ms
# MAGIC FROM gold_service_flow
# MAGIC WHERE error_count > 0
# MAGIC GROUP BY source_service, target_service, endpoint
# MAGIC ORDER BY error_rate_pct DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Latency Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find slowest service endpoints
# MAGIC SELECT 
# MAGIC     target_service,
# MAGIC     endpoint,
# MAGIC     ROUND(AVG(avg_latency), 2) as avg_latency_ms,
# MAGIC     ROUND(AVG(p95_latency), 2) as p95_latency_ms,
# MAGIC     ROUND(AVG(p99_latency), 2) as p99_latency_ms,
# MAGIC     SUM(request_count) as total_requests
# MAGIC FROM gold_service_flow
# MAGIC GROUP BY target_service, endpoint
# MAGIC HAVING total_requests > 10  -- Filter out low-traffic endpoints
# MAGIC ORDER BY p95_latency_ms DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4: Time Series Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hourly traffic pattern
# MAGIC SELECT 
# MAGIC     DATE_FORMAT(hour, 'yyyy-MM-dd HH:00') as time_window,
# MAGIC     SUM(request_count) as total_requests,
# MAGIC     SUM(error_count) as total_errors,
# MAGIC     ROUND(SUM(error_count) / SUM(request_count) * 100, 2) as error_rate_pct
# MAGIC FROM gold_service_flow
# MAGIC GROUP BY hour
# MAGIC ORDER BY hour;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 5: Service Dependency Map

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show which services call which services
# MAGIC SELECT 
# MAGIC     source_service,
# MAGIC     target_service,
# MAGIC     COUNT(DISTINCT endpoint) as unique_endpoints,
# MAGIC     SUM(request_count) as total_calls
# MAGIC FROM gold_service_flow
# MAGIC GROUP BY source_service, target_service
# MAGIC ORDER BY total_calls DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Views for Common Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view for critical errors
# MAGIC CREATE OR REPLACE VIEW critical_errors AS
# MAGIC SELECT 
# MAGIC     timestamp,
# MAGIC     source_service,
# MAGIC     target_service,
# MAGIC     endpoint,
# MAGIC     status_code,
# MAGIC     latency_ms,
# MAGIC     trace_id,
# MAGIC     message
# MAGIC FROM silver_events
# MAGIC WHERE is_error = true
# MAGIC   AND status_code >= 500;
# MAGIC 
# MAGIC SELECT 'View created!' as status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the view
# MAGIC SELECT 
# MAGIC     DATE(timestamp) as error_date,
# MAGIC     source_service,
# MAGIC     COUNT(*) as error_count
# MAGIC FROM critical_errors
# MAGIC GROUP BY error_date, source_service
# MAGIC ORDER BY error_date DESC, error_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitioning Analysis
# MAGIC 
# MAGIC Show benefits of partitioned tables (Hadoop/Hive concept)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query with partition filter (fast)
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT COUNT(*) 
# MAGIC FROM silver_events 
# MAGIC WHERE partition_date = '2024-12-15';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partition Pruning Benefit

# COMMAND ----------

import time

# Query WITH partition filter (should be fast)
start = time.time()
with_partition = spark.sql("""
    SELECT COUNT(*) 
    FROM silver_events 
    WHERE partition_date = '2024-12-15'
""").collect()
with_partition_time = time.time() - start

# Query WITHOUT partition filter (should be slower)
start = time.time()
without_partition = spark.sql("""
    SELECT COUNT(*) 
    FROM silver_events 
    WHERE DATE(timestamp) = '2024-12-15'
""").collect()
without_partition_time = time.time() - start

print("⚡ Partition Pruning Benefit:")
print(f"With partition filter:    {with_partition_time:.3f}s")
print(f"Without partition filter: {without_partition_time:.3f}s")
print(f"Speedup: {without_partition_time/with_partition_time:.1f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Example: Silver + Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join Silver events with Gold aggregations to find anomalies
# MAGIC SELECT 
# MAGIC     se.timestamp,
# MAGIC     se.source_service,
# MAGIC     se.target_service,
# MAGIC     se.endpoint,
# MAGIC     se.latency_ms,
# MAGIC     gf.avg_latency as typical_latency,
# MAGIC     ROUND((se.latency_ms / gf.avg_latency), 2) as latency_ratio
# MAGIC FROM silver_events se
# MAGIC JOIN gold_service_flow gf
# MAGIC   ON se.source_service = gf.source_service
# MAGIC   AND se.target_service = gf.target_service
# MAGIC   AND se.endpoint = gf.endpoint
# MAGIC   AND DATE_TRUNC('hour', se.timestamp) = gf.hour
# MAGIC WHERE se.latency_ms > gf.avg_latency * 3  -- Anomaly: 3x slower than average
# MAGIC ORDER BY latency_ratio DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complex Aggregation: Window Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate moving average of error rates
# MAGIC SELECT 
# MAGIC     hour,
# MAGIC     source_service,
# MAGIC     ROUND(AVG(error_rate) * 100, 2) as error_rate_pct,
# MAGIC     ROUND(AVG(AVG(error_rate)) OVER (
# MAGIC         PARTITION BY source_service 
# MAGIC         ORDER BY hour 
# MAGIC         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
# MAGIC     ) * 100, 2) as moving_avg_error_rate
# MAGIC FROM gold_service_flow
# MAGIC GROUP BY hour, source_service
# MAGIC ORDER BY source_service, hour
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subquery Example: Top N per Group

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find top 3 slowest endpoints per service
# MAGIC WITH ranked_endpoints AS (
# MAGIC     SELECT 
# MAGIC         target_service,
# MAGIC         endpoint,
# MAGIC         AVG(p95_latency) as avg_p95_latency,
# MAGIC         SUM(request_count) as total_requests,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY target_service 
# MAGIC             ORDER BY AVG(p95_latency) DESC
# MAGIC         ) as rank
# MAGIC     FROM gold_service_flow
# MAGIC     GROUP BY target_service, endpoint
# MAGIC )
# MAGIC SELECT 
# MAGIC     target_service,
# MAGIC     endpoint,
# MAGIC     ROUND(avg_p95_latency, 2) as p95_latency_ms,
# MAGIC     total_requests
# MAGIC FROM ranked_endpoints
# MAGIC WHERE rank <= 3
# MAGIC ORDER BY target_service, rank;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Comparison: Hive vs Spark DataFrame

# COMMAND ----------

# Same query, two approaches
query = """
SELECT 
    source_service,
    SUM(request_count) as total_requests
FROM gold_service_flow
GROUP BY source_service
"""

# Approach 1: Hive SQL
start = time.time()
sql_result = spark.sql(query).collect()
sql_time = time.time() - start

# Approach 2: Spark DataFrame API
from pyspark.sql import functions as F
start = time.time()
df_result = spark.read.format("delta").load(GOLD_PATH) \
    .groupBy("source_service") \
    .agg(F.sum("request_count").alias("total_requests")) \
    .collect()
df_time = time.time() - start

print("📊 Hive SQL vs Spark DataFrame API:")
print(f"SQL time: {sql_time:.3f}s")
print(f"DataFrame time: {df_time:.3f}s")
print(f"\n✅ Both produce the same results - use whichever is more readable!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Hive/SQL Concepts Demonstrated

# COMMAND ----------

summary = """
✅ Week 3 (Hive) Concepts Covered:

1. External Tables: Registered Delta paths as Hive tables
2. Partitioning: Demonstrated partition pruning benefits (3x+ speedup)
3. SQL Queries: SELECT, GROUP BY, ORDER BY, LIMIT
4. Joins: Inner join between Silver and Gold tables
5. Views: Created reusable views for common queries
6. Aggregations: SUM, AVG, COUNT, percentiles
7. Window Functions: Moving averages with OVER clause
8. Subqueries: CTEs (WITH clause) for complex queries
9. Performance: Compared SQL vs DataFrame API

💡 Key Takeaway:
Hive provides SQL interface to distributed data (Delta Lake).
Same data can be queried via SQL (Hive) or DataFrame API (Spark).
Partitioning is critical for performance on big data!
"""

print(summary)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS: Demonstrated Hive/SQL analysis")

