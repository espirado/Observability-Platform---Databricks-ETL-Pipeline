# Week 1-4 Implementation Guide

## Overview

This guide walks you through implementing the **foundational concepts** for DS-610 Weeks 1-4:
- **Week 1**: Introduction & Setup
- **Week 2**: Apache Hadoop (Delta Lake)
- **Week 3**: Apache Hive (SQL queries)
- **Week 4**: Apache Spark (DataFrames & Aggregations)

By the end, you'll have a complete working pipeline ready for your **Week 5 proposal**.

---

## Week 1: Introduction & Project Setup

### Learning Objectives
- Set up Databricks environment
- Understand the observability use case
- Generate sample data for testing

### Step 1: Create Databricks Account

**Option A: Community Edition** (Free, recommended for learning)
1. Go to: https://community.cloud.databricks.com/
2. Sign up with email
3. Verify account

**Option B: Saint Peter's University** (If provided by instructor)
1. Log in to SPU Databricks system
2. Screenshot for Precheck Assignment

### Step 2: Import Notebooks

```bash
# Clone the repository
git clone https://github.com/espirado/Observability-Platform---Databricks-ETL-Pipeline.git
cd Observability-Platform---Databricks-ETL-Pipeline

# Import to Databricks workspace
databricks workspace import_dir notebooks /Workspace/observability-etl
```

**Or manually via UI**:
1. In Databricks → Workspace → Create → Folder → "observability-etl"
2. Upload each `.py` file from `notebooks/` folder

### Step 3: Create Storage Mount (Optional for Community Edition)

For Community Edition, you can use DBFS directly:
```python
# In a notebook
BASE_PATH = "/dbfs/observability-data"
dbutils.fs.mkdirs(BASE_PATH)
```

For production/university accounts, see [SETUP.md](SETUP.md) for S3/ADLS mount instructions.

### Step 4: Generate Sample Data

**Run Notebook 00**: `00_generate_sample_data.py`

This creates:
- 100,000 synthetic log events
- Realistic microservice trace patterns
- 24 hours of time-series data

**Output**: `/dbfs/observability-data/sample-logs/sample.jsonl`

**Expected Runtime**: 2-3 minutes

### Week 1 Deliverable
✅ Databricks account created  
✅ Notebooks imported  
✅ Sample data generated  
✅ Screenshot for Precheck Assignment

---

## Week 2: Apache Hadoop (Delta Lake)

### Learning Objectives
- Understand distributed storage (Hadoop HDFS → Delta Lake)
- Implement Bronze layer (raw data ingestion)
- Learn partitioning strategies
- Use Delta Lake features (ACID transactions, time travel)

### Hadoop Concepts in Delta Lake

| Traditional Hadoop | Delta Lake Equivalent |
|-------------------|----------------------|
| HDFS | Cloud storage (S3/ADLS/DBFS) |
| Parquet files | Delta files (Parquet + transaction log) |
| Partitioned directories | `.partitionBy()` |
| ACID transactions | ❌ Not supported | ✅ Built-in |

### Step 1: Run Notebook 01 (Bronze Ingestion)

**Notebook**: `01_ingest_raw_logs.py`

**What it does**:
1. Reads JSONL logs from DBFS
2. Validates schema
3. Adds metadata (ingestion time, source file)
4. Writes to Delta Lake Bronze layer
5. **Partitions by date** (Hadoop concept!)
6. Applies optimizations (compaction, Z-ordering)

**Run it**:
```python
# In Databricks
%run /Workspace/observability-etl/01_ingest_raw_logs

# Or via job:
databricks runs submit --json '{
  "run_name": "Bronze Ingestion",
  "existing_cluster_id": "<your-cluster-id>",
  "notebook_task": {
    "notebook_path": "/Workspace/observability-etl/01_ingest_raw_logs",
    "base_parameters": {
      "input_date": "2024-12-15"
    }
  }
}'
```

**Expected Runtime**: 3-5 minutes for 100K events

### Step 2: Explore Delta Lake Features

**Time Travel** (query historical versions):
```python
# In a new notebook
bronze_df = spark.read.format("delta").load("/mnt/observability/bronze/logs")

# Query specific version
version_1 = spark.read.format("delta") \
    .option("versionAsOf", 1) \
    .load("/mnt/observability/bronze/logs")

# Query at specific timestamp
yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-12-14T00:00:00Z") \
    .load("/mnt/observability/bronze/logs")
```

**View Table History**:
```sql
%sql
DESCRIBE HISTORY delta.`/mnt/observability/bronze/logs`
```

### Step 3: Understand Partitioning

```python
# View partition structure
display(dbutils.fs.ls("/mnt/observability/bronze/logs"))

# Output should show:
# partition_date=2024-12-15/
#     part-00000-abc.snappy.parquet
#     part-00001-def.snappy.parquet
```

**Benefits of Partitioning**:
- ✅ Skip irrelevant partitions (partition pruning)
- ✅ Faster queries on date ranges
- ✅ Easier data lifecycle management

### Week 2 Deliverable
✅ Bronze layer created with 100K events  
✅ Data partitioned by date  
✅ Time travel demonstrated  
✅ Understand Hadoop → Delta Lake mapping

---

## Week 3: Apache Hive (SQL Queries)

### Learning Objectives
- Create Hive external tables
- Write SQL queries on distributed data
- Understand partition pruning
- Create views and CTEs

### Step 1: Run Notebook 02 (Silver Enrichment)

**Notebook**: `02_enrich_events.py`

**What it does**:
1. Reads Bronze Delta table
2. Flattens nested JSON
3. Extracts source/target services from trace context
4. Enriches with metadata
5. Writes to Silver layer

**Run it**:
```python
%run /Workspace/observability-etl/02_enrich_events
```

**Expected Runtime**: 5-7 minutes for 100K events

### Step 2: Run Notebook 04 (Hive/SQL Analysis)

**Notebook**: `04_hive_sql_analysis.py`

**What it does**:
1. Creates Hive external tables on Delta paths
2. Demonstrates SQL queries (SELECT, JOIN, GROUP BY)
3. Shows partition pruning benefits
4. Creates views for common queries
5. Uses window functions and CTEs

**Run it**:
```python
%run /Workspace/observability-etl/04_hive_sql_analysis
```

**Expected Runtime**: 10-15 minutes (includes multiple queries)

### Step 3: Practice SQL Queries

Try these queries in a SQL notebook:

**Query 1: Top Services by Traffic**
```sql
USE observability;

SELECT 
    source_service,
    COUNT(*) as request_count,
    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) as error_count
FROM silver_events
WHERE partition_date = '2024-12-15'
GROUP BY source_service
ORDER BY request_count DESC;
```

**Query 2: Error Timeline**
```sql
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    source_service,
    COUNT(*) as errors
FROM silver_events
WHERE is_error = true
GROUP BY hour, source_service
ORDER BY hour, errors DESC;
```

**Query 3: Latency Analysis**
```sql
SELECT 
    target_service,
    endpoint,
    ROUND(AVG(latency_ms), 2) as avg_latency,
    ROUND(PERCENTILE_APPROX(latency_ms, 0.95), 2) as p95_latency
FROM silver_events
WHERE partition_date = '2024-12-15'
GROUP BY target_service, endpoint
HAVING COUNT(*) > 10
ORDER BY p95_latency DESC;
```

### Week 3 Deliverable
✅ Silver layer created  
✅ Hive tables registered  
✅ SQL queries executed successfully  
✅ Understand Hive on Delta Lake

---

## Week 4: Apache Spark (Aggregations)

### Learning Objectives
- Use Spark DataFrame API
- Perform complex aggregations
- Calculate percentiles and window functions
- Understand Spark vs Hive trade-offs

### Step 1: Run Notebook 03 (Gold Aggregation)

**Notebook**: `03_build_flow_dataset.py`

**What it does**:
1. Reads Silver events
2. Aggregates by (hour, source, target, endpoint)
3. Calculates:
   - Request counts
   - Error rates
   - Latency percentiles (p50, p95, p99)
4. Preserves trace samples for context
5. Writes to Gold layer

**Run it**:
```python
%run /Workspace/observability-etl/03_build_flow_dataset
```

**Expected Runtime**: 7-10 minutes for 100K events

### Step 2: Understand Aggregations

**Key Spark Operations Used**:

```python
# Group and aggregate
hourly_edges = silver_df \
    .withColumn("hour", F.date_trunc("hour", F.col("timestamp"))) \
    .groupBy("hour", "source_service", "target_service", "endpoint") \
    .agg(
        F.count("*").alias("request_count"),
        F.avg("latency_ms").alias("avg_latency"),
        F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency")
    )
```

**Percentiles** (compute-intensive but valuable):
```python
# Approximate percentiles (faster, 99% accurate)
F.expr("percentile_approx(latency_ms, 0.95)")

# Exact percentiles (slower, 100% accurate)
F.expr("percentile(latency_ms, 0.95)")
```

### Step 3: Compare Spark vs Hive SQL

**Same Query, Two Ways**:

**Spark DataFrame API**:
```python
from pyspark.sql import functions as F

result = (spark.read.format("delta").load(GOLD_PATH)
    .groupBy("source_service")
    .agg(F.sum("request_count").alias("total_requests"))
    .orderBy(F.col("total_requests").desc()))
```

**Hive SQL**:
```sql
SELECT 
    source_service,
    SUM(request_count) as total_requests
FROM gold_service_flow
GROUP BY source_service
ORDER BY total_requests DESC;
```

**When to use which**:
- **SQL**: Simple queries, exploratory analysis, familiar syntax
- **DataFrame API**: Complex transformations, programmatic pipelines, type safety

### Step 4: Analyze Results

```python
# Read Gold table
gold_df = spark.read.format("delta").load("/mnt/observability/gold/service_flow_edges")

# Show aggregation quality
gold_df.show(10, truncate=False)

# Data reduction metrics
silver_count = spark.read.format("delta").load("/mnt/observability/silver/events").count()
gold_count = gold_df.count()

print(f"Silver events: {silver_count:,}")
print(f"Gold edges: {gold_count:,}")
print(f"Reduction: {silver_count / gold_count:.1f}x")
```

**Expected Results**:
- Silver: ~100,000 events
- Gold: ~200-500 edges
- Reduction: ~200-500x

### Week 4 Deliverable
✅ Gold layer created  
✅ Hourly aggregations computed  
✅ Latency percentiles calculated  
✅ Understand Spark DataFrame API

---

## Complete Pipeline Verification

### Run End-to-End Test

```python
# In a new notebook
print("🧪 End-to-End Pipeline Test\n")

# 1. Check Bronze
bronze_df = spark.read.format("delta").load("/mnt/observability/bronze/logs")
bronze_count = bronze_df.count()
print(f"✅ Bronze: {bronze_count:,} raw logs")

# 2. Check Silver
silver_df = spark.read.format("delta").load("/mnt/observability/silver/events")
silver_count = silver_df.count()
print(f"✅ Silver: {silver_count:,} enriched events")

# 3. Check Gold
gold_df = spark.read.format("delta").load("/mnt/observability/gold/service_flow_edges")
gold_count = gold_df.count()
print(f"✅ Gold: {gold_count:,} aggregated edges")

# 4. Validate totals
gold_total = gold_df.agg(F.sum("request_count")).collect()[0][0]
print(f"\n📊 Validation:")
print(f"   Silver total events: {silver_count:,}")
print(f"   Gold sum of requests: {gold_total:,}")
print(f"   Match: {'✅ PASS' if gold_total == silver_count else '❌ FAIL'}")
```

### Performance Metrics

Run this to generate metrics for your proposal:

```python
import time

# Query performance test
start = time.time()
silver_query = silver_df.groupBy("source_service").agg(F.count("*")).collect()
silver_time = time.time() - start

start = time.time()
gold_query = gold_df.groupBy("source_service").agg(F.sum("request_count")).collect()
gold_time = time.time() - start

print(f"⚡ Query Performance:")
print(f"   Silver: {silver_time:.2f}s")
print(f"   Gold: {gold_time:.2f}s")
print(f"   Speedup: {silver_time/gold_time:.1f}x")
```

---

## Week 5 Proposal Preparation

### What to Include

**1. Problem Statement**
> "How can we reduce log storage costs while preserving full context for ML models?"

**2. Your Approach**
> "Medallion architecture (Bronze/Silver/Gold) with context-preserving aggregation"

**3. Implemented So Far** (Weeks 1-4):
- ✅ Bronze: Raw log ingestion with Delta Lake partitioning
- ✅ Silver: Event enrichment with trace correlation
- ✅ Gold: Hourly aggregation with trace sample preservation
- ✅ Hive: SQL interface for analysis

**4. Metrics to Report**:
```
Dataset: 100,000 synthetic log events (1 day)
Bronze size: ~50 MB (raw)
Silver size: ~40 MB (enriched)
Gold size: ~2 MB (aggregated)
Reduction: 25x storage savings
Query speedup: 15-20x faster on Gold vs Silver
Context preserved: 100% of error traces retained
```

**5. Next Steps** (Weeks 5-9):
- Week 6: MLlib anomaly detection
- Week 7: Performance optimization
- Week 8: Streaming integration (optional)
- Week 9: Production data integration
- Week 10: Final presentation

### Sample Proposal Text

See the full example in [RESEARCH_GOALS.md](../RESEARCH_GOALS.md)

---

## Troubleshooting

### "No such file or directory" Error

**Problem**: Delta path doesn't exist

**Solution**:
```python
# Check if path exists
display(dbutils.fs.ls("/mnt/observability"))

# Create directories if needed
dbutils.fs.mkdirs("/mnt/observability/bronze/logs")
dbutils.fs.mkdirs("/mnt/observability/silver/events")
dbutils.fs.mkdirs("/mnt/observability/gold/service_flow_edges")
```

### "Table not found" Error (Hive)

**Problem**: External table not registered

**Solution**:
```sql
-- Re-create table
CREATE TABLE IF NOT EXISTS observability.silver_events
USING DELTA
LOCATION '/mnt/observability/silver/events';
```

### Notebook Fails with "Out of Memory"

**Problem**: Cluster too small for dataset

**Solution**:
1. Reduce sample size in Notebook 00 (e.g., 10K events instead of 100K)
2. Increase cluster size (add more workers)
3. Enable auto-scaling

### Performance is Slow

**Solution**:
```python
# Enable optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Run OPTIMIZE manually
spark.sql("OPTIMIZE delta.`/mnt/observability/silver/events`")
```

---

## Summary Checklist

### Week 1
- [ ] Databricks account created
- [ ] Notebooks imported
- [ ] Sample data generated (100K events)
- [ ] Precheck assignment screenshot taken

### Week 2
- [ ] Notebook 01 completed (Bronze layer)
- [ ] Delta Lake partitioning verified
- [ ] Time travel demonstrated
- [ ] Understand Hadoop concepts

### Week 3
- [ ] Notebook 02 completed (Silver layer)
- [ ] Notebook 04 completed (Hive SQL)
- [ ] Hive tables created
- [ ] SQL queries executed

### Week 4
- [ ] Notebook 03 completed (Gold layer)
- [ ] Aggregations verified
- [ ] Performance metrics collected
- [ ] End-to-end pipeline tested

### Week 5 Ready
- [ ] All notebooks running successfully
- [ ] Metrics documented
- [ ] Proposal draft written
- [ ] Ready to collect production data (optional)

---

## Next Steps

1. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: completed Week 1-4 implementation"
   git push
   ```

2. **Schedule instructor check-in** (if required)

3. **Write Week 5 proposal** using [RESEARCH_GOALS.md](../RESEARCH_GOALS.md) as template

4. **Optional**: Start collecting real production logs (see [PRODUCTION_DATA_GUIDE.md](PRODUCTION_DATA_GUIDE.md))

---

**Questions?** Open an issue on GitHub or email your instructor.

**Last Updated**: December 15, 2024

