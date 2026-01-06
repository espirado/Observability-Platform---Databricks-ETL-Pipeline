# Observability Platform - Databricks ETL Pipeline

**Apache Spark ETL pipeline for processing observability data at scale**

## Overview

This repository contains the Databricks notebooks and job configurations for processing logs, metrics, and traces from distributed systems. The pipeline transforms raw telemetry data into analytical datasets optimized for visualization and incident analysis.

## Architecture

```
Data Sources (S3/ADLS/GCS)
    ↓
Delta Lake Bronze (Raw)
    ↓
Spark Transformations
    ↓
Delta Lake Silver (Enriched)
    ↓
Aggregations & ML
    ↓
Delta Lake Gold (Analytical)
    ↓
ClickHouse (Analytical Store)
```

## Data Sources

### LogHub - Real Production Logs
**Primary Data Source**: [LogHub](https://github.com/logpai/loghub) - A collection of system log datasets for AI-driven log analytics

**Available Datasets**:
- **HDFS** (Hadoop) - 11M+ lines of distributed file system logs
- **Spark** (Apache Spark) - 33K+ lines of data processing logs
- **Zookeeper** - 74K+ lines of coordination service logs
- **BGL** (Blue Gene/L) - 4.7M+ lines from supercomputer
- **OpenStack** - 207K+ lines from cloud platform
- **And 10+ more real production datasets**

**Why LogHub?**
- ✅ Real production logs (not synthetic)
- ✅ Publicly available (no privacy issues)
- ✅ Academic credibility (100+ research citations)
- ✅ Perfect for DS-610 big data project

## Data Flow

### Bronze Layer (Raw Ingestion)
- **Input**: LogHub datasets (HDFS, Spark, OpenStack, etc.) or JSON logs, OTel traces
- **Output**: Partitioned Delta tables by date
- **Processing**: Schema validation, deduplication
- **Retention**: 30 days

### Silver Layer (Enrichment)
- **Input**: Bronze Delta tables
- **Output**: Cleaned, enriched events with trace context
- **Processing**: 
  - Parse nested JSON fields
  - Extract trace/span relationships
  - Enrich with service metadata
  - Flatten complex structures
- **Retention**: 90 days

### Gold Layer (Analytics)
- **Input**: Silver Delta tables
- **Output**: Pre-aggregated datasets for visualization
- **Processing**:
  - Service flow edge statistics (hourly)
  - Infrastructure health snapshots (5-min)
  - Anomaly detection (MLlib)
  - Causality scoring
- **Retention**: 1 year

## Repository Structure

```
observability-databricks/
├── README.md
├── notebooks/
│   ├── 01_ingest_raw_logs.py          # Bronze: Raw log ingestion
│   ├── 02_enrich_events.py            # Silver: Event enrichment
│   ├── 03_build_flow_dataset.py       # Gold: Service flow analytics
│   ├── 04_build_topology_dataset.py   # Gold: Infrastructure topology
│   ├── 05_detect_anomalies.py         # Gold: ML anomaly detection
│   ├── 06_sync_to_clickhouse.py       # Export to analytical store
│   └── utils/
│       ├── schema_definitions.py      # Delta Lake schemas
│       ├── enrichment_functions.py    # UDFs for enrichment
│       └── clickhouse_writer.py       # Bulk insert helpers
├── jobs/
│   ├── hourly_etl.json               # Hourly batch job config
│   ├── streaming_critical.json       # Streaming job for alerts
│   └── nightly_backfill.json         # Historical recompute
├── schemas/
│   ├── bronze_logs.json              # Bronze layer schema
│   ├── silver_events.json            # Silver layer schema
│   └── gold_datasets.json            # Gold layer schemas
├── tests/
│   ├── test_enrichment.py            # Unit tests
│   ├── test_aggregations.py          # Unit tests
│   └── fixtures/                     # Sample data
├── config/
│   ├── cluster_config.json           # Databricks cluster spec
│   ├── mount_points.json             # Storage mount configs
│   └── secrets.json.example          # Secrets template
├── docs/
│   ├── SETUP.md                      # Databricks workspace setup
│   ├── DATA_MODEL.md                 # Delta Lake schema docs
│   └── DEPLOYMENT.md                 # Job deployment guide
└── requirements.txt                  # Python dependencies
```

## Quick Start

### Prerequisites

- Databricks workspace (AWS, Azure, or GCP)
- S3/ADLS/GCS bucket for Delta Lake storage
- ClickHouse instance (for final export)

### Setup

1. **Clone repository**
   ```bash
   git clone <repo-url>
   cd observability-databricks
   ```

2. **Import notebooks to Databricks**
   ```bash
   databricks workspace import_dir notebooks /Workspace/observability-etl
   ```

3. **Configure storage mount**
   ```python
   # In Databricks notebook
   dbutils.fs.mount(
       source="s3a://your-bucket/observability-data",
       mount_point="/mnt/observability",
       extra_configs={"spark.hadoop.fs.s3a.access.key": "...",
                      "spark.hadoop.fs.s3a.secret.key": "..."}
   )
   ```

4. **Create Delta tables**
   ```bash
   # Run setup notebook
   databricks runs submit --json @jobs/setup_tables.json
   ```

5. **Run ETL pipeline**
   ```bash
   # Test with sample data
   databricks runs submit --json @jobs/hourly_etl.json
   ```

## Notebooks

### 01_ingest_raw_logs.py
**Purpose**: Ingest raw logs into Delta Bronze layer

**Input**: 
- S3 path: `s3://bucket/raw-logs/YYYY-MM-DD/*.jsonl`
- Kafka topic: `observability.logs`

**Processing**:
```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Read raw JSON
raw_df = (spark.read
    .format("json")
    .option("multiLine", "false")
    .load("s3://bucket/raw-logs/2024-12-15/"))

# Add metadata
enriched_df = (raw_df
    .withColumn("ingestion_time", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("partition_date", F.to_date(F.col("timestamp"))))

# Write to Delta Bronze
(enriched_df.write
    .format("delta")
    .mode("append")
    .partitionBy("partition_date")
    .save("/mnt/observability/bronze/logs"))
```

**Output**: Delta table at `/mnt/observability/bronze/logs`

---

### 02_enrich_events.py
**Purpose**: Parse and enrich logs into Silver events

**Processing**:
- Flatten nested JSON (`http.*`, `kubernetes.*`, `trace.*`)
- Parse timestamps to consistent format
- Extract service/endpoint from various formats
- Join with service metadata table
- Deduplicate by `trace_id + span_id`

**Output**: Delta table at `/mnt/observability/silver/events`

**Key Transformations**:
```python
from pyspark.sql import functions as F

enriched = (bronze_df
    # Flatten nested fields
    .withColumn("service", F.coalesce(
        F.col("service"),
        F.col("kubernetes.labels.app"),
        F.col("serviceName")
    ))
    .withColumn("http_method", F.col("http.method"))
    .withColumn("http_path", F.col("http.path"))
    .withColumn("http_status", F.col("http.status_code"))
    
    # Parse timestamps
    .withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
    
    # Extract source/target from trace context
    .withColumn("source_service", 
        F.when(F.col("span_kind") == "CLIENT", F.col("service"))
         .otherwise(F.lag("service").over(trace_window)))
    .withColumn("target_service",
        F.when(F.col("span_kind") == "SERVER", F.col("service"))
         .otherwise(F.col("service")))
)
```

---

### 03_build_flow_dataset.py
**Purpose**: Aggregate service flow edges (Gold Dataset 1)

**Processing**:
- Group by `(hour, source_service, target_service, endpoint)`
- Calculate:
  - Request count
  - Error count, error rate
  - Latency percentiles (p50, p95, p99)
  - First/last seen timestamps
- Identify critical paths using graph algorithms

**Output**: Delta table at `/mnt/observability/gold/service_flow_edges`

**Aggregation Example**:
```python
from pyspark.sql import functions as F

flow_edges = (silver_events
    .withColumn("hour", F.date_trunc("hour", F.col("timestamp")))
    .groupBy("hour", "source_service", "target_service", "endpoint")
    .agg(
        F.count("*").alias("request_count"),
        F.sum(F.when(F.col("status_code") >= 500, 1).otherwise(0)).alias("error_count"),
        F.mean("latency_ms").alias("avg_latency"),
        F.expr("percentile_approx(latency_ms, 0.50)").alias("p50_latency"),
        F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency"),
        F.expr("percentile_approx(latency_ms, 0.99)").alias("p99_latency"),
        F.min("timestamp").alias("first_seen"),
        F.max("timestamp").alias("last_seen")
    )
    .withColumn("error_rate", F.col("error_count") / F.col("request_count"))
)
```

---

### 04_build_topology_dataset.py
**Purpose**: Build infrastructure topology (Gold Dataset 2)

**Processing**:
- Extract entities (services, pods, nodes, zones)
- Build relationships (pod→node, service→pod)
- Aggregate health metrics (CPU, memory, saturation)
- Detect status changes

**Output**: 
- `gold/entities` - Infrastructure components
- `gold/relations` - Entity relationships
- `gold/entity_snapshots` - Time-series metrics

---

### 05_detect_anomalies.py
**Purpose**: ML-based anomaly detection

**Processing**:
- Train Isolation Forest on normal behavior
- Score new events for anomalies
- Flag high saturation, status degradation
- Correlate infra anomalies with service errors

**Output**: Delta table at `/mnt/observability/gold/anomalies`

**ML Pipeline**:
```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# Feature engineering
feature_cols = ["request_count", "error_rate", "avg_latency", "cpu_pct", "memory_pct"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Train clustering model to identify normal behavior
kmeans = KMeans(k=5, featuresCol="features", predictionCol="cluster")
model = kmeans.fit(historical_data)

# Score new data
scored = model.transform(current_data)
anomalies = scored.filter(F.col("cluster") == 4)  # Outlier cluster
```

---

### 06_sync_to_clickhouse.py
**Purpose**: Bulk export Gold datasets to ClickHouse

**Processing**:
- Read Gold Delta tables
- Batch into 100k row chunks
- Bulk insert to ClickHouse via JDBC

**Output**: Data in ClickHouse `observability` database

**Export Example**:
```python
from clickhouse_driver import Client

# Read Gold dataset
gold_df = spark.read.format("delta").load("/mnt/observability/gold/service_flow_edges")

# Convert to pandas for bulk insert (or use native JDBC)
batch_size = 100000
for batch in gold_df.toLocalIterator(prefetchPartitions=True):
    pandas_df = batch.toPandas()
    
    # Bulk insert
    client.insert_dataframe(
        "INSERT INTO observability.service_flow_edges_hourly VALUES",
        pandas_df
    )
```

---

## Spark Job Configurations

### Hourly ETL Job
**File**: `jobs/hourly_etl.json`

**Schedule**: Every hour at :00  
**Cluster**: 4 workers (i3.xlarge), 8 cores, 32GB RAM  
**Notebooks**: Run in sequence:
1. `01_ingest_raw_logs` (10 min)
2. `02_enrich_events` (15 min)
3. `03_build_flow_dataset` (10 min)
4. `04_build_topology_dataset` (10 min)
5. `06_sync_to_clickhouse` (5 min)

**Timeout**: 60 minutes  
**Retry**: 2 attempts

### Streaming Critical Job
**File**: `jobs/streaming_critical.json`

**Schedule**: Continuous (Structured Streaming)  
**Cluster**: 2 workers, auto-scaling  
**Input**: Kafka topic `observability.logs.critical`  
**Processing**: Near real-time (5 sec batches)  
**Output**: Gold layer + immediate ClickHouse push

---

## Data Model

### Bronze: Raw Logs
```json
{
  "timestamp": "2024-12-15T14:23:45.123Z",
  "message": "Request completed",
  "level": "INFO",
  "service": "payment-service",
  "http": {
    "method": "POST",
    "path": "/api/v1/checkout",
    "status_code": 200,
    "latency_ms": 145.3
  },
  "trace": {
    "trace_id": "abc123",
    "span_id": "def456",
    "parent_span_id": "ghi789"
  },
  "kubernetes": {
    "pod": "payment-service-7c45d9f9cf-lkqj9",
    "node": "ip-10-0-1-12",
    "namespace": "production"
  },
  "ingestion_time": "2024-12-15T14:24:00.000Z",
  "partition_date": "2024-12-15"
}
```

### Silver: Enriched Events
```python
silver_events = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("trace_id", StringType(), False),
    StructField("span_id", StringType(), False),
    StructField("parent_span_id", StringType(), True),
    StructField("source_service", StringType(), False),
    StructField("target_service", StringType(), False),
    StructField("endpoint", StringType(), False),
    StructField("method", StringType(), True),
    StructField("status_code", IntegerType(), False),
    StructField("latency_ms", DoubleType(), True),
    StructField("error_message", StringType(), True),
    StructField("pod_id", StringType(), True),
    StructField("node_id", StringType(), True),
    StructField("partition_date", DateType(), False)
])
```

### Gold: Service Flow Edges
```python
service_flow_edges = StructType([
    StructField("window_start", TimestampType(), False),
    StructField("source_service", StringType(), False),
    StructField("target_service", StringType(), False),
    StructField("endpoint", StringType(), False),
    StructField("request_count", LongType(), False),
    StructField("error_count", LongType(), False),
    StructField("error_rate", DoubleType(), False),
    StructField("avg_latency", DoubleType(), True),
    StructField("p50_latency", DoubleType(), True),
    StructField("p95_latency", DoubleType(), True),
    StructField("p99_latency", DoubleType(), True)
])
```

---

## Performance Optimization

### Partitioning
```python
# Partition by date for efficient pruning
.partitionBy("partition_date")

# Z-order for co-located reads
spark.sql("OPTIMIZE delta.`/mnt/observability/silver/events` ZORDER BY (service, trace_id)")
```

### Caching
```python
# Cache frequently accessed tables
spark.sql("CACHE TABLE service_metadata")

# Persist intermediate DataFrames
enriched_df = bronze_df.transform(enrich_events)
enriched_df.cache()
```

### Auto-scaling
```json
{
  "autoscale": {
    "min_workers": 2,
    "max_workers": 20
  },
  "spark_conf": {
    "spark.dynamicAllocation.enabled": "true",
    "spark.shuffle.service.enabled": "true"
  }
}
```

---

## Cost Estimates

### Cluster Costs (AWS, us-east-1)

| Job Type | Cluster Size | Runtime | Frequency | Monthly Cost |
|----------|--------------|---------|-----------|--------------|
| Hourly ETL | 4 × i3.xlarge | 30 min | 720x/month | $1,200 |
| Streaming | 2 × i3.large | 24/7 | Continuous | $800 |
| Nightly Backfill | 8 × i3.2xlarge | 2 hours | 30x/month | $600 |
| **Total** | | | | **$2,600/month** |

**Optimization**: Use spot instances (70% savings) → **$780/month**

### Storage Costs (S3)

| Layer | Retention | Size (1M events/day) | Monthly Cost |
|-------|-----------|----------------------|--------------|
| Bronze | 30 days | 15 GB/day × 30 = 450 GB | $10 |
| Silver | 90 days | 10 GB/day × 90 = 900 GB | $20 |
| Gold | 1 year | 1 GB/day × 365 = 365 GB | $8 |
| **Total** | | 1.7 TB | **$38/month** |

---

## Testing

### Unit Tests
```bash
# Local Spark testing
pytest tests/ -v

# Sample output:
# tests/test_enrichment.py::test_flatten_nested_json PASSED
# tests/test_aggregations.py::test_hourly_rollup PASSED
```

### Integration Tests
```python
# Test full Bronze → Silver → Gold pipeline
def test_etl_pipeline():
    # Ingest sample data to Bronze
    sample_logs = spark.read.json("tests/fixtures/sample_logs.jsonl")
    sample_logs.write.format("delta").save("/tmp/bronze")
    
    # Run enrichment
    run_notebook("02_enrich_events", {"input_path": "/tmp/bronze"})
    
    # Validate Silver output
    silver = spark.read.format("delta").load("/tmp/silver")
    assert silver.count() == 100
    assert silver.filter("source_service IS NULL").count() == 0
```

---

## Monitoring

### Databricks Job Metrics
- Job success/failure rate
- Runtime duration (alert if > 60 min)
- Data processed per run
- Cluster utilization

### Data Quality Checks
```python
# In each notebook, add quality checks
def validate_silver_events(df):
    # No nulls in required fields
    assert df.filter("timestamp IS NULL").count() == 0
    assert df.filter("source_service IS NULL").count() == 0
    
    # Reasonable latency range
    assert df.agg(F.max("latency_ms")).collect()[0][0] < 60000  # < 1 min
    
    # Status codes valid
    assert df.filter("status_code NOT BETWEEN 100 AND 599").count() == 0
```

---

## Deployment

### CI/CD Pipeline
```yaml
# .github/workflows/deploy-databricks.yml
name: Deploy Notebooks

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Install Databricks CLI
        run: pip install databricks-cli
      
      - name: Deploy notebooks
        run: |
          databricks workspace import_dir notebooks /Workspace/observability-etl --overwrite
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

---

## Related Repositories

- **[Observability Platform](../project3)** - Main repository with backend, UI, documentation
- **[ClickHouse Configs](../observability-clickhouse)** - Analytical storage layer (optional separate repo)

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## License

MIT License - See [LICENSE](LICENSE)

---

**Questions?**  
Andrew Espira  
Course: Big Data Analytics  
Last Updated: December 15, 2024

