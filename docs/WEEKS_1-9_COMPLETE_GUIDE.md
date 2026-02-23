# Complete Week 1-9 Guide: DS-610 Big Data Analytics

**Course**: DS-610 Big Data Analytics  
**Project**: Context-Preserving Log Analysis Pipeline  
**Platform**: Databricks with Apache Spark  
**Data Source**: Real production logs from LogHub (https://github.com/logpai/loghub)

---

## 📅 Weekly Schedule Overview

| Week | Topic | Notebooks | Concepts |
|------|-------|-----------|----------|
| **1** | Intro to Big Data | `00_ingest_from_loghub.py` | Big data challenges, distributed storage |
| **2** | Apache Hadoop | `01_ingest_raw_logs.py` | MapReduce, HDFS, Bronze layer |
| **3** | Apache Hive | `02_enrich_events.py`, `04_hive_sql_analysis.py` | Hive tables, SQL, Silver layer |
| **4** | Apache Spark | `03_build_flow_dataset.py` | DataFrames, aggregations, Gold layer |
| **5** | Proposal | *(submitted)* | Project scoping |
| **6** | MLlib | `06_anomaly_detection_mllib.py` | K-Means, feature engineering, ML pipeline |
| **7** | RDDs | `07_log_parsing_with_rdds.py` | map, flatMap, reduceByKey, partitioning |
| **8** | Streaming | `08_streaming_log_analysis.py` | Structured Streaming, watermarks, windows |
| **9** | NoSQL | `09_nosql_log_storage.py` | HBase patterns, row keys, CAP theorem |

> **Bonus Notebooks** (separate from weekly coursework):
>
> | Notebook | Purpose |
> |----------|---------|
> | `10_dashboard_video.py` | Dashboard visualization + narrated MP4 video |
> | `11_ai_dashboard_analytics.py` | AI-powered analytics, LLM insights, Data Copilot |

---

## 🎯 Learning Objectives

By completing this pipeline, you will demonstrate:

### Technical Skills
- ✅ **Big Data Processing**: Ingest and process millions of production logs
- ✅ **Hadoop Ecosystem**: Understand HDFS, MapReduce, Hive, Spark
- ✅ **Machine Learning**: Train anomaly detection models with MLlib
- ✅ **Streaming Analytics**: Process real-time data with Structured Streaming
- ✅ **NoSQL Concepts**: Design efficient key-value storage patterns

### Real-World Impact
- ✅ **Context Preservation**: Maintain trace IDs through 200x data reduction
- ✅ **Query Optimization**: 15s → 0.2s speedup with proper layering
- ✅ **Academic Rigor**: Use datasets cited by 100+ research papers
- ✅ **Production Patterns**: Delta Lake, Z-ordering, partitioning

---

## 📊 Data Flow Architecture

```
LogHub Raw Logs (HDFS/Spark/OpenStack/BGL)
    ↓
[Week 1] 00_ingest_from_loghub.py
    ↓ Enhanced JSONL
[Week 2] 01_ingest_raw_logs.py (Bronze - Raw ingestion)
    ↓ Delta Bronze Table
[Week 3] 02_enrich_events.py (Silver - Enrichment)
    ↓ Delta Silver Table
[Week 4] 03_build_flow_dataset.py (Gold - Aggregations)
    ↓ Delta Gold Table (Service Edges)
    ├─→ [Week 3] 04_hive_sql_analysis.py (SQL Analytics)
    ├─→ [Week 6] 06_anomaly_detection_mllib.py (ML)
    ├─→ [Week 7] 07_log_parsing_with_rdds.py (RDD Processing)
    ├─→ [Week 8] 08_streaming_log_analysis.py (Real-time)
    ├─→ [Week 9] 09_nosql_log_storage.py (NoSQL)
    │
    ├─→ [Bonus] 10_dashboard_video.py (Dashboard + Narrated Video)
    └─→ [Bonus] 11_ai_dashboard_analytics.py (AI Analytics + Data Copilot)
```

---

## 🚀 Quick Start

### Prerequisites
1. **Databricks Workspace** (Community Edition or full)
2. **Cluster**: Databricks Runtime 13.3 LTS or higher
3. **Storage**: `/mnt/observability/` or `/dbfs/observability-data/`

### Import Notebooks
**Option 1: Databricks Repos (Recommended)**
```
1. Go to: Workspace → Repos → Add Repo
2. Paste: https://github.com/espirado/Observability-Platform---Databricks-ETL-Pipeline
3. Clone
```

**Option 2: Manual Upload**
```
1. Download all .py files from notebooks/
2. Go to: Workspace → Import
3. Upload each notebook
```

### Create Cluster
```
Name: observability-cluster
Runtime: 13.3 LTS (Scala 2.12, Spark 3.4.1)
Workers: 2-4 (or 1 for Community Edition)
Auto-termination: 30 minutes
```

---

## 📖 Week-by-Week Execution Guide

### **Week 1: Introduction to Big Data Analytics**

**Notebook**: `00_ingest_from_loghub.py`

**Objectives**:
- Define big data and analysis challenges
- Download real production logs from LogHub
- Parse unstructured logs into structured JSON

**Steps**:
1. Open `00_ingest_from_loghub.py`
2. Attach to cluster
3. Set parameters:
   - **Dataset**: `HDFS` (or `Spark`, `OpenStack`, `BGL`)
   - **Sample Size**: `100000` (or `0` for all)
4. Click **Run All**

**Expected Output**:
```
✅ Downloaded 100,000 logs from LogHub-HDFS
📄 Enhanced with observability context (trace IDs, K8s metadata)
💾 Written to /dbfs/observability-data/loghub/HDFS_enhanced.jsonl
```

**Key Concepts Demonstrated**:
- Big data scale (millions of logs)
- Distributed storage (DBFS)
- Schema-on-read vs schema-on-write

---

### **Week 2: Apache Hadoop (MapReduce)**

**Notebook**: `01_ingest_raw_logs.py`

**Objectives**:
- Understand MapReduce paradigm
- Ingest raw logs into Hadoop-compatible storage (Delta Bronze)
- Partition data for distributed processing

**Steps**:
1. Open `01_ingest_raw_logs.py`
2. Set parameters:
   - **input_date**: `2024-12-15` (or today's date)
3. Click **Run All**

**Expected Output**:
```
✅ Ingested 100,000 records to Bronze layer
📂 Partitioned by date: /mnt/observability/bronze/events_raw
🔧 Ready for Silver processing
```

**Key Concepts Demonstrated**:
- MapReduce pattern (read → transform → write)
- HDFS-style partitioning
- Bronze layer (raw, immutable data)

---

### **Week 3: Apache Hive**

**Notebooks**: `02_enrich_events.py` + `04_hive_sql_analysis.py`

**Objectives**:
- Create Hive-compatible Delta tables
- Enrich events with derived fields (Silver layer)
- Query logs with SQL

**Steps**:
1. **Run Silver Enrichment**:
   - Open `02_enrich_events.py`
   - Set **input_date**: `2024-12-15`
   - Click **Run All**

2. **Run Hive SQL Analysis**:
   - Open `04_hive_sql_analysis.py`
   - Click **Run All**

**Expected Output**:
```
✅ Silver layer: 100,000 enriched events
📊 Hive tables created: events_raw, events_silver
🔍 SQL queries executed:
   - Top service edges by error rate
   - Latency percentiles by endpoint
   - Hourly traffic patterns
```

**Key Concepts Demonstrated**:
- Hive table DDL (CREATE TABLE)
- SQL aggregations (GROUP BY, HAVING)
- External vs managed tables
- Query optimization (partitioning, columnar format)

---

### **Week 4: Introduction to Apache Spark**

**Notebook**: `03_build_flow_dataset.py`

**Objectives**:
- Use Spark DataFrame API for transformations
- Aggregate events into service flow edges (Gold layer)
- Demonstrate 200x data reduction with context preservation

**Steps**:
1. Open `03_build_flow_dataset.py`
2. Set **input_date**: `2024-12-15`
3. Click **Run All**

**Expected Output**:
```
✅ Gold layer: 500 service edges (from 100,000 events = 200x reduction)
📈 Aggregated metrics: request counts, error rates, latency percentiles
🔗 Context preserved: Sample trace IDs kept for drilldown
⚡ Query speedup: 15s → 0.2s (75x faster)
```

**Key Concepts Demonstrated**:
- Spark DataFrame API (select, groupBy, agg)
- Window functions
- Data reduction without loss of context
- Delta Lake optimization (OPTIMIZE, Z-ORDER)

---

### **Week 5: Final Project Proposal**

**Task**: Submit proposal

**Your Proposal Should Include**:
- **Problem Statement**: Observability for distributed systems
- **Data Source**: LogHub (HDFS, Spark, OpenStack logs)
- **Approach**: Bronze → Silver → Gold pipeline with context preservation
- **Expected Results**: 200x data reduction, <1s query latency, anomaly detection

---

### **Week 6: MLlib (Machine Learning on Spark)**

**Notebook**: `06_anomaly_detection_mllib.py`

**Objectives**:
- Train K-Means clustering model for anomaly detection
- Engineer features from log aggregations
- Identify anomalous service edges

**Steps**:
1. Open `06_anomaly_detection_mllib.py`
2. Set **input_date**: `2024-12-15`
3. Click **Run All**

**Expected Output**:
```
✅ Trained K-Means model (3 clusters)
📊 Silhouette score: 0.65
🚨 Detected 47 anomalies (5% of edges)
📁 Model saved to /mnt/observability/models/anomaly_detector
```

**Key Concepts Demonstrated**:
- MLlib Pipeline (VectorAssembler, StandardScaler, KMeans)
- Feature engineering (error rate, latency variance)
- Model training and evaluation
- Anomaly detection use case

---

### **Week 7: Unstructured APIs in Apache Spark (RDDs)**

**Notebook**: `07_log_parsing_with_rdds.py`

**Objectives**:
- Use RDD transformations for low-level log parsing
- Extract log templates with regex
- Compare RDD vs DataFrame performance

**Steps**:
1. Open `07_log_parsing_with_rdds.py`
2. Set **dataset**: `HDFS`
3. Click **Run All**

**Expected Output**:
```
✅ Parsed 100,000 log lines as RDD
🔍 Extracted 287 unique log templates
📝 Top tokens: "error", "failed", "connection", "timeout"
⚡ Performance: DataFrame 1.5x faster than RDD (but RDD gives more control)
```

**Key Concepts Demonstrated**:
- RDD transformations: map, flatMap, filter, reduceByKey
- Pair RDDs (key-value pairs)
- Custom partitioning
- When to use RDDs vs DataFrames

---

### **Week 8: Streaming Analytics**

**Notebook**: `08_streaming_log_analysis.py`

**Objectives**:
- Process logs in real-time with Structured Streaming
- Implement windowed aggregations (tumbling, sliding)
- Detect anomalies in streaming data

**Steps**:
1. Open `08_streaming_log_analysis.py`
2. Click **Run All** (notebook generates streaming data automatically)

**Expected Output**:
```
✅ Processed 900 streaming logs in 6 micro-batches
📊 Created 45 windowed metrics (1-minute tumbling windows)
🚨 Detected 8 anomalies in real-time
⏰ Watermark: 2 minutes (handles late data)
```

**Key Concepts Demonstrated**:
- Structured Streaming API
- Micro-batch processing
- Windowed aggregations (tumbling, sliding)
- Watermarks for late data
- Event time vs processing time

---

### **Week 9: NoSQL Databases (HBase Patterns)**

**Notebook**: `09_nosql_log_storage.py`

**Objectives**:
- Design HBase-like row keys for fast lookups
- Implement column family patterns
- Compare NoSQL vs SQL query performance

**Steps**:
1. Open `09_nosql_log_storage.py`
2. Click **Run All**

**Expected Output**:
```
✅ Created NoSQL table with row key: service#reverse_timestamp#target
📊 Point lookup: 12ms
📊 Range scan: 45ms (100 rows)
⚡ Row key lookup 3.2x faster than SQL filter
🗄️  Z-ordering optimized for row key scans
```

**Key Concepts Demonstrated**:
- HBase row key design (composite keys, reverse timestamp)
- Column families (simulated with structs)
- Point lookups vs range scans
- CAP theorem trade-offs (CP system)
- Delta Lake as NoSQL alternative

---

## 📊 Validation and Results

### Data Quality Checks
```sql
-- Validate Bronze → Silver → Gold
SELECT 
  'Bronze' as layer, COUNT(*) as records FROM delta.`/mnt/observability/bronze/events_raw`
UNION ALL
SELECT 'Silver' as layer, COUNT(*) as records FROM delta.`/mnt/observability/silver/events`
UNION ALL
SELECT 'Gold' as layer, COUNT(*) as records FROM delta.`/mnt/observability/gold/service_flow_edges`;
```

### Expected Metrics
| Metric | Value |
|--------|-------|
| Raw logs ingested | 100,000 |
| Silver events | 100,000 |
| Gold edges | 500 |
| Data reduction | 200x |
| Query speedup | 75x |
| Anomalies detected | 47 (5%) |
| Unique log templates | 287 |
| Streaming windows | 45 |
| NoSQL point lookup | <20ms |

---

## 📸 Screenshots for Presentation

### Must-Have Screenshots
1. **Workspace Structure** (All 9 notebooks)
2. **Cluster Configuration** (Runtime version, workers)
3. **Week 1**: LogHub download confirmation
4. **Week 3**: Hive SQL query results table
5. **Week 4**: Gold aggregation metrics (200x reduction)
6. **Week 6**: MLlib anomaly detection results
7. **Week 7**: RDD template extraction output
8. **Week 8**: Streaming dashboard (windowed metrics)
9. **Week 9**: NoSQL row key design diagram

---

## 🎥 Presentation Structure (25 minutes)

### Recommended Flow

**1. Introduction (3 min)**
- Problem: Distributed system observability at scale
- Solution: Context-preserving log pipeline
- Data: Real production logs from LogHub

**2. Weeks 1-4: Core Pipeline (7 min)**
- Ingest → Bronze → Silver → Gold
- Demonstrate: 200x reduction, 75x speedup
- Show: Hive SQL queries

**3. Week 6: Machine Learning (5 min)**
- MLlib anomaly detection
- Show: Trained model, detected anomalies
- Discuss: Feature engineering

**4. Week 7-8: Advanced Processing (5 min)**
- RDDs: Template extraction
- Streaming: Real-time windowed metrics

**5. Week 9: NoSQL Patterns (3 min)**
- HBase-like row key design
- Point lookups vs range scans
- CAP theorem

**6. Results and Q&A (2 min)**
- Summary metrics table
- Academic and industry relevance

---

## 🔧 Troubleshooting

### Common Issues

**Issue 1**: `FileNotFoundError: /dbfs/observability-data/loghub/...`
- **Fix**: Run notebook 00 first to download LogHub data

**Issue 2**: `Table not found: delta.\`/mnt/observability/bronze/events_raw\``
- **Fix**: Run notebooks in order (00 → 01 → 02 → 03 → ...)

**Issue 3**: Streaming query stuck
- **Fix**: Check `STREAM_SOURCE_PATH` has data files; wait 30s for micro-batch

**Issue 4**: Memory error during MLlib training
- **Fix**: Reduce sample size or increase cluster workers

**Issue 5**: Z-ordering takes too long
- **Fix**: Use `WHERE partition_date = '...'` to optimize specific partitions

---

## 📚 Additional Resources

### LogHub Datasets
- **HDFS**: Hadoop distributed file system logs (11M lines)
- **Spark**: Apache Spark driver/executor logs (33K lines)
- **OpenStack**: Cloud infrastructure logs (207K lines)
- **BGL**: Blue Gene/L supercomputer logs (4.7M lines)

### Delta Lake Docs
- [Delta Lake Quickstart](https://docs.delta.io/latest/quick-start.html)
- [OPTIMIZE](https://docs.delta.io/latest/optimizations-oss.html)
- [Z-Ordering](https://docs.delta.io/latest/optimizations-oss.html#z-ordering-multi-dimensional-clustering)

### Spark Docs
- [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [MLlib](https://spark.apache.org/docs/latest/ml-guide.html)
- [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)

---

## ✅ Success Criteria

You've successfully completed DS-610 if you can:

- ✅ Run all 9 notebooks end-to-end
- ✅ Explain Bronze → Silver → Gold layering
- ✅ Demonstrate 200x data reduction with context preservation
- ✅ Train and evaluate an MLlib anomaly detection model
- ✅ Show RDD transformations (map, flatMap, reduceByKey)
- ✅ Process streaming logs with windowed aggregations
- ✅ Design efficient NoSQL row keys for fast lookups
- ✅ Discuss CAP theorem trade-offs

---

## 🎓 Academic Citations

When referencing LogHub in your report:

```
@inproceedings{zhu2019tools,
  title={Tools and benchmarks for automated log parsing},
  author={Zhu, Jieming and He, Shilin and Liu, Jinyang and He, Pinjia and Xie, Qi and Zheng, Zibin and Lyu, Michael R},
  booktitle={2019 IEEE/ACM 41st International Conference on Software Engineering: Software Engineering in Practice (ICSE-SEIP)},
  pages={121--130},
  year={2019},
  organization={IEEE}
}
```

---

## 📧 Support

**Repository**: https://github.com/espirado/Observability-Platform---Databricks-ETL-Pipeline

**Issues**: Open a GitHub issue for questions

**Documentation**: See `docs/` folder for detailed architecture

---

**Good luck with your DS-610 final project! 🚀**
