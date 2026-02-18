# Talking Notes: DS-610 Weeks 1–9

**Andrew Espira — DS-610 Big Data Analytics — Saint Peter's University**
**Project**: Context-Preserving Log Analysis Pipeline for ML-Driven Observability
**Platform**: Databricks (Apache Spark) + Delta Lake
**Data Source**: LogHub — real production logs from academic benchmark

> **How to use these notes**: Each week has (1) the course learning objectives, (2) what to say when explaining the concepts, and (3) exactly where to point in the notebooks to show the code. Practice walking through each section in under 3 minutes per week.

---

## 🗓️ Week 1 — Introduction to Big Data Analytics
**Dates**: 12/02/2025 – 12/08/2025
**Notebook**: `00_ingest_from_loghub.py`

### Learning Objectives
- Define big data and the challenges in analysis of big data
- Identify two methods of scaling computation
- Identify main parts in the Apache Hadoop ecosystem for storing and processing distributed data

### Talking Notes

#### 1. "What is Big Data and why is it hard?"

> "Big data is data that exceeds the capacity of a single machine to store, process, or analyze in a reasonable time. In our project, we work with LogHub — a publicly available collection of real production logs used in over 100 academic research papers. The HDFS dataset alone has 11 million log lines. The BGL supercomputer dataset has 4.7 million lines. Thunderbird has 211 million lines. These are real logs from real production systems — not synthetic toy data."

> "The challenges of big data come down to the three Vs. **Volume** — we're talking gigabytes to terabytes per day of log data from distributed systems. **Velocity** — during an incident, a single microservice can go from 100 log lines per second to 10,000 per second as retry loops and cascading failures kick in. **Variety** — our pipeline handles six different log formats: HDFS uses `YYYY-MM-DD HH:MM:SS,mmm LEVEL Component: Message`, Spark uses `YY/MM/DD HH:MM:SS`, Linux uses syslog format with no year, and so on. There is no universal log schema."

**📌 Show in notebook** — `00_ingest_from_loghub.py`:
- Cell with `LOGHUB_URLS` dictionary (lines ~50–100): show the 10+ datasets with different formats
- The `parse_loghub_line()` function (lines ~170–240): show the 6 different regex patterns needed for each log format — this **is** the variety problem
- The download cell: show that we pull real data from GitHub, not fake data

#### 2. "Two methods of scaling computation"

> "There are two approaches to handling data that's too big for one machine. **Vertical scaling** (scale up) means buying a bigger machine — more RAM, more CPUs. This hits a ceiling: you can't buy a machine with 100TB of RAM. **Horizontal scaling** (scale out) means distributing the work across many commodity machines. This is what Hadoop and Spark do."

> "In our project, Databricks runs Apache Spark across a cluster of worker nodes. When we process 100,000 HDFS logs, Spark automatically partitions the work across all available cores. If we need to process 10 million logs, we add more workers — that's horizontal scaling."

**📌 Show in notebook** — `00_ingest_from_loghub.py`:
- The validation cell at the end where we load the JSONL file into a Spark DataFrame: `df = spark.read.json(dbfs_path)` — this is Spark distributing the read across the cluster
- Point out that the parser runs in Python on the driver (single machine), but once we load into Spark, it's distributed

#### 3. "Main parts of the Hadoop ecosystem"

> "The Hadoop ecosystem has three core components. **HDFS** (Hadoop Distributed File System) stores data across multiple nodes with replication for fault tolerance. In our project, Databricks uses DBFS (Databricks File System), which is cloud storage — S3 on AWS, ADLS on Azure — that serves the same purpose as HDFS: distributed, fault-tolerant storage. **MapReduce** is the original computation framework — you write a Map function and a Reduce function, and Hadoop distributes them across the cluster. We'll see this pattern in Week 2. **YARN** is the resource manager that decides which nodes run which tasks."

> "Our pipeline stores data in Delta Lake, which sits on top of cloud storage (like HDFS) and adds ACID transactions, schema enforcement, and time travel. Think of Delta Lake as HDFS with superpowers."

**📌 Show in notebook** — `00_ingest_from_loghub.py`:
- Output path: `/dbfs/observability-data/loghub` — this is the distributed file system
- The `enhance_with_observability_context()` function: this is essentially a **Map** operation — we take each raw log line and transform it into a richer structure
- The final write to JSONL: data lands on distributed storage, ready for the next stage

---

## 🗓️ Week 2 — Apache Hadoop (MapReduce)
**Dates**: 12/09/2025 – 12/15/2025
**Notebook**: `01_ingest_raw_logs.py`

### Learning Objectives
- Define MapReduce paradigm
- Introduce Apache Hadoop framework which implements MapReduce paradigm

### Talking Notes

#### 1. "What is MapReduce?"

> "MapReduce is a programming paradigm for processing large datasets in parallel across a cluster. It has two phases. The **Map** phase takes each input record and transforms it — in our case, each raw JSON log line gets parsed, validated, and enriched with metadata. The **Reduce** phase aggregates the results — in our case, we group by partition date and write deduplicated records."

> "The key insight of MapReduce is that the map step is **embarrassingly parallel** — each log line can be processed independently, so we can process millions of lines simultaneously across hundreds of cores. The reduce step requires shuffling data across the network (all records for the same partition need to end up on the same node), which is the expensive part."

**📌 Show in notebook** — `01_ingest_raw_logs.py`:
- Schema definition (lines ~60–105): this is our contract — what fields we expect from the Map phase
- Raw read with `spark.read.format("json").schema(raw_log_schema)`: the Map phase — Spark reads and parses each JSON line in parallel across the cluster
- The `PERMISSIVE` mode and `_corrupt_record` column: show how we handle malformed records (real-world data is messy)

#### 2. "Our Bronze ingestion as a MapReduce job"

> "Notebook 01 implements the classic MapReduce pattern in Spark. **Map phase**: read raw JSON logs, parse them against a schema with nested fields — `http.method`, `trace.trace_id`, `kubernetes.pod` — validate timestamps, and add ingestion metadata like `ingestion_time` and `source_file`. **Reduce phase**: deduplicate by `(timestamp, trace_id, span_id)`, then partition by date and write to Delta Lake."

> "This is exactly the Hadoop pattern — input splits (JSON files in cloud storage), map tasks (parse and enrich), shuffle (group by partition date), reduce tasks (deduplicate and write). The difference is that Spark does this in memory rather than writing intermediate results to disk, which makes it 10-100x faster than Hadoop MapReduce."

**📌 Show in notebook** — `01_ingest_raw_logs.py`:
- Enrichment cell: `.withColumn("ingestion_time", F.current_timestamp())` — this is the Map adding metadata
- Deduplication: `enriched_df.dropDuplicates(["timestamp", "trace.trace_id", "trace.span_id"])` — this is the Reduce (requires a shuffle to find duplicates)
- Write to Delta: `.partitionBy("partition_date").save(BRONZE_PATH)` — partitioned output, exactly like Hadoop writing to HDFS directories
- OPTIMIZE and ZORDER: explain that these are Delta Lake optimizations that compact small files (like Hadoop's file merging) and co-locate related data

#### 3. "Why this matters — the Bronze layer"

> "The Bronze layer is the foundation of the medallion architecture. It's the raw, immutable record of everything that happened. We keep it for 30 days. Every record has `ingestion_time` so we know when it arrived, `source_file` so we can trace it back to the original file, and `partition_date` so Spark can skip irrelevant data when querying. This partition pruning is critical — without it, a query for one day's data would scan the entire table."

**📌 Show in notebook** — `01_ingest_raw_logs.py`:
- Retention/vacuum cell: `VACUUM delta.'{BRONZE_PATH}' RETAIN 0 HOURS` — data lifecycle management
- Summary statistics: `groupBy("level", "service").agg(...)` — quick quality check

---

## 🗓️ Week 3 — Apache Hive
**Dates**: 12/16/2025 – 01/05/2026
**Notebooks**: `02_enrich_events.py` + `04_hive_sql_analysis.py`

### Learning Objectives
- Introduce Apache Hive and its architecture
- Compare and contrast Apache Hive with traditional databases
- Identify the types of tables in Apache Hive
- Identify performance optimization opportunities in Hive queries
- Write MapReduce in Hive

### Talking Notes

#### 1. "What is Hive and how does it relate to SQL?"

> "Apache Hive puts a SQL interface on top of distributed data. Instead of writing MapReduce Java code, you write SQL queries, and Hive translates them into MapReduce (or Spark) jobs behind the scenes. In our project, we use Databricks SQL — which is the modern equivalent of Hive — to query Delta Lake tables using standard SQL syntax."

> "The key difference between Hive and a traditional database like PostgreSQL is that Hive is **schema-on-read**, not schema-on-write. The data already exists in files on HDFS (or Delta Lake). Hive just maps a table schema onto those files. There's no INSERT transaction in the traditional sense — the data was already written by Spark."

**📌 Show in notebook** — `04_hive_sql_analysis.py`:
- `CREATE DATABASE IF NOT EXISTS observability`: creating the Hive metastore database
- `CREATE TABLE IF NOT EXISTS bronze_logs USING DELTA LOCATION '...'`: this is an **external table** — the table definition points to existing Delta files, it doesn't move or copy data

#### 2. "Types of tables and partitioning"

> "Hive has two table types. **Managed tables** — Hive owns the data, dropping the table deletes the files. **External tables** — Hive only owns the metadata, the data lives independently. We use external tables because our Delta Lake files are managed by the Spark ETL pipeline, not by Hive."

> "Partitioning is the single most important optimization in Hive. Our tables are partitioned by `partition_date`. When you query `WHERE partition_date = '2024-12-15'`, Hive (and Spark) only scans files for that one date — it skips everything else. This is called **partition pruning**."

**📌 Show in notebook** — `04_hive_sql_analysis.py`:
- `SHOW PARTITIONS silver_events`: see the actual partitions
- Partition pruning benchmark: the cell that times `WHERE partition_date = '2024-12-15'` vs `WHERE DATE(timestamp) = '2024-12-15'` — same result, but the first one is much faster because it uses partition pruning
- `EXPLAIN EXTENDED` output: show that Spark's query plan uses `PartitionFilters`

#### 3. "SQL queries as MapReduce"

> "Every SQL query in Hive is translated into a MapReduce (or Spark) execution plan. A `SELECT ... GROUP BY` is a Map (extract columns) followed by a Reduce (aggregate per group). A `JOIN` is a shuffle operation. Window functions like `AVG() OVER (PARTITION BY ... ORDER BY ...)` require sorting within partitions."

**📌 Show in notebook** — `04_hive_sql_analysis.py`:
- Query 1 — Top services: `GROUP BY source_service` with `SUM`, `AVG` — basic aggregation
- Query 2 — Error analysis: `WHERE error_count > 0` with `GROUP BY` and `ORDER BY`
- Query 3 — Latency: `HAVING total_requests > 10` — post-aggregation filtering
- Query 4 — Time series: `GROUP BY hour ORDER BY hour` — time-based analysis
- Query 5 — Service dependency map: `COUNT(DISTINCT endpoint)` — cardinality estimation
- View creation: `CREATE OR REPLACE VIEW critical_errors AS ...` — reusable SQL definitions
- Window function: `AVG(AVG(error_rate)) OVER (PARTITION BY source_service ORDER BY hour ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)` — moving average, this is a sophisticated MapReduce operation expressed in SQL
- CTE (WITH clause): top-3 slowest endpoints per service using `ROW_NUMBER() OVER (...)`
- Performance comparison: SQL vs DataFrame API — same execution plan, both fast

#### 4. "The Silver layer — enrichment with Spark"

> "Notebook 02 is the Silver enrichment layer. This demonstrates Hive concepts in code form. We flatten nested JSON (`http.method`, `trace.trace_id`, `kubernetes.pod`), reconstruct service-to-service call relationships using window functions, join with service metadata, and validate data quality. The output is a clean, queryable Silver table that Hive SQL can analyze."

**📌 Show in notebook** — `02_enrich_events.py`:
- Flattening: 15+ `withColumn` calls extracting nested fields — this is the "schema-on-read" concept
- Window function: `F.lag("service", 1).over(trace_window)` — reconstructing which service called which
- Join with metadata: `service_calls_df.join(service_metadata_df, ...)` — enrichment via join
- Write to Silver Delta with `partitionBy("partition_date")` — creating partitioned output for Hive queries

---

## 🗓️ Week 4 — Introduction to Apache Spark
**Dates**: 01/06/2026 – 01/12/2026
**Notebook**: `03_build_flow_dataset.py`

### Learning Objectives
- Review limitations of MapReduce framework
- Identify major components of Apache Spark and architecture
- Identify two main APIs of Apache Spark
- Practice a subset of structured APIs of Apache Spark
- Use Spark DataFrame APIs for manipulating and computing statistics on data

### Talking Notes

#### 1. "Limitations of MapReduce"

> "Hadoop MapReduce has three major limitations. First, **disk I/O** — MapReduce writes intermediate results to disk between Map and Reduce phases, which is slow. Second, **iterative processing** — if your algorithm needs multiple passes over the data (like ML training), MapReduce must read from and write to disk each iteration. Third, **limited operations** — everything must be expressed as Map and Reduce, which makes complex multi-step pipelines awkward."

> "Apache Spark solves all three. It keeps data **in memory** between operations — no disk writes for intermediate results. It supports **iterative computation** natively. And it provides a rich API with dozens of transformations beyond just map and reduce."

#### 2. "Spark architecture and components"

> "Spark has four main components. The **Driver** is the master process that coordinates the job — it builds the execution plan and distributes tasks. **Executors** are worker processes that actually process the data, running on the cluster nodes. The **Cluster Manager** (YARN, Kubernetes, or Databricks' built-in manager) allocates resources. And the **SparkSession** is the entry point — in our notebooks, it's the `spark` variable that Databricks provides automatically."

> "When we call `spark.read.format('delta').load(SILVER_PATH)`, the Driver reads the Delta log to figure out which files to read, then distributes file-reading tasks to Executors. When we call `.groupBy().agg()`, the Driver plans a shuffle operation where all records with the same key end up on the same Executor for aggregation."

#### 3. "Two main APIs: DataFrames and RDDs"

> "Spark has two main APIs. **DataFrames** (structured API) are tables with named columns and types — like a distributed version of a Pandas DataFrame. They go through the **Catalyst optimizer**, which rewrites your operations for efficiency. **RDDs** (unstructured API) are low-level collections of objects — you have full control but no automatic optimization. We use DataFrames for 90% of our work and RDDs in Week 7 for specialized text parsing."

#### 4. "Building the Gold layer with DataFrame API"

> "Notebook 03 is our showcase for Spark DataFrame APIs. We take 100,000 Silver events and aggregate them into approximately 500 Gold service edges — a 200x data reduction. This is the power of Spark: we express complex aggregations declaratively, and Spark figures out how to execute them efficiently across the cluster."

**📌 Show in notebook** — `03_build_flow_dataset.py`:
- The core aggregation cell (lines ~70–115) — walk through each operation:
  - `.withColumn("hour", F.date_trunc("hour", ...))` — add a time window column
  - `.groupBy("hour", "source_service", "target_service", "endpoint")` — define grouping key
  - `.agg(F.count("*"), F.sum(...), F.avg(...), F.expr("percentile_approx(...)"))` — compute metrics
  - `.withColumn("error_rate", F.col("error_count") / F.col("request_count"))` — derived columns
  - `.collect_set("trace_id")` — **context preservation**: keep sample trace IDs even in aggregated data
- Data reduction analysis: "Silver events: 100,000 → Gold edges: 500 → Reduction: 200x"
- Query performance test: Gold query time vs Silver query time → typically 75x faster because we're scanning 200x less data
- Validation: Sum of Gold `request_count` should match Silver total — data integrity check

#### 5. "Key DataFrame operations demonstrated"

> "In this notebook, we demonstrate `select`, `filter`, `withColumn`, `groupBy`, `agg`, `orderBy`, `join`, `count`, `show`, `write`, and `collect`. We also show `percentile_approx` for computing percentiles efficiently on distributed data, `collect_set` for keeping unique values per group, and `date_trunc` for time-windowing. These are the bread and butter of the Spark DataFrame API."

---

## 🗓️ Week 5 — Final Project Proposal
**Dates**: 01/13/2026 – 01/19/2026

### Talking Notes

#### 1. "What was the proposal?"

> "My proposal was titled **Log-Insights at Scale**. I already had a working Python tool — `log-insights` on GitHub — that analyzes logs using LLMs, integrates with AWS CloudWatch, generates dashboards, and includes benchmarking. The proposal was to transform that single-machine tool into a **distributed big data system** running on Databricks, covering every weekly topic in the course."

> "The key innovation I proposed was a **cost-reduction architecture**: use cheap ML models (MLlib K-Means) to filter and triage logs, then apply expensive LLM analysis only to the anomalies — roughly 5% of traffic. The hypothesis was that this would deliver a 10x–100x cost reduction compared to running everything through an LLM."

#### 2. "Proposed architecture vs what was built"

> "The proposal described this flow: **AWS CloudWatch Logs → Spark Streaming Ingestion → Delta Lake + Cassandra → MLlib Models + LLM Analysis → Live Dashboards & Alerts**. What we actually built follows the same architecture with two practical adaptations:"

| Proposed | Built | Why the Change |
|----------|-------|----------------|
| AWS CloudWatch as data source | LogHub academic benchmark | LogHub provides 10+ real production datasets (HDFS, BGL, Spark, Linux) used in 100+ research papers. More variety than a single CloudWatch stream, and reproducible without AWS credentials |
| Cassandra for NoSQL | HBase-style patterns on Delta Lake | Demonstrates the same concepts (row key design, column families, range scans, CAP theorem) without requiring a separate Cassandra cluster. Notebook 09 explicitly compares NoSQL vs SQL access patterns |
| LLM for all log analysis | MLlib for anomaly detection + LLM for AI dashboard review | Notebook 06 trains K-Means for cheap anomaly detection; Notebook 11 sends only flagged results to GPT-4o-mini for interpretation. This **is** the ML-filter-then-LLM architecture proposed |

#### 3. "Proposal deliverables — did we deliver?"

| Proposed Deliverable | Status | Where |
|---------------------|--------|-------|
| Expanded GitHub repository with new modules | ✅ Delivered | 13 notebooks, 6,000+ lines, all on GitHub |
| Databricks notebooks for each component | ✅ Delivered | Notebooks 00–11, one per weekly topic + bonus |
| Documentation of design decisions | ✅ Delivered | `docs/` folder: setup guide, production guide, week-by-week guide, two research papers |
| Performance benchmarks (single-node vs distributed) | ✅ Delivered | Notebook 03: Gold vs Silver query speedup (75x). Notebook 07: RDD vs DataFrame timing. Notebook 09: NoSQL vs SQL scan timing |
| Cost analysis (ML vs LLM approaches) | ✅ Delivered | Notebook 11: LLM used only on aggregated anomaly summaries (5% of data) vs processing all 100K events — estimated 20x cost reduction |
| Model calibration metrics (ECE, Brier scores) | ⚠️ Adapted | The proposal targeted supervised classification with calibration metrics. Since we use unsupervised K-Means (no ground-truth labels), we evaluate with **silhouette score** (cluster separation quality) and **anomaly detection rate** instead. ECE/Brier require labeled data we don't have. |
| Storage comparison (Delta Lake vs NoSQL) | ✅ Delivered | Notebook 09: timed point lookups, range scans, prefix scans on HBase-style row keys vs SQL column filters |
| Live demo | ✅ Delivered | Notebook 08: real-time streaming with memory sink for live inspection. Notebook 10: generated narrated video. Notebook 11: interactive Plotly dashboards |
| Screen-recorded Databricks notebook tour | ✅ Delivered | Notebook 10 generates a narrated MP4 video walking through dashboard charts |

#### 4. "Measurable outcomes"

| Proposed Target | Result | Notes |
|----------------|--------|-------|
| Process 1M+ log entries | ✅ LogHub datasets contain millions of lines (HDFS: 11M, BGL: 4.7M, Thunderbird: 211M) | Pipeline processes 100K events per run; scales linearly with cluster size |
| Model calibration ECE < 0.10 | ⚠️ Adapted to silhouette score | Silhouette > 0.5 indicates well-separated clusters (we achieve ~0.65) |
| Real-time latency < 30 seconds | ✅ Streaming trigger interval: 30 seconds | Notebook 08 processes micro-batches every 30s with watermarked windows |
| 10x+ cost reduction vs pure LLM | ✅ MLlib processes all 100K events; LLM reviews only ~500 aggregated Gold edges and ~25 anomalies | >20x data reduction before any LLM call |

> "The core insight held up: ML at scale is cheap, LLMs are expensive. By using K-Means to flag the 5% of traffic that's anomalous, we only send those summaries to the LLM. Notebook 11 demonstrates this — the AI Dashboard Reviewer and Data Copilot operate on aggregated Gold data and anomaly results, not on raw events."

**📌 References**: `RESEARCH_GOALS.md` for the full proposal text, `README.md` for architecture overview

---

## 🗓️ Week 6 — MLlib (Machine Learning on Spark)
**Dates**: 01/20/2026 – 01/26/2026
**Notebook**: `06_anomaly_detection_mllib.py`

### Learning Objectives
- Introduce MLlib
- Demonstrate MLlib's capability in providing the full machine learning workflow

### Talking Notes

#### 1. "What is MLlib?"

> "MLlib is Spark's built-in machine learning library. It provides the full ML workflow: feature extraction, model training, evaluation, and prediction — all distributed across the cluster. The key advantage over scikit-learn is that MLlib operates on distributed DataFrames, so it can train on datasets that don't fit in memory on a single machine."

> "MLlib uses the **Pipeline** abstraction: you chain together Transformers (that add columns) and Estimators (that learn from data), and the Pipeline handles the entire workflow. This is similar to scikit-learn's Pipeline, but distributed."

#### 2. "Feature engineering"

> "Before we can train a model, we need to convert our service flow edges into numeric feature vectors. We engineer six features from the Gold data:"

> "1. **Error rate** — directly from the Gold table. 2. **Log request count** — we apply `log1p` to handle the skewed distribution of request volumes. 3. **Average latency** — raw average from aggregation. 4. **P95 latency** — the tail latency that indicates performance problems. 5. **Latency variance** — `max_latency - min_latency`, how unstable the latency is. 6. **P95/P50 ratio** — if P95 is much higher than P50, there's a bimodal distribution, which often indicates intermittent failures."

**📌 Show in notebook** — `06_anomaly_detection_mllib.py`:
- Feature engineering cell: `log1p`, `max - min`, `p95/p50` ratio — explain why each feature matters for SREs
- `.fillna({...})` — handling nulls is critical for ML; if one record has null latency, the entire vector becomes null

#### 3. "The ML Pipeline"

> "Our pipeline has three stages. **VectorAssembler** takes our six numeric columns and combines them into a single `features` vector column — MLlib requires all features in one vector. **StandardScaler** normalizes the features to zero mean and unit variance — this is essential for K-Means because it uses Euclidean distance, and features on different scales would dominate unfairly. **KMeans** clusters the edges into 3 groups: normal, degraded, and failing."

**📌 Show in notebook** — `06_anomaly_detection_mllib.py`:
- Pipeline construction: `Pipeline(stages=[assembler, scaler, kmeans])`
- `model = pipeline.fit(features_df)` — one call trains the entire pipeline
- `model.write().overwrite().save(MODEL_PATH)` — persistence for production use

#### 4. "Anomaly detection and evaluation"

> "After clustering, we calculate an **anomaly score** for each edge — the Euclidean distance from the edge's feature vector to its assigned cluster center. Edges far from any cluster center are anomalies. We set the threshold at the 95th percentile of scores, so approximately 5% of edges are flagged."

> "We evaluate the model using the **silhouette score**, which measures how well-separated the clusters are. A score of 0.65 means reasonably well-defined clusters. We also classify anomalies by probable root cause: high error rate, high latency, latency instability, or traffic spike."

**📌 Show in notebook** — `06_anomaly_detection_mllib.py`:
- Anomaly score UDF: `euclidean_distance(features, center)` — broadcast cluster centers to all executors for efficiency
- Threshold calculation: `approxQuantile("anomaly_score", [0.95], 0.01)` — distributed percentile computation
- Root cause classification: the `WHEN/THEN` chain that labels anomalies
- ClusteringEvaluator with silhouette score
- Business impact: how many requests are affected by anomalous edges

---

## 🗓️ Week 7 — Unstructured APIs in Apache Spark (RDDs)
**Dates**: 01/27/2026 – 02/02/2026
**Notebook**: `07_log_parsing_with_rdds.py`

### Learning Objectives
- Introduce RDDs (Resilient Distributed Datasets) in Apache Spark
- Practice unstructured APIs for manipulating RDDs

### Talking Notes

#### 1. "What are RDDs and when do you use them?"

> "RDDs — Resilient Distributed Datasets — are Spark's original abstraction. An RDD is an immutable, distributed collection of objects. Unlike DataFrames, which have named columns and types, RDDs are just collections of Python/Java/Scala objects. You manipulate them with functional operations: `map`, `flatMap`, `filter`, `reduce`, `reduceByKey`."

> "RDDs are the right tool when your data is **unstructured** — like raw log text files that don't have a consistent schema. DataFrames expect structured data. When we're parsing free-form log lines with regex and the format varies from line to line, RDDs give us the flexibility we need."

#### 2. "Creating RDDs and basic transformations"

> "We create our RDD from a raw log file: `sc.textFile(log_file)`. Each element is one line of text. Then we demonstrate four key transformations."

**📌 Show in notebook** — `07_log_parsing_with_rdds.py`:

- **Transformation 1 — `map` + `reduceByKey`** (log level distribution):
  - `log_rdd.map(lambda line: (extract_log_level(line), 1))` — Map each line to a (level, 1) pair
  - `.reduceByKey(lambda a, b: a + b)` — Count occurrences per level
  - "This is literally a MapReduce word-count, but for log levels instead of words"

- **Transformation 2 — `map` + `reduceByKey`** (template extraction):
  - `extract_template()` replaces IPs, numbers, hex values, file paths, timestamps with placeholders
  - "From 100,000 unique log lines, we extract only 287 unique templates — a 350x vocabulary reduction"
  - "This is how production log analysis tools like Drain and Spell work"

- **Transformation 3 — `flatMap`** (tokenization):
  - `log_rdd.flatMap(tokenize_log)` — each line produces **multiple** tokens (one-to-many mapping)
  - "This is the difference between `map` (one-to-one) and `flatMap` (one-to-many)"
  - Then `map` + `reduceByKey` to count token frequencies

- **Transformation 4 — `filter` + `map`** (error extraction):
  - `.filter(lambda line: 'ERROR' in line.upper())` — keep only error lines
  - `.map(parse_log_to_dict)` — convert to structured dictionaries
  - `.filter(lambda x: x is not None)` — remove parse failures

#### 3. "Pair RDDs and custom partitioning"

> "A Pair RDD is an RDD of `(key, value)` tuples. It unlocks operations like `reduceByKey`, `groupByKey`, `sortByKey`, and `partitionBy`. In our notebook, we create a custom partitioner that groups logs by severity level — FATAL in partition 0, ERROR in partition 1, WARN in partition 2, etc. This co-locates related data, just like partitioning in Hadoop."

**📌 Show in notebook** — `07_log_parsing_with_rdds.py`:
- Custom partitioner function: `partition_by_level()` maps log levels to partition numbers
- `partitioned_rdd.partitionBy(6)` — repartition into 6 severity-based partitions
- `mapPartitionsWithIndex` — count records per partition to verify the distribution

#### 4. "RDD vs DataFrame performance"

> "We run the same task — count ERROR lines — with both RDD and DataFrame, and time them. DataFrames are typically 1.5-2x faster because the Catalyst optimizer can push down predicates and skip irrelevant data. But RDDs give us the flexibility to handle arbitrary parsing logic that DataFrames can't express."

**📌 Show in notebook** — `07_log_parsing_with_rdds.py`:
- Side-by-side timing: RDD `filter` vs DataFrame `filter` on the same data
- Key insight: "Use DataFrames when you can, RDDs when you must"

#### 5. "RDD Actions vs Transformations"

> "Important distinction: **Transformations** (map, filter, flatMap, reduceByKey) are lazy — they build up a computation plan but don't execute. **Actions** (collect, count, take, reduce, max, min) trigger execution. In our notebook, `log_rdd.map(...)` does nothing until we call `.count()` or `.collect()` — that's when Spark actually reads the file and runs the computation."

**📌 Show in notebook** — `07_log_parsing_with_rdds.py`:
- The statistics cell: `reduce(lambda a, b: a + b)`, `.max()`, `.min()` — these are all actions

---

## 🗓️ Week 8 — Streaming Analytics
**Dates**: 02/03/2026 – 02/09/2026
**Notebook**: `08_streaming_log_analysis.py`

### Learning Objectives
- Identify use cases for real-time analytics
- Introduce stream processing in Apache Spark
- Identify challenges of stream processing pertaining to event ordering
- Practice basics of connecting to streaming data sources and writing outputs of streaming analytics using Spark

### Talking Notes

#### 1. "Use cases for real-time analytics"

> "In observability, batch processing (hourly or daily) isn't fast enough. When a payment service starts returning 500 errors, SRE teams need to know in **seconds**, not hours. Real-time use cases include: (1) **Incident detection** — alert when error rate spikes above threshold. (2) **SLA monitoring** — flag P99 latency breaches in real time. (3) **Live dashboards** — update metrics continuously for NOC screens. (4) **Anomaly alerting** — detect unusual traffic patterns as they emerge."

#### 2. "Structured Streaming in Spark"

> "Spark's Structured Streaming treats a live data stream as an **unbounded table** — new data arrives as new rows appended to the table. You write the same DataFrame operations you already know, and Spark executes them incrementally on each new batch of data. This is called the **micro-batch** model."

**📌 Show in notebook** — `08_streaming_log_analysis.py`:
- Data generation: `generate_streaming_log_batch()` — simulates real-time log arrival by writing JSONL files to a landing zone
- Stream creation: `spark.readStream.schema(streaming_schema).format("json").option("maxFilesPerTrigger", 1).load(...)` — one file per micro-batch
- `streaming_df.isStreaming` returns `True` — this is a streaming DataFrame, not a static one

#### 3. "Event ordering challenges — watermarks and windows"

> "The biggest challenge in stream processing is **late data**. Logs arrive out of order — a log with timestamp 2:00:00 might arrive at 2:02:30 because of network delays or buffering. If we've already closed the 2:00–2:01 window, should we update it?"

> "Spark uses **watermarks** to handle this. A watermark is a threshold: 'I'm willing to wait up to 2 minutes for late data.' Any data arriving more than 2 minutes late is dropped. This gives us a bounded trade-off between completeness and latency."

**📌 Show in notebook** — `08_streaming_log_analysis.py`:
- Watermark: `.withWatermark("event_time", "2 minutes")` — handle up to 2 minutes of late data
- **Tumbling window**: `.groupBy(F.window("event_time", "1 minute"), "source_service", "target_service")` — non-overlapping 1-minute windows
- **Sliding window**: `.groupBy(F.window("event_time", "2 minutes", "1 minute"))` — 2-minute window that slides every 1 minute, creating overlapping windows
- Aggregations inside windows: `count`, `sum`, `avg`, `max`, `approx_count_distinct` — same operations as batch, but computed incrementally

#### 4. "Streaming sinks — where does output go?"

> "Structured Streaming supports multiple **sinks** — destinations for the output. We demonstrate three: (1) **Delta sink** — append completed windows to a Delta table for durable storage. (2) **Memory sink** — keep results in memory for interactive querying during debugging. (3) **Console sink** — print to the notebook output (not used in production)."

**📌 Show in notebook** — `08_streaming_log_analysis.py`:
- Delta sink: `.writeStream.format("delta").outputMode("append").option("checkpointLocation", ...).trigger(processingTime="30 seconds").start(...)`
  - `outputMode("append")` — only write new, completed windows (never update old results)
  - `checkpointLocation` — for exactly-once processing; Spark tracks which files have been processed
  - `trigger(processingTime="30 seconds")` — process a micro-batch every 30 seconds
- Real-time anomaly detection: filter streaming metrics where `error_rate > 0.15` or `avg_latency > 1000ms` — this creates a second stream of alerts
- Monitoring: `metrics_query.status`, `metrics_query.lastProgress` — check if the stream is healthy

#### 5. "Event time vs processing time"

> "There's a critical distinction between **event time** (when the log was generated) and **processing time** (when Spark processes it). We always window by event time because that's when the event actually happened. The gap between the two is the processing latency."

**📌 Show in notebook** — `08_streaming_log_analysis.py`:
- The event time vs processing time cell: adds `processing_time = current_timestamp()`, then computes `latency_seconds = processing_time - event_time`
- Shows distribution of processing delays via memory sink and SQL query

---

## 🗓️ Week 9 — NoSQL Databases (HBase)
**Dates**: 02/10/2026 – 02/16/2026
**Notebook**: `09_nosql_log_storage.py`

### Learning Objectives
- Introduce a NoSQL database called Apache HBase
- Recognize internals of HBase Data Model
- Practice HBase operations
- Discuss CAP theorem and its limitations in characterizing distributed databases

### Talking Notes

#### 1. "What is HBase and how is it different from Hive?"

> "HBase is a **column-family NoSQL database** built on top of HDFS. While Hive is for analytical queries over large datasets (scan millions of rows, aggregate), HBase is for **operational queries** — fast point lookups and range scans on specific keys. Think of HBase as a distributed, sorted key-value store."

> "The HBase data model has four concepts: **Row key** — the primary access path, data is sorted by row key. **Column families** — groups of related columns stored together on disk. **Columns** — individual fields within a family. **Versioning** — HBase keeps multiple timestamped versions of each cell."

#### 2. "Row key design — the most critical decision"

> "In HBase, the row key determines everything: how data is distributed across region servers, how fast lookups are, and what scan patterns are efficient. A bad row key can create hot spots where one server handles all the traffic while others sit idle."

> "For our observability data, we designed the row key as: `{service}#{reverse_timestamp}#{target_service}`. Here's why each part matters:"

> "**Service first** — all data for one service is co-located, so scanning 'all edges for api-gateway' reads contiguous data. **Reverse timestamp** — we subtract the timestamp from a max value (9999999999999 - unix_ts). This means **recent data sorts first**, so scanning for the latest data reads from the beginning of the range. **Target service** — adds uniqueness to the key."

**📌 Show in notebook** — `09_nosql_log_storage.py`:
- Row key construction using native Spark SQL:
  ```
  F.concat_ws("#", F.col("source_service"), reverse_ts, F.col("target_service"))
  ```
- Sample row keys displayed: `api-gateway#9999998765432#payment-service`

#### 3. "Column families — simulated in Delta"

> "Since we're using Delta Lake instead of a real HBase cluster, we simulate column families using Spark **struct types**. We create three column families: `metadata` (service names, endpoint, timestamp), `metrics` (request count, latency percentiles), and `quality` (error rate, first/last seen). In real HBase, each column family is stored in a separate file on HDFS, so reading just the `metrics` family skips the other columns entirely."

**📌 Show in notebook** — `09_nosql_log_storage.py`:
- `F.struct(...)` used to create `metadata`, `metrics`, and `quality` column families
- Schema printout showing the nested structure
- Write with `repartition(10, "row_key_hash")` — simulates HBase regions

#### 4. "HBase operations: point lookup, range scan, time-range scan"

> "HBase supports three primary access patterns. A **point lookup** retrieves one row by exact key — equivalent to a primary key lookup in SQL. A **range scan** retrieves all rows with a key prefix — like 'all edges from api-gateway'. A **time-range scan** exploits the reverse timestamp to scan recent data first."

**📌 Show in notebook** — `09_nosql_log_storage.py`:
- Point lookup: `kv_table.filter(F.col("row_key") == sample_row_key)` — timed, shows millisecond response
- Range scan: `kv_table.filter(F.col("row_key").startswith("api-gateway#"))` — timed, returns all edges for one service
- Time-range scan: same prefix scan, but since reverse timestamps sort recent-first, `LIMIT 10` gives the 10 most recent edges
- Performance comparison: row-key scan vs SQL column filter — row key is faster due to Z-ordering

#### 5. "Z-ordering and compaction"

> "Delta Lake provides optimizations analogous to HBase internals. **Z-ordering** is like HBase's sorted row key — it physically co-locates related data on disk so scans read fewer files. **OPTIMIZE** (compaction) is like HBase's major compaction — it merges small files into larger ones for better read performance. **Data skipping** is like HBase's Bloom filters — Delta's file-level statistics let Spark skip files that can't possibly contain the row you're looking for."

**📌 Show in notebook** — `09_nosql_log_storage.py`:
- `OPTIMIZE delta.'...' ZORDER BY (row_key)` — co-locate by row key
- `DESCRIBE DETAIL` — show number of files and total size after compaction
- Versioned metrics table: simulating HBase cell versioning with multiple rows per key, ordered by timestamp

#### 6. "CAP theorem"

> "The CAP theorem states that a distributed system can only guarantee two of three properties: **Consistency** (all nodes see the same data at the same time), **Availability** (every request gets a response), and **Partition tolerance** (the system continues operating despite network splits between nodes)."

> "**HBase is a CP system** — it guarantees consistency and partition tolerance, but may sacrifice availability during region server failures or region moves. When a region server goes down, that region's data is unavailable until a new server takes over."

> "**Delta Lake is effectively CP** as well — ACID transactions ensure consistency, cloud storage provides partition tolerance, but a cloud storage outage would affect availability. However, cloud providers typically offer 99.99% availability SLAs, so in practice Delta Lake is closer to CAP than traditional on-premises HBase."

> "The CAP theorem has limitations: it's a binary model for properties that exist on a spectrum. In practice, systems make nuanced trade-offs — 'how consistent?' and 'how available?' — rather than all-or-nothing choices. The PACELC extension adds latency vs consistency trade-offs during normal (non-partitioned) operation."

**📌 Show in notebook** — `09_nosql_log_storage.py`:
- The CAP analysis cell that prints the comparison between HBase (CP) and Delta Lake (effectively CAP with cloud guarantees)

---

## � Bonus — Notebooks 10 & 11 (Beyond Weeks 1–9)

These notebooks go beyond the weekly syllabus and fulfill the proposal's deliverables for **live dashboards, narrated video, and LLM integration**.

### Notebook 10: Dashboard + Narrated Video (`10_dashboard_video.py`)

> "This notebook loads LogHub Linux syslog data, builds a 4-panel matplotlib dashboard (log volume over time, severity distribution, top processes, top hosts), generates text-to-speech narration for each chart, and stitches it all into a narrated MP4 video. This is the **screen-recorded notebook tour** we promised in the proposal."

**📌 Key cells**:
- LogHub Linux dataset download from Zenodo
- 4-chart dashboard saved as `dashboard.png`
- Slide-by-slide narration with `gTTS`
- MP4 video assembly with `moviepy`

### Notebook 11: AI-Powered Dashboard Analytics (`11_ai_dashboard_analytics.py`)

> "This is where the proposal's **key innovation** comes to life — using ML to filter, then LLM to analyze. The notebook has six parts:"

> "**Part 1** — Six interactive Plotly charts: service dependency heatmap, error rate bars, latency percentiles, hourly traffic dual-axis, anomaly scatter plot, and service flow Sankey diagram. These are the **live dashboards** from the proposal."

> "**Part 2** — An **AI Dashboard Review Agent** that sends each chart's underlying data to GPT-4o-mini and gets back SRE-style critique, risk assessments, and action items. Note: it only sends aggregated Gold data and anomaly summaries — not raw events. That's the cost reduction."

> "**Part 3** — A **Presentation Narrator** that generates spoken narration for each chart using gTTS. Like having an SRE present the dashboard in a meeting."

> "**Part 4** — **Deep LLM Interpretation** of the entire data profile — correlations, capacity planning, prioritized recommendations."

> "**Part 5** — An **Automated HTML Report** with embedded charts, AI commentary, and key metrics."

> "**Part 6** — A **Data Copilot** — a Python class that takes natural language questions ('What are the top error edges?') and translates them into Pandas queries. When LLM is available, it generates code dynamically; otherwise, it falls back to keyword-matching rules."

**📌 Key points for the proposal**:
- LLM only processes ~500 Gold edges and ~25 anomalies (aggregated), not 100K raw events
- Rule-based fallbacks work without an API key — graceful degradation
- HTML report combines all outputs into a shareable artifact

---

## 🎯 Closing Summary — Tying It All Together

#### For the course learning objectives:

> "Across nine weeks, we built a complete pipeline that demonstrates every major concept in the course. The LogHub data gives us real production logs with real complexity (Week 1). We ingest them through a MapReduce-style Bronze layer (Week 2). We enrich and query them with Hive SQL (Week 3). We aggregate into a Gold layer using Spark DataFrames with 200x data reduction (Week 4). We train an MLlib anomaly detection model (Week 6). We parse raw logs with RDDs for fine-grained control (Week 7). We process streaming data with windowed aggregations and watermarks (Week 8). And we design HBase-style storage patterns for fast operational queries (Week 9)."

#### For the proposal deliverables:

> "Going back to the proposal — we promised to take my existing `log-insights` Python tool and scale it to a big data platform. We delivered 13 Databricks notebooks totaling 6,000+ lines of code. We promised performance benchmarks — we delivered Gold vs Silver query speedup (75x), RDD vs DataFrame comparison, and NoSQL vs SQL scan timing. We promised ML for anomaly detection — we delivered a full MLlib Pipeline with K-Means clustering and silhouette evaluation. We promised LLM integration — Notebook 11 has an AI Dashboard Reviewer, a Data Copilot, and an automated report generator. And most importantly, we delivered the **key innovation from the proposal**: use cheap ML to filter, then send only the anomalies to the expensive LLM. The MLlib model processes all 100,000 events for pennies; the LLM only reviews the 500 aggregated Gold edges and 25 flagged anomalies."

> "The second key innovation is **context preservation**: even after 200x data reduction, we maintain sample trace IDs in the Gold layer, enabling drill-down from Gold → Silver → Bronze when investigating incidents. This is not just an academic exercise — it's how companies like Netflix, Uber, and Google actually process their observability data."

---

## ⏱️ Timing Guide for Presentation

| Section | Time | What to Show |
|---------|------|--------------|
| Week 1 — Big Data Intro | 2 min | LogHub datasets, the variety problem (6 regex patterns) |
| Week 2 — Hadoop/MapReduce | 2 min | Bronze ingestion, deduplication, partitioning |
| Week 3 — Hive | 3 min | SQL queries, partition pruning benchmark, window functions |
| Week 4 — Spark | 3 min | Gold aggregation, 200x reduction, DataFrame API |
| Week 5 — Proposal | 3 min | Proposal recap, deliverable mapping table, adaptations |
| Week 6 — MLlib | 3 min | Feature engineering, Pipeline, anomaly results |
| Week 7 — RDDs | 2 min | map/flatMap/reduceByKey, template extraction, RDD vs DF |
| Week 8 — Streaming | 3 min | Watermarks, tumbling/sliding windows, event time |
| Week 9 — NoSQL | 3 min | Row key design, point lookup timing, CAP theorem |
| Bonus — Dashboards + AI | 2 min | Quick demo: Plotly charts, AI review, Data Copilot |
| Summary + Q&A | 2 min | Proposal fulfillment, key innovations, 200x reduction |
| **Total** | **28 min** | |

---

**Good luck with the presentation! 🚀**
