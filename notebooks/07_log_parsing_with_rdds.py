# Databricks notebook source
# MAGIC %md
# MAGIC # 07: Advanced Log Parsing with RDDs (Week 7)
# MAGIC
# MAGIC **Purpose**: Use RDD (Resilient Distributed Datasets) for low-level log parsing
# MAGIC
# MAGIC **Input**: Raw LogHub logs (`/dbfs/observability-data/loghub/*_raw.log`)
# MAGIC
# MAGIC **Output**: 
# MAGIC - Parsed log patterns with RDDs
# MAGIC - Custom log templates extracted
# MAGIC - Performance comparison: RDDs vs DataFrames
# MAGIC
# MAGIC **RDD Concepts Covered**:
# MAGIC - Creating RDDs from text files
# MAGIC - Transformations: map, flatMap, filter, reduceByKey
# MAGIC - Actions: collect, count, take, reduce
# MAGIC - Pair RDDs and aggregations
# MAGIC - Custom partitioning
# MAGIC
# MAGIC **Why RDDs for Log Parsing?**:
# MAGIC - Fine-grained control over parsing logic
# MAGIC - Handle malformed/unstructured logs
# MAGIC - Extract templates from raw text

# COMMAND ----------

from pyspark import SparkContext
from datetime import datetime
import re
import json

# Get Spark context
sc = spark.sparkContext

# Configuration
LOGHUB_PATH = "/dbfs/observability-data/loghub"
OUTPUT_PATH = "/observability-data/rdd_parsed"

# Get dataset
try:
    dataset = dbutils.widgets.get("dataset")
except:
    dataset = "HDFS"
    dbutils.widgets.dropdown("dataset", "HDFS", 
                            ["HDFS", "Spark", "Zookeeper", "BGL", "Linux", "OpenStack"],
                            "Dataset")

print(f"🔧 RDD Log Parsing for: {dataset}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Raw Logs as RDD

# COMMAND ----------

# DBTITLE 1,Cell 4
# Load raw log file as RDD (line-by-line)
# Note: sc.textFile() uses HDFS-style paths (without /dbfs prefix)
log_file = f"{LOGHUB_PATH}/{dataset}_raw.log"
log_file_hdfs = log_file.replace("/dbfs", "")  # Remove /dbfs for RDD operations
log_rdd = sc.textFile(log_file_hdfs)

# Count lines
line_count = log_rdd.count()
print(f"📄 Loaded {line_count:,} log lines as RDD")

# Show sample
print("\n📋 Sample raw logs:")
for line in log_rdd.take(5):
    print(f"  {line[:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD Transformation 1: Extract Log Level

# COMMAND ----------

def extract_log_level(log_line):
    """Extract log level (INFO, WARN, ERROR, etc.) from log line"""
    # Common log level patterns
    levels = ['FATAL', 'ERROR', 'WARN', 'WARNING', 'INFO', 'DEBUG', 'TRACE']
    
    for level in levels:
        if level in log_line.upper():
            return level
    
    return 'UNKNOWN'

# Map each log line to (level, 1) pair
level_pairs = log_rdd.map(lambda line: (extract_log_level(line), 1))

# Reduce by key to count each level
level_counts = level_pairs.reduceByKey(lambda a, b: a + b)

# Collect results
level_counts_dict = dict(level_counts.collect())

print("📊 Log Level Distribution (via RDD reduceByKey):")
for level, count in sorted(level_counts_dict.items(), key=lambda x: x[1], reverse=True):
    pct = (count / line_count) * 100
    print(f"   {level:10s}: {count:7,} ({pct:5.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD Transformation 2: Extract Templates
# MAGIC
# MAGIC **Template Extraction**: Replace variable parts with `<*>`
# MAGIC - IPs → `<*>`
# MAGIC - Numbers → `<*>`
# MAGIC - Timestamps → `<*>`
# MAGIC
# MAGIC This helps identify common log patterns

# COMMAND ----------

def extract_template(log_line):
    """Convert log line to template by replacing variables"""
    template = log_line
    
    # Replace IPs
    template = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '<IP>', template)
    
    # Replace numbers (but not in words)
    template = re.sub(r'\b\d+\b', '<NUM>', template)
    
    # Replace hex addresses
    template = re.sub(r'0x[0-9a-fA-F]+', '<HEX>', template)
    
    # Replace file paths
    template = re.sub(r'/[\w/.-]+', '<PATH>', template)
    
    # Replace timestamps (various formats)
    template = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', '<TIMESTAMP>', template)
    template = re.sub(r'\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}', '<TIMESTAMP>', template)
    
    # Collapse multiple spaces
    template = re.sub(r'\s+', ' ', template)
    
    return template.strip()

# Map each log to its template
template_pairs = log_rdd.map(lambda line: (extract_template(line), 1))

# Count occurrences of each template
template_counts = template_pairs.reduceByKey(lambda a, b: a + b)

# Sort by frequency
top_templates = template_counts.takeOrdered(20, key=lambda x: -x[1])

print("🔍 Top 10 Log Templates (via RDD):")
print(f"{'Count':>7}  {'Template'}")
print("-" * 100)
for template, count in top_templates[:10]:
    print(f"{count:7,}  {template[:90]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD Transformation 3: FlatMap for Tokenization

# COMMAND ----------

def tokenize_log(log_line):
    """Tokenize log line into words"""
    # Remove special characters
    cleaned = re.sub(r'[^\w\s]', ' ', log_line.lower())
    # Split into tokens
    tokens = cleaned.split()
    # Filter short tokens and numbers
    return [t for t in tokens if len(t) > 3 and not t.isdigit()]

# FlatMap to create one token per line
tokens_rdd = log_rdd.flatMap(tokenize_log)

# Count token frequencies
token_counts = tokens_rdd.map(lambda token: (token, 1)) \
                        .reduceByKey(lambda a, b: a + b)

# Get top tokens
top_tokens = token_counts.takeOrdered(20, key=lambda x: -x[1])

print("📝 Top 20 Tokens in Logs (via RDD flatMap):")
print(f"{'Count':>8}  {'Token'}")
print("-" * 40)
for token, count in top_tokens:
    print(f"{count:8,}  {token}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD Transformation 4: Filter Errors and Map to Structured Format

# COMMAND ----------

def parse_log_to_dict(log_line):
    """Parse log line into structured dictionary"""
    try:
        level = extract_log_level(log_line)
        template = extract_template(log_line)
        
        # Check if error
        is_error = level in ['ERROR', 'FATAL', 'CRITICAL']
        
        # Extract timestamp if present
        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})', log_line)
        timestamp = timestamp_match.group(1) if timestamp_match else datetime.now().isoformat()
        
        return {
            'timestamp': timestamp,
            'level': level,
            'template': template,
            'message': log_line[:200],  # First 200 chars
            'is_error': is_error,
            'length': len(log_line)
        }
    except Exception as e:
        return None

# Filter errors only and map to structured format
error_rdd = (log_rdd
    .filter(lambda line: 'ERROR' in line.upper() or 'FAIL' in line.upper())
    .map(parse_log_to_dict)
    .filter(lambda x: x is not None))  # Remove parse failures

error_count = error_rdd.count()
print(f"🚨 Found {error_count:,} error logs via RDD filter")

# Show sample errors
print("\n📋 Sample error records:")
for error in error_rdd.take(3):
    print(json.dumps(error, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD Action: Aggregation Statistics

# COMMAND ----------

# Calculate statistics using RDD actions
total_chars = log_rdd.map(lambda line: len(line)).reduce(lambda a, b: a + b)
avg_line_length = total_chars / line_count

max_line_length = log_rdd.map(lambda line: len(line)).max()
min_line_length = log_rdd.map(lambda line: len(line)).min()

print("📏 Log Line Statistics (via RDD reduce/max/min):")
print(f"   Total lines: {line_count:,}")
print(f"   Total characters: {total_chars:,}")
print(f"   Avg line length: {avg_line_length:.1f} chars")
print(f"   Min line length: {min_line_length} chars")
print(f"   Max line length: {max_line_length} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Partitioning by Log Level

# COMMAND ----------

def partition_by_level(line):
    """Custom partitioner: assign partition based on log level"""
    level = extract_log_level(line)
    
    # Map levels to partition numbers
    level_map = {
        'FATAL': 0,
        'ERROR': 1,
        'WARN': 2,
        'WARNING': 2,
        'INFO': 3,
        'DEBUG': 4,
        'TRACE': 4,
        'UNKNOWN': 5
    }
    
    return level_map.get(level, 5)

# Create pair RDD with custom partitioning key
partitioned_rdd = log_rdd.map(lambda line: (partition_by_level(line), line))

# Repartition based on key
partitioned_rdd = partitioned_rdd.partitionBy(6)  # 6 partitions

print(f"📦 RDD Partitioned into {partitioned_rdd.getNumPartitions()} partitions")

# Count records per partition
def count_per_partition(partition_id, iterator):
    count = sum(1 for _ in iterator)
    yield (partition_id, count)

partition_counts = partitioned_rdd.mapPartitionsWithIndex(count_per_partition).collect()

print("\n📊 Records per partition:")
level_names = ['FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG', 'UNKNOWN']
for part_id, count in partition_counts:
    level = level_names[part_id] if part_id < len(level_names) else 'OTHER'
    print(f"   Partition {part_id} ({level:10s}): {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert RDD to DataFrame for Storage

# COMMAND ----------

# DBTITLE 1,Cell 18
# Convert parsed error RDD to DataFrame
if error_rdd.isEmpty():
    # Create empty DataFrame with expected schema
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType
    
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("level", StringType(), True),
        StructField("template", StringType(), True),
        StructField("message", StringType(), True),
        StructField("is_error", BooleanType(), True),
        StructField("length", IntegerType(), True)
    ])
    
    error_df = spark.createDataFrame([], schema)
    print(f"⚠️  No errors found in logs. Created empty DataFrame.")
else:
    error_df = error_rdd.toDF()
    print(f"✅ Converted RDD to DataFrame: {error_df.count():,} records")

print("\nSchema:")
error_df.printSchema()

if error_df.count() > 0:
    print("\nSample data:")
    error_df.show(5, truncate=False)
else:
    print("\n(No data to display)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Comparison: RDD vs DataFrame

# COMMAND ----------

# DBTITLE 1,Cell 20
import time

# Test 1: Count errors with RDD
start = time.time()
rdd_error_count = log_rdd.filter(lambda line: 'ERROR' in line.upper()).count()
rdd_time = time.time() - start

# Test 2: Count errors with DataFrame
log_df = spark.read.text(log_file_hdfs)
start = time.time()
df_error_count = log_df.filter(log_df.value.contains('ERROR')).count()
df_time = time.time() - start

print("⚡ Performance Comparison: RDD vs DataFrame")
print(f"   Task: Count ERROR lines")
print(f"   Total lines: {line_count:,}")
print(f"\n   RDD approach:")
print(f"      Time: {rdd_time:.3f}s")
print(f"      Result: {rdd_error_count:,} errors")
print(f"\n   DataFrame approach:")
print(f"      Time: {df_time:.3f}s")
print(f"      Result: {df_error_count:,} errors")
print(f"\n   Speedup: DataFrame is {rdd_time/df_time:.2f}x faster")

print("\n💡 Key Insight: DataFrames have Catalyst optimizer, but RDDs give more control!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write RDD Results to Delta

# COMMAND ----------

# Write error DataFrame to Delta
(error_df.write
    .format("delta")
    .mode("overwrite")
    .save(f"{OUTPUT_PATH}/errors_{dataset}"))

# Write templates as DataFrame
template_df = spark.createDataFrame(
    template_counts.collect(),
    ["template", "count"]
).orderBy("count", ascending=False)

(template_df.write
    .format("delta")
    .mode("overwrite")
    .save(f"{OUTPUT_PATH}/templates_{dataset}"))

print(f"✅ Written RDD results to {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = {
    "dataset": dataset,
    "total_lines": line_count,
    "errors_extracted": error_count,
    "unique_templates": template_counts.count(),
    "unique_tokens": token_counts.count(),
    "num_partitions": partitioned_rdd.getNumPartitions(),
    "rdd_processing_time": f"{rdd_time:.3f}s",
    "df_processing_time": f"{df_time:.3f}s",
    "output_path": OUTPUT_PATH,
    "status": "SUCCESS"
}

print("\n" + "="*60)
print("RDD LOG PARSING COMPLETE")
print("="*60)
for key, value in summary.items():
    print(f"{key:30s}: {value}")
print("="*60)

print("\n✅ Week 7 (RDD Unstructured APIs) complete!")
print("🔧 Demonstrated: map, flatMap, filter, reduceByKey, partitionBy")
print(f"📊 Extracted {template_counts.count():,} unique log templates")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Parsed {line_count:,} logs with RDDs, extracted {error_count:,} errors")