# Databricks notebook source
# MAGIC %md
# MAGIC # 00: Ingest from LogHub (Real Production Logs)
# MAGIC
# MAGIC **Source**: https://github.com/logpai/loghub
# MAGIC
# MAGIC **LogHub**: A large collection of system log datasets for AI-driven log analytics
# MAGIC
# MAGIC **Available Datasets**:
# MAGIC - HDFS (Hadoop logs) - 11M+ lines
# MAGIC - Spark (Apache Spark logs) - 33K+ lines
# MAGIC - Zookeeper - 74K+ lines
# MAGIC - BGL (Blue Gene/L supercomputer) - 4.7M+ lines
# MAGIC - HPC (High Performance Computing) - 433K+ lines
# MAGIC - Thunderbird - 211M+ lines
# MAGIC - Windows - 114K+ lines
# MAGIC - Linux - 120K+ lines
# MAGIC - OpenStack - 207K+ lines
# MAGIC - Apache - 56K+ lines
# MAGIC
# MAGIC **Perfect for DS-610** because it's real production data used in academic research!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("dataset", "HDFS", 
                         ["HDFS", "Spark", "Zookeeper", "BGL", "HPC", 
                          "Thunderbird", "Windows", "Linux", "OpenStack", "Apache"],
                         "Dataset")
dbutils.widgets.text("sample_size", "100000", "Sample Size (0 = all)")
dbutils.widgets.text("output_path", "/dbfs/observability-data/loghub", "Output Path")

DATASET = dbutils.widgets.get("dataset")
SAMPLE_SIZE = int(dbutils.widgets.get("sample_size"))
OUTPUT_PATH = dbutils.widgets.get("output_path")

print(f"📊 Selected Dataset: {DATASET}")
print(f"📏 Sample Size: {SAMPLE_SIZE if SAMPLE_SIZE > 0 else 'All'}")
print(f"💾 Output Path: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LogHub Dataset URLs

# COMMAND ----------

# LogHub raw data URLs (from GitHub)
LOGHUB_URLS = {
    "HDFS": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/HDFS/HDFS_2k.log",
        "format": "structured",
        "description": "Hadoop Distributed File System logs"
    },
    "Spark": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/Spark/Spark_2k.log",
        "format": "structured",
        "description": "Apache Spark logs"
    },
    "Zookeeper": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/Zookeeper/Zookeeper_2k.log",
        "format": "structured",
        "description": "Apache Zookeeper logs"
    },
    "BGL": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/BGL/BGL_2k.log",
        "format": "structured",
        "description": "Blue Gene/L supercomputer logs"
    },
    "HPC": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/HPC/HPC_2k.log",
        "format": "structured",
        "description": "High Performance Computing logs"
    },
    "Thunderbird": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/Thunderbird/Thunderbird_2k.log",
        "format": "structured",
        "description": "Thunderbird supercomputer logs"
    },
    "Windows": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/Windows/Windows_2k.log",
        "format": "structured",
        "description": "Windows system event logs"
    },
    "Linux": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/Linux/Linux_2k.log",
        "format": "structured",
        "description": "Linux system logs"
    },
    "OpenStack": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/OpenStack/OpenStack_2k.log",
        "format": "structured",
        "description": "OpenStack cloud logs"
    },
    "Apache": {
        "url": "https://raw.githubusercontent.com/logpai/loghub/master/Apache/Apache_2k.log",
        "format": "structured",
        "description": "Apache web server logs"
    }
}

dataset_info = LOGHUB_URLS[DATASET]
print(f"\n📖 Dataset: {dataset_info['description']}")
print(f"🔗 URL: {dataset_info['url']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download LogHub Dataset

# COMMAND ----------

import urllib.request
import os

# Create output directory
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Download file
download_url = dataset_info['url']
raw_log_file = f"{OUTPUT_PATH}/{DATASET}_raw.log"

print(f"📥 Downloading {DATASET} dataset...")
print(f"   From: {download_url}")

try:
    urllib.request.urlretrieve(download_url, raw_log_file)
    
    # Check file size
    file_size = os.path.getsize(raw_log_file)
    print(f"✅ Downloaded {file_size:,} bytes")
    
    # Count lines
    with open(raw_log_file, 'r', encoding='utf-8', errors='ignore') as f:
        line_count = sum(1 for _ in f)
    
    print(f"✅ Total log lines: {line_count:,}")
    
except Exception as e:
    print(f"❌ Download failed: {e}")
    print(f"\n💡 Alternative: Download manually from GitHub and upload to DBFS:")
    print(f"   https://github.com/logpai/loghub/tree/master/{DATASET}")
    dbutils.notebook.exit("DOWNLOAD_FAILED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse LogHub Format
# MAGIC
# MAGIC LogHub logs are typically in this format:
# MAGIC ```
# MAGIC <Date> <Time> <Pid> <Level> <Component>: <Content>
# MAGIC ```

# COMMAND ----------

import re
import json
from datetime import datetime, timedelta
import random

def parse_loghub_line(line, dataset_type):
    """Parse a LogHub log line into structured JSON"""
    
    # Common patterns for different log types
    patterns = {
        "HDFS": r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (\w+) ([^\s:]+): (.+)$',
        "Spark": r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (\w+) ([^\s:]+): (.+)$',
        "Linux": r'^(\w{3}\s+\d{1,2} \d{2}:\d{2}:\d{2}) ([^\s]+) ([^\[]+)\[(\d+)\]: (.+)$',
        "OpenStack": r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\d+) (\w+) ([^\[]+) \[([^\]]+)\] (.+)$',
        "Apache": r'^(\S+ \S+) \[([^\]]+)\] "([^"]+)" (\d+) (\d+)$',
        "BGL": r'^- (\d{10}) (\d{4}\.\d{2}\.\d{2}) \S+ \S+ ([A-Z]+) ([^:]+): (.+)$',
        "HPC": r'^(\d+) (\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d+) \S+ ([A-Z]+) ([^:]+): (.+)$',
    }
    
    # Default pattern (generic)
    pattern = patterns.get(dataset_type, r'^(.+)$')
    
    match = re.match(pattern, line.strip())
    
    if not match:
        # If no match, return simple structure
        return {
            "timestamp": datetime.now().isoformat() + "Z",
            "level": "INFO",
            "service": dataset_type.lower(),
            "message": line.strip(),
            "component": "unknown"
        }
    
    # Extract fields based on dataset type
    if dataset_type == "HDFS":
        timestamp_str, level, component, content = match.groups()
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f").isoformat() + "Z"
    
    elif dataset_type == "Spark":
        timestamp_str, level, component, content = match.groups()
        # Parse Spark timestamp (yy/MM/dd HH:mm:ss)
        timestamp = datetime.strptime(timestamp_str, "%y/%m/%d %H:%M:%S").isoformat() + "Z"
    
    elif dataset_type == "Linux":
        timestamp_str, hostname, component, pid, content = match.groups()
        # Linux logs don't have year, use current year
        current_year = datetime.now().year
        timestamp = datetime.strptime(f"{current_year} {timestamp_str}", "%Y %b %d %H:%M:%S").isoformat() + "Z"
    
    elif dataset_type == "OpenStack":
        timestamp_str, pid, level, module, request_id, content = match.groups()
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f").isoformat() + "Z"
        component = module
    
    elif dataset_type == "BGL" or dataset_type == "HPC":
        # BGL/HPC have multiple fields
        groups = match.groups()
        timestamp = datetime.fromtimestamp(int(groups[0])).isoformat() + "Z" if dataset_type == "BGL" else groups[1]
        level = groups[2] if len(groups) > 2 else "INFO"
        component = groups[3] if len(groups) > 3 else "unknown"
        content = groups[4] if len(groups) > 4 else line
    
    else:
        # Default parsing
        timestamp = datetime.now().isoformat() + "Z"
        level = "INFO"
        component = "unknown"
        content = line.strip()
    
    # Determine if this is an error
    is_error = any(keyword in line.upper() for keyword in ["ERROR", "FAIL", "EXCEPTION", "FATAL", "CRITICAL"])
    
    # Generate synthetic trace ID (for linking related logs)
    trace_id = f"trace_{abs(hash(line[:50])) % 1000000:06d}"
    
    return {
        "timestamp": timestamp,
        "level": level if 'level' in locals() else "INFO",
        "service": dataset_type.lower(),
        "component": component if 'component' in locals() else "unknown",
        "message": content if 'content' in locals() else line.strip(),
        "is_error": is_error,
        "trace_id": trace_id,
        "source": f"LogHub-{dataset_type}"
    }

print("✅ Parser function defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to Structured JSON

# COMMAND ----------

print(f"🔄 Parsing {DATASET} logs...")

parsed_logs = []
error_count = 0

with open(raw_log_file, 'r', encoding='utf-8', errors='ignore') as f:
    for i, line in enumerate(f):
        if SAMPLE_SIZE > 0 and i >= SAMPLE_SIZE:
            break
        
        try:
            parsed_log = parse_loghub_line(line, DATASET)
            parsed_logs.append(parsed_log)
        except Exception as e:
            error_count += 1
            if error_count < 10:  # Show first 10 errors
                print(f"   ⚠️  Parse error on line {i}: {e}")

print(f"\n✅ Parsed {len(parsed_logs):,} logs")
print(f"   Errors skipped: {error_count}")

# Show sample
print("\n📄 Sample parsed log:")
print(json.dumps(parsed_logs[0], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Observability Context
# MAGIC
# MAGIC Enhance logs with microservice-like metadata for our use case

# COMMAND ----------

from random import choice, randint, uniform

# Simulated microservice mapping
SERVICE_COMPONENTS = {
    "HDFS": ["namenode", "datanode", "hdfs-client"],
    "Spark": ["driver", "executor", "spark-master", "spark-worker"],
    "Zookeeper": ["zookeeper-server", "zookeeper-client"],
    "BGL": ["compute-node", "io-node", "service-node"],
    "Linux": ["kernel", "systemd", "network-service"],
    "OpenStack": ["nova", "neutron", "cinder", "keystone", "glance"],
    "Apache": ["httpd", "mod_ssl", "mod_proxy"]
}

# HTTP endpoints mapping
ENDPOINTS = {
    "HDFS": ["/api/v1/namenode", "/api/v1/datanode", "/dfs/read", "/dfs/write"],
    "Spark": ["/api/v1/submit", "/api/v1/status", "/api/v1/kill"],
    "OpenStack": ["/v2/servers", "/v2/volumes", "/v2/networks", "/v3/auth/tokens"],
    "Apache": ["/index.html", "/api/v1/users", "/api/v1/orders", "/api/v1/health"]
}

def enhance_with_observability_context(log, dataset):
    """Add microservice-like observability fields"""
    
    # Map to source/target service
    components = SERVICE_COMPONENTS.get(dataset, ["unknown"])
    source_service = choice(components)
    target_service = choice(components) if randint(0, 1) == 1 else source_service
    
    # Add HTTP-like fields
    endpoints = ENDPOINTS.get(dataset, ["/unknown"])
    endpoint = choice(endpoints)
    
    # Add status code (errors get 500s)
    if log.get("is_error", False):
        status_code = choice([500, 502, 503, 504])
        latency_ms = uniform(500, 5000)
    else:
        status_code = choice([200, 200, 200, 201, 204])
        latency_ms = uniform(10, 500)
    
    # Enhance log
    log["source_service"] = source_service
    log["target_service"] = target_service
    log["endpoint"] = endpoint
    log["http"] = {
        "method": choice(["GET", "POST", "PUT", "DELETE"]),
        "path": endpoint,
        "status_code": status_code,
        "latency_ms": round(latency_ms, 2)
    }
    
    # Add Kubernetes-like metadata
    log["kubernetes"] = {
        "cluster": f"{dataset.lower()}-cluster",
        "namespace": "production",
        "pod": f"{source_service}-{randint(1000, 9999)}",
        "container": source_service
    }
    
    return log

print("🔄 Enhancing logs with observability context...")

enhanced_logs = [enhance_with_observability_context(log.copy(), DATASET) for log in parsed_logs]

print(f"✅ Enhanced {len(enhanced_logs):,} logs")
print("\n📄 Sample enhanced log:")
print(json.dumps(enhanced_logs[0], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to JSONL

# COMMAND ----------

output_file = f"{OUTPUT_PATH}/{DATASET}_enhanced.jsonl"

print(f"💾 Writing to {output_file}...")

with open(output_file, 'w') as f:
    for log in enhanced_logs:
        f.write(json.dumps(log) + '\n')

print(f"✅ Wrote {len(enhanced_logs):,} logs to {output_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate with Spark

# COMMAND ----------

# Convert local path to DBFS path for Spark
dbfs_path = output_file.replace("/dbfs/", "dbfs:/")

df = spark.read.json(dbfs_path)

# Validation
display(df)

# Show schema
df.printSchema()

# Sample records
display(df.limit(5))

# Statistics by 'level'
display(df.groupBy("level").count())

# Only group by 'is_error' if it exists
if "is_error" in df.columns:
    display(df.groupBy("is_error").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = {
    "dataset": DATASET,
    "source": "LogHub (https://github.com/logpai/loghub)",
    "description": dataset_info['description'],
    "raw_lines": line_count if 'line_count' in locals() else 0,
    "parsed_logs": len(parsed_logs),
    "enhanced_logs": len(enhanced_logs),
    "output_file": output_file,
    "sample_size": SAMPLE_SIZE if SAMPLE_SIZE > 0 else "All",
    "status": "SUCCESS"
}

print("\n" + "="*60)
print("LOGHUB INGESTION COMPLETE")
print("="*60)
for key, value in summary.items():
    print(f"{key:20s}: {value}")
print("="*60)

print("\n✅ Ready to run notebook 01 (Bronze ingestion)!")
print(f"\n💡 Tip: This is REAL production data from {dataset_info['description']}")
print("   Perfect for your DS-610 project proposal!")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Ingested {len(enhanced_logs):,} logs from LogHub-{DATASET}")

