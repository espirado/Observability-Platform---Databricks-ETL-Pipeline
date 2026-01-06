# Databricks notebook source
# MAGIC %md
# MAGIC # 00: Generate Sample Observability Data
# MAGIC 
# MAGIC **Purpose**: Generate synthetic log data for testing the pipeline without production data
# MAGIC 
# MAGIC **Output**: Sample JSONL logs simulating microservice interactions
# MAGIC 
# MAGIC **Use Case**: Week 1-4 learning and testing before collecting real production logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import json
import uuid

# Configuration
NUM_EVENTS = 100000  # Generate 100K events (adjust as needed)
OUTPUT_PATH = "/dbfs/observability-data/sample-logs/sample.jsonl"

# Microservices in our simulated system
SERVICES = [
    "api-gateway",
    "payment-service", 
    "user-service",
    "order-service",
    "inventory-service",
    "notification-service",
    "risk-service"
]

# Endpoints per service
ENDPOINTS = {
    "api-gateway": ["/api/v1/health", "/api/v1/checkout", "/api/v1/orders", "/api/v1/users"],
    "payment-service": ["/internal/payments/authorize", "/internal/payments/capture", "/internal/payments/refund"],
    "user-service": ["/internal/users/profile", "/internal/users/authenticate"],
    "order-service": ["/internal/orders/create", "/internal/orders/status", "/internal/orders/cancel"],
    "inventory-service": ["/internal/inventory/check", "/internal/inventory/reserve"],
    "notification-service": ["/internal/notifications/email", "/internal/notifications/sms"],
    "risk-service": ["/internal/risk/score", "/internal/risk/verify"]
}

# HTTP methods
METHODS = ["GET", "POST", "PUT", "DELETE"]

# Log levels
LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]

print(f"Generating {NUM_EVENTS:,} sample log events...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Synthetic Traces
# MAGIC 
# MAGIC Create realistic service-to-service call patterns with trace IDs

# COMMAND ----------

def generate_trace_id():
    """Generate a unique trace ID"""
    return str(uuid.uuid4()).replace('-', '')

def generate_span_id():
    """Generate a unique span ID"""
    return str(uuid.uuid4())[:16]

def generate_service_call_chain():
    """Generate a realistic service call chain"""
    # Start with api-gateway
    chain = ["api-gateway"]
    
    # Randomly add downstream services
    if random.random() < 0.8:  # 80% chance of calling payment
        chain.append("payment-service")
    
    if random.random() < 0.6:  # 60% chance of calling user-service
        chain.append("user-service")
    
    if random.random() < 0.5:  # 50% chance of calling order-service
        chain.append("order-service")
        
        if random.random() < 0.7:  # If order, likely check inventory
            chain.append("inventory-service")
    
    return chain

def generate_latency(is_error=False):
    """Generate realistic latency (slower if error)"""
    if is_error:
        return random.uniform(500, 5000)  # 500ms to 5s for errors
    else:
        return random.uniform(10, 500)  # 10ms to 500ms for success

def generate_status_code(service, is_critical_path=False):
    """Generate HTTP status code (more errors on critical path)"""
    if is_critical_path and random.random() < 0.1:  # 10% error rate on critical path
        return random.choice([500, 502, 503, 504])
    elif random.random() < 0.02:  # 2% general error rate
        return random.choice([400, 404, 500, 502, 503])
    else:
        return random.choice([200, 200, 200, 200, 201, 204])  # Mostly 200s

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Log Events

# COMMAND ----------

def generate_log_event(trace_id, service, endpoint, span_id, parent_span_id, timestamp, is_critical=False):
    """Generate a single log event"""
    
    status_code = generate_status_code(service, is_critical)
    is_error = status_code >= 500
    latency = generate_latency(is_error)
    
    # Determine log level
    if is_error:
        level = "ERROR"
        message = f"Request failed with {status_code}"
    elif status_code >= 400:
        level = "WARN"
        message = f"Client error: {status_code}"
    else:
        level = random.choice(["INFO", "INFO", "INFO", "DEBUG"])
        message = "Request completed successfully"
    
    event = {
        "timestamp": timestamp.isoformat() + "Z",
        "level": level,
        "service": service,
        "message": message,
        "http": {
            "method": random.choice(METHODS),
            "path": endpoint,
            "route": endpoint,
            "status_code": status_code,
            "latency_ms": round(latency, 2),
            "client_ip": f"10.0.{random.randint(1,255)}.{random.randint(1,255)}",
            "user_agent": "internal-service-mesh/1.0"
        },
        "trace": {
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id
        },
        "kubernetes": {
            "cluster": "production-cluster",
            "namespace": "production",
            "pod": f"{service}-{random.choice(['abc', 'def', 'ghi'])}-{random.randint(1000, 9999)}",
            "container": service
        },
        "business": {
            "user_id": f"user_{random.randint(1000, 9999)}",
            "order_id": f"order_{random.randint(10000, 99999)}" if service in ["order-service", "payment-service"] else None,
            "payment_id": f"pay_{random.randint(100000, 999999)}" if service == "payment-service" else None
        }
    }
    
    return event

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Dataset

# COMMAND ----------

import io

# Generate events
events = []
start_time = datetime.now() - timedelta(days=1)  # Start 24 hours ago

print("Generating traces and spans...")

num_traces = NUM_EVENTS // 4  # ~4 spans per trace on average

for i in range(num_traces):
    if i % 1000 == 0:
        print(f"  Generated {i:,} traces ({len(events):,} events)...")
    
    # Generate a service call chain
    trace_id = generate_trace_id()
    chain = generate_service_call_chain()
    
    # Is this a critical path trace? (more likely to have errors)
    is_critical = len(chain) >= 4 and random.random() < 0.3
    
    # Generate events for each service in the chain
    parent_span_id = None
    base_timestamp = start_time + timedelta(seconds=random.randint(0, 86400))
    
    for idx, service in enumerate(chain):
        span_id = generate_span_id()
        endpoint = random.choice(ENDPOINTS[service])
        
        # Add slight time offset for each span
        timestamp = base_timestamp + timedelta(milliseconds=idx * 50)
        
        event = generate_log_event(
            trace_id=trace_id,
            service=service,
            endpoint=endpoint,
            span_id=span_id,
            parent_span_id=parent_span_id,
            timestamp=timestamp,
            is_critical=is_critical
        )
        
        events.append(event)
        parent_span_id = span_id

print(f"\n✅ Generated {len(events):,} total events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to File

# COMMAND ----------

# Write events to JSONL
print(f"Writing events to {OUTPUT_PATH}...")

with open(OUTPUT_PATH, 'w') as f:
    for event in events:
        f.write(json.dumps(event) + '\n')

print(f"✅ Wrote {len(events):,} events to {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Generated Data

# COMMAND ----------

# Read back and validate
print("\n📊 Dataset Statistics:")

# Load as Spark DataFrame to analyze
sample_df = spark.read.json(OUTPUT_PATH)

print(f"Total events: {sample_df.count():,}")
print(f"\nEvents by service:")
sample_df.groupBy("service").count().orderBy("count", ascending=False).show()

print(f"\nEvents by log level:")
sample_df.groupBy("level").count().orderBy("count", ascending=False).show()

print(f"\nHTTP status code distribution:")
sample_df.groupBy("http.status_code").count().orderBy("http.status_code").show()

print(f"\nSample events:")
sample_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = {
    "total_events": len(events),
    "total_traces": num_traces,
    "avg_spans_per_trace": len(events) / num_traces,
    "services": len(SERVICES),
    "time_range": f"{start_time.isoformat()} to {(start_time + timedelta(days=1)).isoformat()}",
    "output_file": OUTPUT_PATH,
    "file_size_mb": round(len(json.dumps(events)) / (1024 * 1024), 2)
}

print("\n" + "="*60)
print("SAMPLE DATA GENERATION COMPLETE")
print("="*60)
for key, value in summary.items():
    print(f"{key:25s}: {value}")
print("="*60)

print("\n✅ You can now run notebook 01 to ingest this data into Bronze layer!")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Generated {len(events):,} events at {OUTPUT_PATH}")

