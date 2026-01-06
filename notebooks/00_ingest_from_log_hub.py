# Databricks notebook source
# MAGIC %md
# MAGIC # 00: Ingest from Log Hub / Real Log Sources
# MAGIC 
# MAGIC **Purpose**: Connect to real production log aggregation platforms
# MAGIC 
# MAGIC **Supported Sources**:
# MAGIC - Azure Event Hub / Azure Log Analytics
# MAGIC - AWS CloudWatch Logs
# MAGIC - Elasticsearch
# MAGIC - HTTP/REST API endpoints
# MAGIC - Kafka
# MAGIC - File-based exports (JSONL)
# MAGIC 
# MAGIC **Output**: Raw JSONL files ready for Bronze ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widget parameters for flexibility
dbutils.widgets.text("source_type", "azure_event_hub", "Log Source Type")
dbutils.widgets.text("start_time", "2024-12-14T00:00:00Z", "Start Time (ISO-8601)")
dbutils.widgets.text("end_time", "2024-12-15T00:00:00Z", "End Time (ISO-8601)")
dbutils.widgets.text("output_path", "/dbfs/observability-data/raw-logs", "Output Path")

SOURCE_TYPE = dbutils.widgets.get("source_type")
START_TIME = dbutils.widgets.get("start_time")
END_TIME = dbutils.widgets.get("end_time")
OUTPUT_PATH = dbutils.widgets.get("output_path")

print(f"Ingesting from: {SOURCE_TYPE}")
print(f"Time range: {START_TIME} to {END_TIME}")
print(f"Output: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Azure Event Hub / Azure Log Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Azure Credentials

# COMMAND ----------

# Get credentials from Databricks secrets
# Setup: databricks secrets create-scope --scope azure-logs
# databricks secrets put --scope azure-logs --key eventhub-connection-string

try:
    AZURE_EVENTHUB_CONNECTION = dbutils.secrets.get(scope="azure-logs", key="eventhub-connection-string")
    AZURE_EVENTHUB_NAME = dbutils.secrets.get(scope="azure-logs", key="eventhub-name")
    print("✅ Azure Event Hub credentials loaded")
except Exception as e:
    print(f"⚠️  Azure credentials not found: {e}")
    print("   Run this to set up secrets:")
    print("   databricks secrets create-scope --scope azure-logs")
    print("   databricks secrets put --scope azure-logs --key eventhub-connection-string")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest from Azure Event Hub (Streaming)

# COMMAND ----------

if SOURCE_TYPE == "azure_event_hub":
    from pyspark.sql import functions as F
    
    # Event Hub configuration
    ehConf = {
        'eventhubs.connectionString': AZURE_EVENTHUB_CONNECTION,
        'eventhubs.eventHubName': AZURE_EVENTHUB_NAME,
        'eventhubs.startingPosition': {
            "offset": "-1",  # Start from beginning
            "seqNo": -1,
            "enqueuedTime": START_TIME,
            "isInclusive": True
        }
    }
    
    # Read from Event Hub
    print("📥 Reading from Azure Event Hub...")
    event_hub_stream = (spark.readStream
        .format("eventhubs")
        .options(**ehConf)
        .load())
    
    # Parse event body (JSON)
    parsed_events = (event_hub_stream
        .select(
            F.col("body").cast("string").alias("json_data"),
            F.col("enqueuedTime").alias("ingestion_time"),
            F.col("sequenceNumber")
        )
        .select(F.from_json("json_data", "MAP<STRING,STRING>").alias("data"), "*")
        .select("data.*", "ingestion_time", "sequenceNumber"))
    
    # Write to output (batch mode for now)
    output_file = f"{OUTPUT_PATH}/azure_eventhub_{START_TIME[:10]}.jsonl"
    
    # Stream to file
    query = (parsed_events.writeStream
        .format("json")
        .option("path", output_file)
        .option("checkpointLocation", f"{OUTPUT_PATH}/_checkpoint")
        .start())
    
    # Run for specified duration
    import time
    time.sleep(60)  # Collect for 60 seconds
    query.stop()
    
    print(f"✅ Ingested data to {output_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative: Azure Log Analytics (REST API)

# COMMAND ----------

if SOURCE_TYPE == "azure_log_analytics":
    import requests
    import json
    from datetime import datetime
    
    # Get credentials
    WORKSPACE_ID = dbutils.secrets.get(scope="azure-logs", key="workspace-id")
    API_KEY = dbutils.secrets.get(scope="azure-logs", key="api-key")
    
    # Query configuration
    QUERY = """
    ContainerLog
    | where TimeGenerated between(datetime({start_time}) .. datetime({end_time}))
    | where Computer contains "kubernetes"
    | project TimeGenerated, Computer, ContainerID, Image, Name, LogEntry
    | order by TimeGenerated desc
    """.format(start_time=START_TIME, end_time=END_TIME)
    
    # API endpoint
    url = f"https://api.loganalytics.io/v1/workspaces/{WORKSPACE_ID}/query"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    
    body = {
        "query": QUERY
    }
    
    print("📥 Querying Azure Log Analytics...")
    response = requests.post(url, headers=headers, json=body)
    
    if response.status_code == 200:
        data = response.json()
        rows = data.get("tables", [{}])[0].get("rows", [])
        columns = data.get("tables", [{}])[0].get("columns", [])
        
        print(f"✅ Retrieved {len(rows)} log entries")
        
        # Convert to JSONL
        output_file = f"{OUTPUT_PATH}/azure_log_analytics_{START_TIME[:10]}.jsonl"
        with open(output_file, 'w') as f:
            for row in rows:
                log_entry = dict(zip([col["name"] for col in columns], row))
                f.write(json.dumps(log_entry) + '\n')
        
        print(f"✅ Wrote {len(rows)} logs to {output_file}")
    else:
        print(f"❌ Error: {response.status_code} - {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: AWS CloudWatch Logs

# COMMAND ----------

if SOURCE_TYPE == "aws_cloudwatch":
    import boto3
    import json
    from datetime import datetime
    
    # Get AWS credentials
    AWS_ACCESS_KEY = dbutils.secrets.get(scope="aws-logs", key="access-key")
    AWS_SECRET_KEY = dbutils.secrets.get(scope="aws-logs", key="secret-key")
    AWS_REGION = dbutils.secrets.get(scope="aws-logs", key="region")
    LOG_GROUP_NAME = dbutils.secrets.get(scope="aws-logs", key="log-group-name")
    
    # Initialize CloudWatch client
    client = boto3.client(
        'logs',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    
    # Convert time strings to milliseconds
    start_time_ms = int(datetime.fromisoformat(START_TIME.replace('Z', '')).timestamp() * 1000)
    end_time_ms = int(datetime.fromisoformat(END_TIME.replace('Z', '')).timestamp() * 1000)
    
    print(f"📥 Querying CloudWatch Logs: {LOG_GROUP_NAME}")
    print(f"   Time range: {START_TIME} to {END_TIME}")
    
    # Query logs
    events = []
    next_token = None
    
    while True:
        if next_token:
            response = client.filter_log_events(
                logGroupName=LOG_GROUP_NAME,
                startTime=start_time_ms,
                endTime=end_time_ms,
                nextToken=next_token
            )
        else:
            response = client.filter_log_events(
                logGroupName=LOG_GROUP_NAME,
                startTime=start_time_ms,
                endTime=end_time_ms
            )
        
        events.extend(response.get('events', []))
        
        next_token = response.get('nextToken')
        if not next_token:
            break
        
        print(f"   Retrieved {len(events)} events so far...")
    
    print(f"✅ Retrieved {len(events)} total log events")
    
    # Write to JSONL
    output_file = f"{OUTPUT_PATH}/aws_cloudwatch_{START_TIME[:10]}.jsonl"
    with open(output_file, 'w') as f:
        for event in events:
            # Parse JSON message if possible
            message = event.get('message', '')
            try:
                log_entry = json.loads(message)
            except:
                log_entry = {"message": message}
            
            # Add metadata
            log_entry['timestamp'] = datetime.fromtimestamp(event['timestamp'] / 1000).isoformat() + 'Z'
            log_entry['log_stream'] = event.get('logStreamName')
            
            f.write(json.dumps(log_entry) + '\n')
    
    print(f"✅ Wrote {len(events)} logs to {output_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 3: Elasticsearch

# COMMAND ----------

if SOURCE_TYPE == "elasticsearch":
    from elasticsearch import Elasticsearch
    import json
    from datetime import datetime
    
    # Get Elasticsearch credentials
    ES_HOST = dbutils.secrets.get(scope="elasticsearch", key="host")
    ES_USERNAME = dbutils.secrets.get(scope="elasticsearch", key="username")
    ES_PASSWORD = dbutils.secrets.get(scope="elasticsearch", key="password")
    ES_INDEX = dbutils.secrets.get(scope="elasticsearch", key="index-pattern")
    
    # Initialize Elasticsearch client
    es = Elasticsearch(
        [ES_HOST],
        http_auth=(ES_USERNAME, ES_PASSWORD),
        verify_certs=True
    )
    
    print(f"📥 Querying Elasticsearch index: {ES_INDEX}")
    
    # Build query
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": START_TIME,
                                "lte": END_TIME
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            {"@timestamp": "asc"}
        ],
        "size": 10000  # Max per scroll
    }
    
    # Use scroll API for large result sets
    results = []
    scroll_id = None
    
    # Initial search
    response = es.search(index=ES_INDEX, body=query, scroll='5m')
    scroll_id = response['_scroll_id']
    results.extend([hit['_source'] for hit in response['hits']['hits']])
    
    print(f"   Retrieved {len(results)} documents...")
    
    # Continue scrolling
    while len(response['hits']['hits']) > 0:
        response = es.scroll(scroll_id=scroll_id, scroll='5m')
        scroll_id = response['_scroll_id']
        hits = [hit['_source'] for hit in response['hits']['hits']]
        results.extend(hits)
        
        if len(hits) == 0:
            break
        
        print(f"   Retrieved {len(results)} documents so far...")
    
    # Clear scroll
    es.clear_scroll(scroll_id=scroll_id)
    
    print(f"✅ Retrieved {len(results)} total documents")
    
    # Write to JSONL
    output_file = f"{OUTPUT_PATH}/elasticsearch_{START_TIME[:10]}.jsonl"
    with open(output_file, 'w') as f:
        for doc in results:
            # Normalize timestamp field
            if '@timestamp' in doc:
                doc['timestamp'] = doc['@timestamp']
            
            f.write(json.dumps(doc) + '\n')
    
    print(f"✅ Wrote {len(results)} logs to {output_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 4: HTTP/REST API (Generic)

# COMMAND ----------

if SOURCE_TYPE == "http_api":
    import requests
    import json
    
    # Get API configuration
    API_URL = dbutils.secrets.get(scope="log-api", key="url")
    API_TOKEN = dbutils.secrets.get(scope="log-api", key="token")
    
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Build query parameters (adjust for your API)
    params = {
        "start_time": START_TIME,
        "end_time": END_TIME,
        "limit": 10000
    }
    
    print(f"📥 Querying API: {API_URL}")
    
    response = requests.get(API_URL, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Extract logs (adjust path for your API response structure)
        logs = data.get('logs', [])  # or data.get('data', []), etc.
        
        print(f"✅ Retrieved {len(logs)} log entries")
        
        # Write to JSONL
        output_file = f"{OUTPUT_PATH}/http_api_{START_TIME[:10]}.jsonl"
        with open(output_file, 'w') as f:
            for log in logs:
                f.write(json.dumps(log) + '\n')
        
        print(f"✅ Wrote {len(logs)} logs to {output_file}")
    else:
        print(f"❌ Error: {response.status_code} - {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 5: Kafka (Streaming)

# COMMAND ----------

if SOURCE_TYPE == "kafka":
    from pyspark.sql import functions as F
    
    # Get Kafka configuration
    KAFKA_BROKERS = dbutils.secrets.get(scope="kafka", key="brokers")
    KAFKA_TOPIC = dbutils.secrets.get(scope="kafka", key="topic")
    
    print(f"📥 Reading from Kafka topic: {KAFKA_TOPIC}")
    
    # Read from Kafka
    kafka_df = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load())
    
    # Parse Kafka messages
    parsed_df = (kafka_df
        .select(
            F.col("key").cast("string"),
            F.col("value").cast("string").alias("json_data"),
            F.col("timestamp")
        )
        .select(
            F.from_json("json_data", "MAP<STRING,STRING>").alias("data"),
            "*"
        )
        .select("data.*", "timestamp"))
    
    # Write to output
    output_file = f"{OUTPUT_PATH}/kafka_{START_TIME[:10]}.jsonl"
    
    (parsed_df.write
        .format("json")
        .mode("overwrite")
        .save(output_file))
    
    print(f"✅ Ingested Kafka data to {output_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 6: File Export (Pre-downloaded JSONL)

# COMMAND ----------

if SOURCE_TYPE == "file_export":
    import json
    
    # If you already exported logs from your log hub as JSONL
    INPUT_FILE = dbutils.widgets.get("input_file")
    
    print(f"📥 Reading from file: {INPUT_FILE}")
    
    # Validate and count
    line_count = 0
    with open(INPUT_FILE, 'r') as f:
        for line in f:
            try:
                json.loads(line)
                line_count += 1
            except:
                print(f"⚠️  Skipping invalid JSON line: {line[:100]}")
    
    print(f"✅ Validated {line_count:,} log entries")
    
    # Copy to output location (already in correct format)
    output_file = f"{OUTPUT_PATH}/file_export_{START_TIME[:10]}.jsonl"
    
    import shutil
    shutil.copy(INPUT_FILE, output_file)
    
    print(f"✅ Copied to {output_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Output

# COMMAND ----------

# Find the output file that was created
import glob
output_files = glob.glob(f"{OUTPUT_PATH}/*.jsonl")

if output_files:
    output_file = output_files[0]
    print(f"📊 Validating output: {output_file}")
    
    # Read with Spark to validate
    df = spark.read.json(output_file)
    
    count = df.count()
    print(f"✅ Total records: {count:,}")
    
    # Show schema
    print("\nSchema:")
    df.printSchema()
    
    # Show sample
    print("\nSample records:")
    df.show(5, truncate=False)
    
    # Check required fields
    required_fields = ['timestamp', 'service', 'message']
    missing_fields = [f for f in required_fields if f not in df.columns]
    
    if missing_fields:
        print(f"\n⚠️  Missing recommended fields: {missing_fields}")
        print("   You may need to transform the data in notebook 01")
    else:
        print("\n✅ All recommended fields present!")
else:
    print("⚠️  No output files found. Check source configuration.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = {
    "source_type": SOURCE_TYPE,
    "time_range": f"{START_TIME} to {END_TIME}",
    "output_path": OUTPUT_PATH,
    "records_ingested": count if 'count' in locals() else 0,
    "output_file": output_file if 'output_file' in locals() else "N/A",
    "status": "SUCCESS"
}

print("\n📈 Ingestion Summary:")
for key, value in summary.items():
    print(f"   {key}: {value}")

print("\n✅ Ready to run notebook 01 to ingest into Bronze layer!")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: Ingested {count:,} records from {SOURCE_TYPE}")

