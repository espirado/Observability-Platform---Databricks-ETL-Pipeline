# Databricks Workspace Setup Guide

## Prerequisites

- Databricks workspace (AWS, Azure, or GCP)
- Cloud storage account (S3/ADLS/GCS)
- Databricks CLI installed locally
- ClickHouse instance for final data export

## Step 1: Install Databricks CLI

```bash
pip install databricks-cli
```

## Step 2: Configure Authentication

```bash
databricks configure --token
```

Enter your Databricks workspace URL and personal access token when prompted.

## Step 3: Create Storage Mount

### For AWS S3:

```python
# Run in a Databricks notebook
dbutils.fs.mount(
    source="s3a://your-bucket-name/observability-data",
    mount_point="/mnt/observability",
    extra_configs={
        "fs.s3a.access.key": dbutils.secrets.get(scope="observability", key="aws-access-key"),
        "fs.s3a.secret.key": dbutils.secrets.get(scope="observability", key="aws-secret-key")
    }
)
```

### For Azure ADLS Gen2:

```python
dbutils.fs.mount(
    source="abfss://container@storageaccount.dfs.core.windows.net/observability-data",
    mount_point="/mnt/observability",
    extra_configs={
        "fs.azure.account.key.storageaccount.dfs.core.windows.net": 
            dbutils.secrets.get(scope="observability", key="azure-storage-key")
    }
)
```

### Verify Mount:

```python
display(dbutils.fs.ls("/mnt/observability"))
```

## Step 4: Create Secrets Scope

```bash
# Create secrets scope
databricks secrets create-scope --scope observability

# Add secrets
databricks secrets put --scope observability --key aws-access-key
databricks secrets put --scope observability --key aws-secret-key
databricks secrets put --scope observability --key clickhouse-host
databricks secrets put --scope observability --key clickhouse-password
```

## Step 5: Import Notebooks

```bash
# Clone this repository
git clone <repo-url>
cd observability-databricks

# Import notebooks to workspace
databricks workspace import_dir notebooks /Workspace/observability-etl --overwrite
```

Verify in Databricks UI: Workspace → observability-etl

## Step 6: Create Cluster

### Via UI:
1. Go to Compute → Create Cluster
2. Name: `observability-etl-cluster`
3. Databricks Runtime: `13.3 LTS (includes Apache Spark 3.4.1, Scala 2.12)`
4. Worker type: `i3.xlarge` (AWS) or equivalent
5. Workers: 2-20 (with autoscaling)
6. Enable spot instances for cost savings

### Via CLI:

```bash
databricks clusters create --json-file config/cluster_config.json
```

## Step 7: Create Delta Tables

Run the setup notebook to create Bronze/Silver/Gold directories:

```bash
databricks runs submit --json '{
  "run_name": "Setup Delta Tables",
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2
  },
  "notebook_task": {
    "notebook_path": "/Workspace/observability-etl/00_setup_tables",
    "base_parameters": {}
  }
}'
```

## Step 8: Test Individual Notebooks

### Test Bronze Ingestion:

```bash
databricks runs submit --json '{
  "run_name": "Test Bronze Ingestion",
  "existing_cluster_id": "<your-cluster-id>",
  "notebook_task": {
    "notebook_path": "/Workspace/observability-etl/01_ingest_raw_logs",
    "base_parameters": {
      "input_date": "2024-12-15"
    }
  }
}'
```

Check run status:
```bash
databricks runs list --limit 10
databricks runs get --run-id <run-id>
```

### Test Silver Enrichment:

```bash
databricks runs submit --json '{
  "run_name": "Test Silver Enrichment",
  "existing_cluster_id": "<your-cluster-id>",
  "notebook_task": {
    "notebook_path": "/Workspace/observability-etl/02_enrich_events",
    "base_parameters": {
      "input_date": "2024-12-15"
    }
  }
}'
```

## Step 9: Schedule Hourly Job

```bash
# Create job from configuration
databricks jobs create --json-file jobs/hourly_etl.json

# List jobs
databricks jobs list

# Manually trigger job
databricks jobs run-now --job-id <job-id>
```

## Step 10: Monitor Jobs

### Via UI:
- Workflows → Jobs → Select job
- View run history, logs, and metrics

### Via CLI:
```bash
# List recent runs
databricks runs list --job-id <job-id> --limit 10

# Get run details
databricks runs get --run-id <run-id>

# Get run output
databricks runs get-output --run-id <run-id>
```

## Troubleshooting

### Mount point not found

```python
# Check existing mounts
display(dbutils.fs.mounts())

# Unmount if needed
dbutils.fs.unmount("/mnt/observability")

# Re-mount with correct credentials
```

### Notebook import failed

```bash
# Check workspace path exists
databricks workspace ls /Workspace

# Create directory if needed
databricks workspace mkdirs /Workspace/observability-etl

# Try import again
databricks workspace import_dir notebooks /Workspace/observability-etl --overwrite
```

### Job fails with "No such file or directory"

- Check that mount point is accessible from cluster
- Verify input_date parameter format (YYYY-MM-DD)
- Ensure Bronze table exists before running Silver notebook

### Delta table schema evolution errors

```python
# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Or add to notebook:
.option("mergeSchema", "true")
```

## Cost Optimization

### Use Spot Instances

In cluster configuration:
```json
"aws_attributes": {
  "availability": "SPOT_WITH_FALLBACK",
  "spot_bid_price_percent": 100
}
```

### Auto-termination

Set cluster to auto-terminate after 30 minutes of inactivity:
```json
"autotermination_minutes": 30
```

### Right-size Clusters

Start small and scale up as needed:
- Development: 2 workers
- Production: 4-8 workers with autoscaling

### Delta Table Optimization

Run OPTIMIZE and VACUUM regularly:
```python
spark.sql("OPTIMIZE delta.`/mnt/observability/silver/events`")
spark.sql("VACUUM delta.`/mnt/observability/silver/events` RETAIN 168 HOURS")  # 7 days
```

## Next Steps

1. **Ingest Sample Data**: Upload sample JSONL logs to S3/ADLS
2. **Run Full Pipeline**: Execute hourly ETL job end-to-end
3. **Verify ClickHouse Sync**: Check that data appears in ClickHouse
4. **Set Up Monitoring**: Configure job alerts and metrics
5. **Optimize Performance**: Tune cluster size and Spark configs

## Additional Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Guide](https://docs.delta.io/)
- [Spark SQL Reference](https://spark.apache.org/docs/latest/sql-ref.html)
- [ClickHouse Integration](https://clickhouse.com/docs/en/integrations/data-ingestion/dbms/jdbc-with-apache-spark)

---

**Questions?**  
See [README.md](../README.md) or open an issue.

