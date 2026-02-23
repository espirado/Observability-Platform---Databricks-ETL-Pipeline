# Working with Real Production Logs

## Data Collection Strategy

### Step 1: Identify Your Log Sources

**Kubernetes Logs** (Recommended):
```bash
# Export logs from a specific namespace (last 24 hours)
kubectl logs --namespace=production \
  --since=24h \
  --all-containers=true \
  --prefix=true > k8s_logs.jsonl

# Or use a log aggregator
kubectl get pods -n production -o json | \
  jq -r '.items[] | .metadata.name' | \
  xargs -I {} kubectl logs {} -n production --since=24h
```

**Application Logs** (JSON format):
- Ensure logs are structured (JSON preferred)
- Must include: timestamp, service name, trace_id (if available)
- Recommended: status_code, latency_ms, endpoint

**Example Production Log**:
```json
{
  "timestamp": "2024-12-15T14:23:45.123Z",
  "level": "ERROR",
  "service": "payment-service",
  "http": {
    "method": "POST",
    "path": "/api/v1/checkout",
    "status_code": 500,
    "latency_ms": 1234.5
  },
  "trace": {
    "trace_id": "abc123def456",
    "span_id": "span789"
  },
  "message": "Database connection timeout",
  "error": {
    "type": "ConnectionTimeout",
    "stack_trace": "..."
  }
}
```

---

## Data Sanitization (CRITICAL!)

### Remove Sensitive Information

**Before uploading to Databricks**, sanitize your logs:

```python
# sanitize_logs.py
import json
import re
import hashlib

def sanitize_log(log_entry):
    """Remove PII and sensitive data from logs"""
    
    # Hash user IDs
    if 'user_id' in log_entry:
        log_entry['user_id'] = hashlib.sha256(
            log_entry['user_id'].encode()
        ).hexdigest()[:16]
    
    # Redact email addresses
    if 'message' in log_entry:
        log_entry['message'] = re.sub(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            '[EMAIL_REDACTED]',
            log_entry['message']
        )
    
    # Redact IP addresses (optional - depends on your requirements)
    if 'client_ip' in log_entry.get('http', {}):
        ip = log_entry['http']['client_ip']
        # Keep first two octets, mask last two
        log_entry['http']['client_ip'] = '.'.join(ip.split('.')[:2] + ['xxx', 'xxx'])
    
    # Remove credit card numbers
    if 'message' in log_entry:
        log_entry['message'] = re.sub(
            r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b',
            '[CC_REDACTED]',
            log_entry['message']
        )
    
    # Remove API keys/tokens
    if 'authorization' in log_entry.get('http', {}).get('headers', {}):
        log_entry['http']['headers']['authorization'] = '[REDACTED]'
    
    return log_entry

# Process file
with open('raw_logs.jsonl', 'r') as infile, \
     open('sanitized_logs.jsonl', 'w') as outfile:
    for line in infile:
        log = json.loads(line)
        sanitized = sanitize_log(log)
        outfile.write(json.dumps(sanitized) + '\n')
```

Run before uploading:
```bash
python sanitize_logs.py
# Upload sanitized_logs.jsonl to S3/ADLS
```

---

## Upload to Cloud Storage

### Option A: AWS S3

```bash
# Install AWS CLI
pip install awscli

# Configure credentials
aws configure

# Upload logs
aws s3 cp sanitized_logs.jsonl \
  s3://your-bucket/observability-data/raw-logs/2024-12-15/ \
  --region us-east-1
```

### Option B: Azure ADLS Gen2

```bash
# Install Azure CLI
pip install azure-cli

# Login
az login

# Upload logs
az storage blob upload \
  --account-name youraccount \
  --container-name observability-data \
  --name raw-logs/2024-12-15/sanitized_logs.jsonl \
  --file sanitized_logs.jsonl
```

### Option C: Databricks DBFS (Direct Upload)

For smaller datasets (< 2GB):
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure
databricks configure --token

# Upload to DBFS
databricks fs cp sanitized_logs.jsonl \
  dbfs:/observability-data/raw-logs/2024-12-15/
```

---

## Data Volume Recommendations

### For DS-610 Project

| Dataset Size | Processing Time | Recommendation |
|--------------|-----------------|----------------|
| 10K events | 30 sec | Good for testing |
| 100K events | 2-3 min | **Ideal for demo** |
| 1M events | 10-15 min | Shows scale, still manageable |
| 10M+ events | 1+ hour | Only if you have time |

**Recommendation**: Start with **100K events** (1 day of logs from a few services).

### Sample Your Production Data

If you have millions of logs, sample intelligently:

```bash
# Sample 10% of logs
cat full_logs.jsonl | awk 'BEGIN {srand()} {if (rand() < 0.1) print}' > sampled_logs.jsonl

# Or: Keep all errors, sample success
jq -c 'select(.level == "ERROR" or (.level != "ERROR" and (now % 10) == 0))' \
  full_logs.jsonl > sampled_logs.jsonl
```

---

## Schema Validation

Before processing, validate your log schema:

```python
# validate_schema.py
import json
from collections import Counter

def analyze_schema(jsonl_file, num_samples=1000):
    """Analyze log structure to identify common fields"""
    
    field_counts = Counter()
    field_types = {}
    
    with open(jsonl_file, 'r') as f:
        for i, line in enumerate(f):
            if i >= num_samples:
                break
            
            log = json.loads(line)
            
            # Count field occurrences
            for key in log.keys():
                field_counts[key] += 1
                
                # Track field types
                if key not in field_types:
                    field_types[key] = type(log[key]).__name__
    
    print("Field Analysis:")
    print("-" * 60)
    for field, count in field_counts.most_common():
        percentage = (count / num_samples) * 100
        field_type = field_types.get(field, 'unknown')
        print(f"{field:30s} {percentage:5.1f}% ({field_type})")
    
    # Check required fields
    required = ['timestamp', 'service', 'message']
    missing = [f for f in required if field_counts[f] < num_samples * 0.95]
    
    if missing:
        print(f"\n⚠️  WARNING: Missing required fields: {missing}")
    else:
        print(f"\n✅ All required fields present")

# Run analysis
analyze_schema('sanitized_logs.jsonl')
```

**Expected output**:
```
Field Analysis:
------------------------------------------------------------
timestamp                       100.0% (str)
service                         100.0% (str)
message                         100.0% (str)
level                            98.5% (str)
http                             75.2% (dict)
trace                            45.8% (dict)
kubernetes                       30.1% (dict)

✅ All required fields present
```

---

## Cost Estimation

### Storage Costs

**Assumptions**: 1M events/day, 500 bytes/event

| Storage | Duration | Size | Monthly Cost (AWS S3) |
|---------|----------|------|----------------------|
| Bronze (raw) | 7 days | 3.5 GB | $0.08 |
| Silver (enriched) | 90 days | 45 GB | $1.04 |
| Gold (aggregated) | 1 year | 180 GB | $4.14 |
| **Total** | | **228.5 GB** | **$5.26/month** |

### Databricks Compute Costs

| Job | Cluster | Runtime | Runs/Month | Monthly Cost |
|-----|---------|---------|------------|--------------|
| Hourly ETL | 4 × i3.xlarge | 30 min | 720 | ~$400 (spot: $120) |
| One-time analysis | 2 × i3.xlarge | 2 hours | 1 | ~$2 |

**For DS-610 Project**: Use **Databricks Community Edition** (free) with sampled data.

---

## Privacy & Compliance Checklist

Before using production logs:

- [ ] **Remove PII**: User IDs, emails, IP addresses
- [ ] **Redact credentials**: API keys, passwords, tokens
- [ ] **Hash identifiers**: Order IDs, payment IDs (use consistent hashing)
- [ ] **Remove stack traces**: May contain internal paths/secrets
- [ ] **Check compliance**: GDPR, HIPAA, CCPA requirements
- [ ] **Get approval**: From your team/company before using production data
- [ ] **Document sanitization**: Keep record of what was removed

---

## Troubleshooting

### "Schema mismatch" errors

**Problem**: Your logs have inconsistent structure

**Solution**: Add schema enforcement in Notebook 01:
```python
# Set mode to PERMISSIVE (don't fail on bad records)
.option("mode", "PERMISSIVE")
.option("columnNameOfCorruptRecord", "_corrupt_record")
```

### "Too many small files" warning

**Problem**: Each log upload creates many small files

**Solution**: Optimize after ingestion:
```python
spark.sql("OPTIMIZE delta.`/mnt/observability/bronze/logs`")
```

### "Out of memory" errors

**Problem**: Dataset too large for cluster

**Solution**: 
1. Reduce dataset size (sample more aggressively)
2. Increase cluster size (add more workers)
3. Process in smaller batches (partition by day)

---

## Example: Real Kubernetes Log Pipeline

```bash
# 1. Export logs from production cluster
kubectl logs --namespace=production \
  --selector=app=payment-service \
  --since=24h > payment_logs.txt

# 2. Convert to JSONL (if not already)
python convert_to_jsonl.py payment_logs.txt > payment_logs.jsonl

# 3. Sanitize
python sanitize_logs.py payment_logs.jsonl > sanitized.jsonl

# 4. Upload to S3
aws s3 cp sanitized.jsonl s3://mybucket/logs/2024-12-15/

# 5. Process with Databricks
databricks runs submit --json @jobs/hourly_etl.json
```

---

## Next Steps

1. **Collect 1 day** of logs from your production system
2. **Sanitize** using the script above
3. **Validate schema** to ensure required fields present
4. **Upload** to cloud storage or DBFS
5. **Run Notebook 01** to ingest into Bronze layer
6. **Verify** data quality and completeness

---

**Questions?** See [RESEARCH_GOALS.md](../RESEARCH_GOALS.md) for the bigger picture.

