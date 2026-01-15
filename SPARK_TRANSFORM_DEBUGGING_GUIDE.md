# Spark Transform Debugging Guide

This document compiles all the issues we encountered while implementing the `transform_data` task in the cryptocurrency ETL pipeline, along with their solutions.

---

## Problem #1: JSON Format Mismatch (multiLine Issue)

### Error Message
```
org.apache.spark.sql.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the referenced columns only include the internal corrupt record column (named _corrupt_record by default).
```

### What Happened
- The `store_data` task wrote a **JSON array** to MinIO:
  ```json
  [
    {"id": "bitcoin", "price": 42000, ...},
    {"id": "ethereum", "price": 2200, ...}
  ]
  ```
- Spark's default JSON reader expects **JSON Lines format** (one object per line):
  ```json
  {"id": "bitcoin", "price": 42000, ...}
  {"id": "ethereum", "price": 2200, ...}
  ```

### Root Cause
**Data format mismatch** between what we wrote (JSON array) and what Spark expected (JSON Lines).

### Solution
Add `.option("multiLine", "true")` to tell Spark to read JSON arrays:

```python
# BEFORE (Failed)
df = spark.read.json(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}")

# AFTER (Working)
df = spark.read.option("multiLine", "true").json(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}")
```

**File Changed**: `spark/notebooks/stock_transform/crypto_transform.py`

---

## Problem #2: Docker Image Caching

### What Happened
When rebuilding the Docker image with:
```bash
docker build -t airflow/stock-app .
```

All layers showed **`CACHED`**:
```
=> CACHED [2/6] ONBUILD COPY requirements.txt /app/            0.0s
=> CACHED [3/6] ONBUILD RUN cd /app ...                        0.0s
=> CACHED [5/6] COPY crypto_transform.py /app/                 0.0s  ← Old code!
```

This means **Docker reused the old cached layers** instead of copying the updated `crypto_transform.py` file.

### Root Cause
Docker caches layers for efficiency. If the Dockerfile itself doesn't change, Docker assumes the content is the same and reuses cached layers, even when source files have been modified.

### Solution
Force a fresh rebuild using `--no-cache`:

```bash
cd /Users/bappenas/astro_project_1/spark/notebooks/stock_transform
docker build --no-cache -t airflow/stock-app .
```

**Key Indicators of Success**:
- Build time increases (90-120 seconds vs 5-10 seconds)
- Each layer shows actual execution time instead of `CACHED`
- Layer 5 shows `=> [5/6] COPY crypto_transform.py /app/  0.1s` (not CACHED)

---

## Problem #3: Nested Struct Cannot Be Written to CSV

### Error Message
```
pyspark.sql.utils.AnalysisException: CSV data source does not support struct<currency:string,percentage:double,times:double> data type.
```

### What Happened
The CoinGecko API returns a nested `roi` object for some cryptocurrencies:
```json
{
  "id": "ethereum",
  "price": 2960.64,
  "roi": {
    "currency": "btc",
    "percentage": 4364.066,
    "times": 43.640
  }
}
```

**CSV format only supports flat data** (scalars like strings, numbers, booleans), not nested objects/structs.

### Root Cause
Trying to write a DataFrame with nested/struct columns directly to CSV format.

### Solution
**Flatten the nested struct** into separate columns before writing to CSV:

```python
# Extract nested fields into separate columns
df_flattened = df.withColumn('roi_currency', df.roi.currency) \
    .withColumn('roi_percentage', df.roi.percentage) \
    .withColumn('roi_times', df.roi.times) \
    .drop('roi')  # Remove the original nested column
```

**Complete Fix in Context**:
```python
# Read JSON from MinIO
df = spark.read.option("multiLine", "true").json(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}")

# Flatten nested 'roi' struct (CSV doesn't support nested objects)
df_flattened = df.withColumn('roi_currency', df.roi.currency) \
    .withColumn('roi_percentage', df.roi.percentage) \
    .withColumn('roi_times', df.roi.times) \
    .drop('roi')

# Add timestamp
df_with_timestamp = df_flattened.withColumn('processed_at', current_timestamp())

# Write to CSV - now works!
df_with_timestamp.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"s3a://crypto-market/transformed_prices")
```

**File Changed**: `spark/notebooks/stock_transform/crypto_transform.py`

---

## Bonus Tip: Understanding `indent=2`

In the `store_data` task, we use:
```python
json_data = json.dumps(crypto_data, indent=2)
```

### What `indent=2` Does
Controls **formatting** (pretty-printing) of JSON output:

**Without indent** (compact):
```json
[{"id":"bitcoin","price":42000},{"id":"ethereum","price":2200}]
```

**With `indent=2`** (2 spaces per level):
```json
[
  {
    "id": "bitcoin",
    "price": 42000
  },
  {
    "id": "ethereum",
    "price": 2200
  }
]
```

**Purpose**: Makes JSON human-readable when viewing in MinIO or text editors. The actual data is identical—it's just cosmetic formatting.

---

## Complete Debugging Checklist

When `transform_data` fails (task turns green but no output):

1. **Check Airflow Logs**
   - Go to Airflow UI → DAG → Latest Run → `transform_data` task → Logs
   - Look for `AnalysisException` or error messages

2. **Common Issues & Quick Fixes**:
   - **"corrupt record"** → Add `.option("multiLine", "true")`
   - **"CSV does not support struct"** → Flatten nested objects
   - **"File not found"** → Check spaces in filenames (use `%Y%m%d_%H%M%S` format)
   - **Changes not reflected** → Rebuild Docker with `--no-cache`

3. **Always Rebuild Docker After Code Changes**:
   ```bash
   cd spark/notebooks/stock_transform
   docker build --no-cache -t airflow/stock-app .
   ```

4. **Verify Success**:
   - Check MinIO browser: `http://localhost:9001/browser/crypto-market/transformed_prices/`
   - Look for `part-00000-*.csv` files and `_SUCCESS` marker

---

## Summary of Fixes Applied

| Issue | File Modified | Key Change |
|-------|--------------|------------|
| JSON format mismatch | `crypto_transform.py` | Added `.option("multiLine", "true")` |
| Nested struct in CSV | `crypto_transform.py` | Flattened `roi` object into 3 columns |
| Docker caching | Build process | Used `docker build --no-cache` |

All issues resolved! ✅
