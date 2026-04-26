---
name: etl-best-practices
description: Provide comprehensive Databricks data engineering best practices for ETL development. Use when the user asks for best practices, coding standards, guidelines, recommendations, or a review of ETL code quality on Databricks — covering code structure, ingestion, transformation, Delta Lake, performance, security, testing, error handling, and operations.
---

# Databricks ETL Development Best Practices

Provide a comprehensive best-practices review or guide for Databricks ETL development. Tailor the response to the user's context — if they share code, review it against these standards. If they ask for general guidance, produce a structured guide covering all relevant sections below.

---

## 1. Project Structure

Every Databricks ETL project must follow a consistent layout that separates concerns and supports CI/CD.

```
project/
├── src/
│   ├── extract/        # Source readers — one module per source system
│   ├── transform/      # Pure transformation functions — no I/O
│   ├── load/           # Delta writers, merge logic
│   └── utils/          # Schema definitions, logging helpers, constants
├── tests/
│   ├── conftest.py     # Shared SparkSession fixture
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── notebooks/          # Thin orchestration only — import from src/, never embed logic
├── migrations/         # Versioned DDL: V001__add_column.sql, V002__rename.sql
├── schema_contracts/   # YAML schema definitions per table
├── databricks.yml      # Databricks Asset Bundle definition
└── pyproject.toml      # pytest, coverage, linting config
```

**Rules:**
- Notebooks are orchestration shells only — all logic lives in `src/` Python modules
- One function per file for transformations — makes unit testing trivial
- `src/utils/schemas.py` holds all `StructType` definitions — single source of truth
- No business logic in `notebooks/` — if you can't test it with pytest, it doesn't belong there

---

## 2. Schema Management

### Always define explicit schemas — never infer

```python
# BAD — schema inference is slow, non-deterministic, and fails on empty files
df = spark.read.option("inferSchema", "true").csv(path)

# GOOD — explicit StructType in src/utils/schemas.py
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

ORDER_SCHEMA = StructType([
    StructField("order_id",    StringType(),      nullable=False),
    StructField("customer_id", StringType(),      nullable=False),
    StructField("amount",      DecimalType(18,4), nullable=True),
    StructField("order_date",  TimestampType(),   nullable=False),
    StructField("status",      StringType(),      nullable=True),
])

df = spark.read.schema(ORDER_SCHEMA).parquet(path)
```

### Schema evolution rules
- **Additive changes** (new nullable columns): safe — use `mergeSchema` or `ADD COLUMN`
- **Type widening** (INT → BIGINT): safe on Delta 3.0+ with `delta.enableTypeWidening`
- **Breaking changes** (type incompatibility, drop required column): always `SHALLOW CLONE` first, then `overwriteSchema`
- Commit a `schema_contract.yml` per table to git; run schema drift detection in CI before every deploy

---

## 3. Data Ingestion

### File ingestion — always use Auto Loader

```python
# BAD — spark.read is not idempotent for incremental file ingestion
df = spark.read.parquet("s3://bucket/landing/")

# GOOD — Auto Loader tracks processed files in a checkpoint
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")   # unexpected columns → _rescued_data
    .schema(ORDER_SCHEMA)
    .load("s3://bucket/landing/")
)
```

### JDBC ingestion — always parallelise reads

```python
# BAD — single-partition JDBC read; entire table on one executor
df = spark.read.jdbc(url=url, table="orders", properties=props)

# GOOD — parallel partitioned read
df = spark.read.jdbc(
    url=url,
    table="(SELECT * FROM orders WHERE updated_at >= '2024-01-01') t",
    column="order_id",          # numeric or date partition column
    lowerBound=1,
    upperBound=10_000_000,
    numPartitions=16,            # match to available cores
    properties={"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "fetchsize": "10000"}
)
```

### Secrets — never hardcode credentials

```python
# BAD
jdbc_password = "mypassword123"

# GOOD
jdbc_password = dbutils.secrets.get(scope="jdbc-secrets", key="db-password")
```

---

## 4. Transformation Best Practices

### Use native Spark SQL functions — avoid Python UDFs

```python
from pyspark.sql import functions as F

# BAD — Python UDF: serialization overhead, no Catalyst optimisation, no Photon support
from pyspark.sql.functions import udf
@udf("string")
def clean_phone(p): return re.sub(r'\D', '', p) if p else None
df = df.withColumn("phone", clean_phone("raw_phone"))

# GOOD — native function
df = df.withColumn("phone", F.regexp_replace(F.col("raw_phone"), r'\D', ''))

# If vectorized logic is unavoidable, use Pandas UDF (runs on Arrow batches, Photon-compatible)
from pyspark.sql.functions import pandas_udf
import pandas as pd
@pandas_udf("string")
def complex_clean(s: pd.Series) -> pd.Series:
    return s.str.replace(r'\D', '', regex=True)
```

### Null handling — always explicit

```python
# BAD — silently drops rows or propagates nulls
df = df.filter(df.customer_id.isNotNull())  # silent row loss without logging

# GOOD — quarantine nulls with a reason, log the count
null_rows = df.filter(F.col("customer_id").isNull())
valid_rows = df.filter(F.col("customer_id").isNotNull())
null_count = null_rows.count()
if null_count > 0:
    null_rows.withColumn("reject_reason", F.lit("null_customer_id")) \
             .write.format("delta").mode("append").saveAsTable("catalog.dq.rejected_orders")
    print(f"WARNING: {null_count} rows rejected due to null customer_id")
```

### Type casting — always safe cast

```python
# BAD — hard cast raises AnalysisException on bad values
df = df.withColumn("amount", F.col("raw_amount").cast("decimal(18,4)"))

# GOOD — try_cast returns null on bad values; handle downstream
df = df.withColumn("amount", F.expr("try_cast(raw_amount AS DECIMAL(18,4))"))
df = df.withColumn("amount_invalid", F.col("amount").isNull() & F.col("raw_amount").isNotNull())
```

### Deduplication — always use window functions for last-write-wins

```python
from pyspark.sql.window import Window

# BAD — dropDuplicates keeps arbitrary row when multiple timestamps exist
df = df.dropDuplicates(["order_id"])

# GOOD — deterministic: keep the latest version by timestamp
window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
df = (
    df.withColumn("_rn", F.row_number().over(window))
      .filter(F.col("_rn") == 1)
      .drop("_rn")
)
```

### Avoid wide transformations inside loops

```python
# BAD — iterative withColumn in a loop creates a deeply nested plan
for col_name in columns:
    df = df.withColumn(col_name, transform(col_name))

# GOOD — build all expressions at once
exprs = [transform(c).alias(c) for c in columns] + [F.col(c) for c in other_cols]
df = df.select(*exprs)
```

---

## 5. Delta Lake Best Practices

### Write patterns

```python
# Full refresh — always use replaceWhere when possible to avoid full table rewrite
df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "order_date >= '2024-01-01'") \
    .saveAsTable("catalog.silver.orders")

# Upsert — always use MERGE, never overwrite + reinsert
from delta.tables import DeltaTable
(
    DeltaTable.forName(spark, "catalog.silver.orders").alias("t")
    .merge(df.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

### Table properties — set on every production table

```sql
CREATE TABLE IF NOT EXISTS catalog.silver.orders (...)
USING DELTA
PARTITIONED BY (order_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite'    = 'true',   -- auto bin-pack small files on write
  'delta.autoOptimize.autoCompact'      = 'true',   -- async compaction after write
  'delta.enableChangeDataFeed'          = 'true',   -- enables CDC for downstream consumers
  'delta.dataSkippingNumIndexedCols'    = '8',      -- index top 8 cols for data skipping
  'delta.targetFileSize'               = '134217728' -- target 128 MB files
);
```

### OPTIMIZE and ZORDER — run after every large load

```python
# Run after bulk writes to compact files and co-locate data for common filters
spark.sql("OPTIMIZE catalog.silver.orders ZORDER BY (customer_id, order_date)")

# VACUUM — remove files older than retention period (default 7 days)
spark.sql("VACUUM catalog.silver.orders RETAIN 168 HOURS")
```

### Partitioning rules
- Partition only on columns used in `WHERE` filters with high cardinality at the partition level (date, region)
- Never partition on columns with < 10 distinct values (e.g., `status`) — creates too many small partitions
- Never partition on columns with > 10,000 distinct values (e.g., `customer_id`) — too many partition directories
- Rule of thumb: each partition directory should contain files totalling 256 MB – 1 GB

### ZORDER column selection
- Choose columns most frequently used in `WHERE`, `JOIN ON`, and `GROUP BY`
- Maximum 4 columns — effectiveness degrades beyond that
- Do not ZORDER on the partition column itself

---

## 6. Performance Optimization

### Enable Adaptive Query Execution (AQE) — always on

```python
# Set at the job level in cluster config or databricks.yml spark_conf
spark.conf.set("spark.sql.adaptive.enabled",                        "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",     "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",               "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled",     "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",      "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",        "true")
```

### Shuffle partition tuning

```python
# BAD — default 200 partitions regardless of data size
# GOOD — size to 2-3x total cores; target 128 MB per shuffle partition
total_cores = spark.sparkContext.defaultParallelism
spark.conf.set("spark.sql.shuffle.partitions", str(total_cores * 2))
```

### Broadcast joins — always hint small tables

```python
# BAD — Spark may choose a sort-merge join for a 10 MB lookup table
df = large_df.join(lookup_df, "customer_id")

# GOOD — force broadcast for tables under ~200 MB
from pyspark.sql.functions import broadcast
df = large_df.join(broadcast(lookup_df), "customer_id")

# Or configure the threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(200 * 1024 * 1024))  # 200 MB
```

### Cache and persist — use deliberately

```python
# BAD — caching everything wastes memory; caching nothing recomputes expensive plans
# GOOD — cache only DataFrames used more than once in the same job
df_transformed = extract_and_transform()
df_transformed.cache()                # triggers lazy evaluation on first action

count = df_transformed.count()        # first action — materialises cache
df_transformed.write...               # reads from cache

df_transformed.unpersist()            # release memory when done
```

### Avoid driver bottlenecks

```python
# NEVER on large DataFrames
result = df.collect()       # pulls all data to driver
result = df.toPandas()      # same — OOM risk

# GOOD — keep processing on executors; only collect aggregated results
summary = df.agg(F.count("*"), F.sum("amount")).collect()   # tiny result, safe
```

---

## 7. Error Handling and Resilience

### Every ETL job must be idempotent

An ETL job is idempotent if running it twice produces the same result as running it once.

```python
# Achieve idempotency with:
# 1. MERGE INTO (upsert) instead of INSERT
# 2. replaceWhere for partition-scoped overwrites
# 3. Control table tracking what was loaded and when
# 4. Watermark-based reads that use the same window on retry
```

### Structured try/except with control table update

```python
run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()

try:
    df = extract(source)
    df_clean = transform(df)
    load(df_clean, target_table)
    log_run(run_id, "SUCCESS", rows_written=df_clean.count())

except Exception as e:
    log_run(run_id, "FAILED", error_msg=str(e))
    raise   # always re-raise so Databricks marks the job as failed

finally:
    df_transformed.unpersist() if 'df_transformed' in dir() else None
```

### Dead-letter table for bad records

```python
# Instead of failing the entire job on bad rows, route them to a quarantine table
valid   = df.filter(is_valid_udf(F.struct(*df.columns)))
invalid = df.subtract(valid).withColumn("reject_reason", diagnose_udf(F.struct(*df.columns))) \
            .withColumn("rejected_at", F.current_timestamp()) \
            .withColumn("pipeline_name", F.lit(pipeline_name))

invalid.write.format("delta").mode("append").saveAsTable("catalog.dq.dead_letter")
```

### Retry pattern for transient source failures

```python
import time

def read_with_retry(read_fn, max_retries=3, backoff_seconds=30):
    for attempt in range(1, max_retries + 1):
        try:
            return read_fn()
        except Exception as e:
            if attempt == max_retries:
                raise
            wait = backoff_seconds * (2 ** (attempt - 1))
            print(f"Attempt {attempt} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
```

---

## 8. Security and Governance

### Naming conventions (Unity Catalog)

```
<env>_<domain>_catalog          # dev_sales_catalog, prod_sales_catalog
  └── bronze                    # schema per medallion layer
  └── silver
  └── gold
  └── semantic
      └── <entity>_<grain>      # fact_orders, dim_customer, obt_orders
```

- Three-part names everywhere: `catalog.schema.table` — never `USE DATABASE`
- Prefix temp/intermediate tables with `_tmp_` — makes cleanup scripts reliable
- Suffix external tables with `_ext` — distinguishes managed from unmanaged

### Least-privilege access pattern

```sql
-- Grant to groups, never to individual users
GRANT USE CATALOG ON CATALOG prod_sales_catalog TO `data-engineers`;
GRANT USE SCHEMA  ON SCHEMA  prod_sales_catalog.silver TO `data-engineers`;
GRANT SELECT      ON TABLE   prod_sales_catalog.silver.orders TO `data-analysts`;
GRANT MODIFY      ON TABLE   prod_sales_catalog.silver.orders TO `etl-service-principal`;
```

### Audit and lineage

```sql
-- Unity Catalog system tables capture all access — query to audit
SELECT user_name, action_name, table_full_name, event_time
FROM system.access.audit
WHERE table_full_name = 'catalog.silver.orders'
  AND event_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY event_time DESC;
```

---

## 9. Testing Standards

### Unit test every transformation function

- All unit tests run locally with `SparkSession.builder.master("local[2]")` — no cluster required
- Required test cases per function: happy path, null input, empty DataFrame, type boundary, idempotency
- Use `pyspark.testing.assertDataFrameEqual` — checks schema and data in one assertion
- Use `tmp_path` pytest fixture for Delta table tests — never write to real tables in unit tests

### Test pyramid

```
         [Integration tests]   ← Run against real Databricks cluster in CI nightly
        [Component tests]      ← Test extract/transform/load stages end-to-end with sample data
      [Unit tests]             ← Test individual functions locally; must pass before every PR merge
```

### Schema contract enforcement in CI

```yaml
# .github/workflows/ci.yml
- name: Unit tests
  run: pytest tests/ -m "not integration" --cov=src --cov-fail-under=80

- name: Schema drift check
  run: python scripts/check_schema_contracts.py

- name: Bundle validate
  run: databricks bundle validate --target dev
```

---

## 10. Operational Best Practices

### Cluster configuration

```yaml
# Always use job clusters for production ETL — never all-purpose clusters
job_clusters:
  - job_cluster_key: etl_cluster
    new_cluster:
      spark_version: 15.4.x-scala2.12
      node_type_id: Standard_DS4_v2
      autoscale:
        min_workers: 2
        max_workers: 8
      azure_attributes:
        availability: SPOT_WITH_FALLBACK_AZURE   # cost reduction; fallback on preemption
        first_on_demand: 1                        # keep 1 on-demand as driver
      spark_conf:
        spark.sql.adaptive.enabled: "true"
        spark.databricks.delta.optimizeWrite.enabled: "true"
```

**Rules:**
- Job clusters for every scheduled ETL run — all-purpose clusters are for development only
- Enable spot/preemptible instances with on-demand fallback for cost savings
- Autoscale between 2 and 8 workers — set min > 0 to avoid cold start delay
- Enable Photon runtime (`spark_version: 15.4.x-photon-scala2.12`) for SQL-heavy workloads

### Control table — every pipeline must have one

```sql
CREATE TABLE IF NOT EXISTS catalog.observability.pipeline_control (
  pipeline_name  STRING    NOT NULL,
  run_id         STRING    NOT NULL,
  status         STRING    NOT NULL,  -- RUNNING | SUCCESS | FAILED
  rows_read      BIGINT,
  rows_written   BIGINT,
  started_at     TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  completed_at   TIMESTAMP,
  error_message  STRING
) USING DELTA;
```

- Write `RUNNING` at job start, update to `SUCCESS` or `FAILED` at end
- The watermark / HWM for incremental loads is only advanced on `SUCCESS`

### Logging standards

```python
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Log at every stage boundary with row counts
logger.info(f"[EXTRACT] Read {source_count:,} rows from {source}")
logger.info(f"[TRANSFORM] {valid_count:,} valid rows; {null_count:,} rejected")
logger.info(f"[LOAD] Wrote {written_count:,} rows to {target_table}")
```

### Job design rules

- `max_concurrent_runs: 1` — always; prevents overlapping writes to the same table
- `max_retries: 2` with `min_retry_interval_millis: 300000` (5 min) — retries on transient failures
- `on_failure` email notification — every production job
- Separate job tasks for extract, transform, and load when stages exceed 30 minutes — enables partial retry
- `depends_on` with `run_if: ALL_SUCCESS` to prevent downstream tasks running on upstream failure

---

## 11. Code Quality Standards

### Type hints and docstrings on all public functions

```python
from pyspark.sql import DataFrame

def clean_orders(df: DataFrame, *, drop_nulls: bool = True) -> DataFrame:
    """
    Cleanses raw order data: casts types, deduplicates, optionally drops nulls.
    Returns a DataFrame matching ORDER_SCHEMA.
    """
    ...
```

### Constants — never hardcode magic values

```python
# BAD
df = df.filter("status IN ('OPEN', 'CLOSED')")

# GOOD — in src/utils/constants.py
VALID_STATUSES = ("OPEN", "CLOSED", "CANCELLED", "RETURNED")
df = df.filter(F.col("status").isin(VALID_STATUSES))
```

### Linting and formatting

```toml
# pyproject.toml
[tool.ruff]
select = ["E", "F", "I", "UP"]
line-length = 120

[tool.mypy]
ignore_missing_imports = true
```

```yaml
# GitHub Actions lint gate (runs before tests)
- name: Lint
  run: ruff check src/ tests/
```

---

## 12. Common Anti-Patterns — Quick Reference

| Anti-pattern | Impact | Fix |
|---|---|---|
| `inferSchema=True` in production | Slow, non-deterministic | Explicit `StructType` |
| Python UDFs on large columns | 10–100x slower, no Photon | Native Spark SQL functions |
| `.collect()` on large DataFrames | Driver OOM | Keep transformations on executors |
| Missing `.cache()` on reused DFs | Recomputes expensive plans | `.cache()` before first action |
| `repartition(n)` before write | Extra unnecessary shuffle | Let AQE handle coalescing |
| Hardcoded credentials in code | Security vulnerability | `dbutils.secrets.get()` |
| `spark.sql.shuffle.partitions=200` | Wrong for most workloads | Tune to 2-3× total cores |
| No `max_concurrent_runs` limit | Duplicate writes | Always set to 1 for ETL jobs |
| Logic in notebooks, not modules | Untestable, unversionable | Move to `src/` Python modules |
| Full table overwrite for incremental | Rewrites all data | `replaceWhere` or `MERGE INTO` |
| No control/audit table | Blind to failures | Log every run start/end/status |
| No watermark on streaming joins | Unbounded state, OOM | `.withWatermark()` always |

---

## Applying This Guide

When reviewing user-provided code, go through each section above and flag every violation as:

`[SECTION] [SEVERITY: HIGH|MED|LOW] <Issue> — <why it matters> → <fix>`

Then produce a corrected version of the code with all HIGH and MED severity issues resolved, and a summary table of every finding.

When generating new ETL code (not reviewing), apply all standards above by default without being asked.
