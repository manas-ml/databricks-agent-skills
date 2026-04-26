---
name: etl-pipeline-scaffold
description: Scaffold a production-ready PySpark ETL pipeline on Databricks. Use when the user asks to build, create, generate, or scaffold an ETL pipeline, data pipeline, or ingestion job on Databricks involving a source system and a Delta Lake target table.
---

# ETL Pipeline Scaffold

Generate a production-ready PySpark ETL pipeline on Databricks based on the user's requirement.

## Step 1 — Clarify before generating

If the user's request does not already specify these, ask once before writing code:
- Source system and format (JDBC, S3/ADLS, Kafka, files — CSV/JSON/Parquet/Avro)
- Target Delta table (catalog.schema.table in Unity Catalog)
- Load mode: full refresh, append, or upsert (MERGE)
- Primary/business keys (required for upsert)
- Any known transformations or business rules

## Step 2 — Generate the ETL notebook

Produce Python code blocks for each section, ready to paste into a Databricks notebook.

### Section 1 — Imports & Config
- Standard PySpark imports (`from pyspark.sql import functions as F`, `from pyspark.sql.types import *`, `from delta.tables import DeltaTable`)
- `dbutils.widgets` for runtime parameters: source path, target table, load date
- Spark session via `spark` (already available in Databricks — do not create a new one)

### Section 2 — Extract
- Read from source with an explicit `StructType` schema — never use `inferSchema=True` in production
- File sources: use Auto Loader (`cloudFiles`) with `cloudFiles.schemaLocation` for schema tracking
- JDBC sources: use `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` for parallel reads
- Add ingestion metadata: `_ingested_at = F.current_timestamp()`, `_source_file = F.input_file_name()` (files only)

### Section 3 — Transform
- Cast all columns to correct types using `F.col(...).cast(...)` with null handling (`F.coalesce`, `F.when`)
- Apply business rules and filters
- Deduplication: `dropDuplicates(["key"])` or window function `ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC)`
- Handle nulls explicitly — never silently drop rows without a comment explaining why

### Section 4 — Load
- **Full refresh**: `.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(...)`
- **Append**: `.write.format("delta").mode("append").saveAsTable(...)`
- **Upsert**: `DeltaTable.forName(spark, target).alias("t").merge(df.alias("s"), "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`
- After write: `spark.sql(f"OPTIMIZE {target} ZORDER BY ({zorder_col})")` on high-cardinality filter columns

### Section 5 — Validation & Logging
- Row counts before and after the load
- Assert target count > 0; raise `ValueError` with a clear message if not
- Write a row to a control table: `pipeline_name`, `run_id`, `rows_written`, `status`, `run_at`

## Step 3 — Also provide

- A separate `CREATE TABLE IF NOT EXISTS` DDL cell for the target Unity Catalog table with a `COMMENT` on each column
- A short **"How to run"** note at the end covering: default widget values, how to override parameters in a Databricks Job, and how to trigger a backfill

## Quality bars

- **Idempotent**: re-running the pipeline produces the same result (no duplicates, no double-counts)
- **Schema-safe**: explicit types everywhere, no silent coercions
- **Parallelism-aware**: no `collect()` on large DataFrames, no Python UDFs where a native Spark SQL function exists
- **Unity Catalog compatible**: three-part names (`catalog.schema.table`) throughout; no `spark.catalog.setCurrentDatabase`
