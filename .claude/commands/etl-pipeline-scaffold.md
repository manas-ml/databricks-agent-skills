Generate a production-ready PySpark ETL pipeline on Databricks for the following requirement:

$ARGUMENTS

---

Follow these steps:

**1. Clarify (if arguments are vague, ask once before generating):**
- Source system and format (JDBC, S3/ADLS, Kafka, files — CSV/JSON/Parquet/Avro)
- Target Delta table (catalog.schema.table in Unity Catalog)
- Load mode: full refresh, append, or upsert (MERGE)
- Primary/business keys (for upsert)
- Any known transformations or business rules

**2. Generate the ETL notebook with these sections:**

```
# Section 1 — Imports & Config
```
- Standard PySpark imports
- `dbutils.widgets` for runtime parameters (source path, target table, load date)
- Spark session access via `spark` (already available in Databricks)

```
# Section 2 — Extract
```
- Read from source with an explicit schema (`StructType`) — never rely on schema inference in production
- For file sources: use Auto Loader (`cloudFiles`) with `cloudFiles.schemaLocation`
- For JDBC: use `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` for parallel reads
- Add ingestion metadata columns: `_ingested_at` (current_timestamp), `_source_file` (if applicable)

```
# Section 3 — Transform
```
- Cast columns to correct types with safe casting (`try_cast` or explicit `.cast()` with null handling)
- Apply business rules and filters
- Deduplication using `dropDuplicates` or window functions (`ROW_NUMBER OVER PARTITION BY key ORDER BY ts DESC`)
- Handle nulls explicitly — never silently drop rows

```
# Section 4 — Load
```
- **Full refresh**: `df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(...)`
- **Append**: `df.write.format("delta").mode("append").saveAsTable(...)`
- **Upsert**: Generate a `MERGE INTO` using `DeltaTable.forName()` with matched update and not-matched insert clauses
- After write: run `OPTIMIZE` and `ZORDER BY` on high-cardinality filter columns

```
# Section 5 — Validation & Logging
```
- Row count before and after (log with `print` or MLflow if in a job context)
- Assert target row count > 0
- Log load metadata to a control table (`pipeline_name`, `run_id`, `rows_written`, `status`, `run_at`)

**3. Output format:**
- Python code blocks for each section, ready to paste into a Databricks notebook
- A separate cell for Unity Catalog `CREATE TABLE IF NOT EXISTS` DDL with `COMMENT` on each column
- Inline comments only where logic is non-obvious (not on every line)
- A short "How to run" note at the end covering widget defaults and job parameter overrides

**Quality bars to meet:**
- Idempotent: re-running produces the same result
- Schema-safe: explicit types, no silent coercions
- Parallelism-aware: no unnecessary `collect()`, no Python UDFs where a Spark SQL function exists
- Unity Catalog compatible: three-part names, no `spark.catalog.setCurrentDatabase`
