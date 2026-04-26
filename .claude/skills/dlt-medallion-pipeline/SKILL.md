---
name: dlt-medallion-pipeline
description: Build a Delta Live Tables (DLT) pipeline following the Bronze → Silver → Gold medallion architecture on Databricks. Use when the user asks to create a DLT pipeline, medallion architecture, or a multi-hop data pipeline using Delta Live Tables.
---

# DLT Medallion Pipeline

Generate a complete Delta Live Tables pipeline following the Bronze → Silver → Gold medallion architecture based on the user's requirement.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Data source: file path (S3/ADLS), Kafka topic, JDBC, or existing Delta table
- Domain and entity names (e.g., "sales orders from Oracle ERP")
- Target Unity Catalog: catalog name and schema prefix
- Key business rules for Silver cleansing
- Gold aggregations or business metrics needed

## Step 2 — Generate three DLT layers in a single Python file

Separate sections with clear headers: `# ── BRONZE ──`, `# ── SILVER ──`, `# ── GOLD ──`

### Bronze — Raw ingestion (append-only, preserve source fidelity)
- Use `@dlt.table` with `comment`, `table_properties`, and `schema` declared explicitly
- File sources: `spark.readStream.format("cloudFiles")` with `cloudFiles.format` and `cloudFiles.schemaLocation`
- Kafka: `readStream.format("kafka")` with value cast to string; preserve raw bytes
- Add metadata columns: `_ingested_at = current_timestamp()`, `_source_file = input_file_name()`
- Table properties: `"quality": "bronze"`, `"pipelines.autoOptimize.zOrderCols": "<key_col>"`
- No transformations — raw is raw; Bronze is an immutable landing zone

### Silver — Cleansed, typed, deduplicated
- Use `@dlt.table` reading from `dlt.read_stream("bronze_table")` (streaming) or `dlt.read("bronze_table")` (batch)
- Apply `@dlt.expect_or_drop("valid_key", "id IS NOT NULL")` for hard constraints (row dropped on violation)
- Apply `@dlt.expect("valid_amount", "amount >= 0")` for soft constraints (warn only, row kept)
- Cast all columns to correct types; remove raw metadata columns not needed downstream
- Deduplication: `dropDuplicates(["business_key"])` or `dlt.apply_changes()` for CDC sources
- CDC sources: use `dlt.apply_changes()` with `sequence_by`, `keys`, `apply_as_deletes`, and `stored_as_scd_type`
- Table property: `"quality": "silver"`

### Gold — Aggregated, business-ready (batch)
- Use `@dlt.table` reading from `dlt.read("silver_table")`
- One function per business metric or reporting entity
- Use Spark SQL expressions (`.groupBy().agg()`) — no Python UDFs in Gold
- Use `@dlt.view` for intermediate joins that do not need to be materialized as tables
- Table property: `"quality": "gold"`

## Step 3 — Also generate a pipeline_config.json

```json
{
  "name": "<pipeline_name>",
  "target": "<catalog>.<schema>",
  "libraries": [{"notebook": {"path": "<notebook_path>"}}],
  "clusters": [{"label": "default", "autoscale": {"min_workers": 1, "max_workers": 4}}],
  "continuous": false,
  "development": true,
  "channel": "CURRENT"
}
```

## Step 4 — Output summary table

End with a markdown table listing every DLT table and view generated:

| Name | Layer | Type | Expectations |
|---|---|---|---|
| bronze_orders | Bronze | Streaming | 0 |
| silver_orders | Silver | Streaming | 3 |
| gold_order_summary | Gold | Batch | 0 |

Note any CDC assumptions, schema evolution behaviour, or SCD Type 2 columns added.

## Quality bars

- Every `@dlt.table` has a `comment` and at least one `@dlt.expect*` constraint (Bronze excluded)
- No `spark.sql()` calls inside DLT functions — use the DLT reader API (`dlt.read`, `dlt.read_stream`) only
- Partition and Z-ORDER recommendations in table properties
- Unity Catalog three-part naming (`catalog.schema.table`) throughout
