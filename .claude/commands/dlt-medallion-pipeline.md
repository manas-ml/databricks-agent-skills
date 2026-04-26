Generate a Delta Live Tables (DLT) pipeline following the medallion architecture for the following requirement:

$ARGUMENTS

---

Follow these steps:

**1. Clarify (if arguments are vague, ask once before generating):**
- Data source: file path (S3/ADLS), Kafka topic, JDBC, or existing Delta table
- Domain and entity names (e.g., "sales orders from Oracle ERP")
- Target Unity Catalog: catalog name and target schema prefix
- Key business rules for Silver cleansing
- Gold aggregations or business metrics needed

**2. Generate three DLT layers in a single Python file:**

```python
# Bronze — Raw ingestion (append-only, preserve source fidelity)
```
- Use `@dlt.table` with `comment`, `table_properties`, and `schema` declared explicitly
- For file sources: `spark.readStream.format("cloudFiles")` with `cloudFiles.format` and `cloudFiles.schemaLocation`
- For Kafka: `readStream.format("kafka")` with value cast to string
- Add metadata: `_ingested_at = current_timestamp()`, `_source_file = input_file_name()`
- Table property: `"quality": "bronze"`, `"pipelines.autoOptimize.zOrderCols": "<key_col>"`
- No transformations — raw is raw

```python
# Silver — Cleansed, typed, deduplicated (streaming or batch)
```
- Use `@dlt.table` reading from `dlt.read_stream("bronze_table")` or `dlt.read("bronze_table")`
- Apply `@dlt.expect_or_drop("valid_key", "id IS NOT NULL")` for hard constraints
- Apply `@dlt.expect("valid_amount", "amount >= 0")` for soft constraints (warn only)
- Cast all columns to correct types; drop raw metadata columns not needed downstream
- Deduplicate with `dropDuplicates(["business_key"])` or `APPLY CHANGES INTO` for CDC
- For CDC sources: use `dlt.apply_changes()` with `sequence_by`, `keys`, and `stored_as_scd_type`
- Table property: `"quality": "silver"`

```python
# Gold — Aggregated, business-ready (batch views)
```
- Use `@dlt.table` reading from `dlt.read("silver_table")`
- One function per business metric or reporting entity
- Use Spark SQL expressions (`.groupBy().agg()`) — no Python UDFs
- Add `@dlt.view` for intermediate joins that don't need to be materialized
- Table property: `"quality": "gold"`

**3. Also generate:**

A `pipeline_config.json` for the DLT pipeline definition:
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

**4. Output format:**
- Single Python file with all three layers, clearly separated by `# ── BRONZE ──`, `# ── SILVER ──`, `# ── GOLD ──` section headers
- The `pipeline_config.json` in a separate code block
- A table at the end listing every DLT table/view: name, layer, type (streaming/batch), quality expectation count
- Note any CDC assumptions or schema evolution considerations

**Quality bars to meet:**
- Every table has a `comment` and at least one `@dlt.expect*` constraint
- No `spark.sql()` calls inside DLT functions — use the DLT reader API only
- Partition and Z-ORDER recommendations included in table properties
- Unity Catalog three-part naming throughout
