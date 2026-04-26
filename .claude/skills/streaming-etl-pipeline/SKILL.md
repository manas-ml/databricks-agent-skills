---
name: streaming-etl-pipeline
description: Build a real-time or near-real-time ETL pipeline using Databricks Structured Streaming. Use when the user asks to build a streaming pipeline, real-time ingestion, event-driven ETL, Kafka consumer, or a low-latency data pipeline on Databricks.
---

# Streaming ETL Pipeline

Generate a production-ready Structured Streaming ETL pipeline on Databricks based on the user's source and target requirements.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Source type: Kafka, Azure Event Hubs, Amazon Kinesis, Auto Loader (file-based), or Delta table (CDC)
- Target: Delta table in Unity Catalog (catalog.schema.table)
- Latency requirement: near-real-time (seconds), micro-batch (minutes), or scheduled batch (`availableNow`)
- Stateful operations needed: windowed aggregations, sessionization, deduplication, joins?
- Any transformations or enrichment (lookups, parsing, type casting)

## Step 2 — Select trigger mode

Map latency requirement to the right trigger:

| Latency target | Trigger mode | When to use |
|---|---|---|
| < 30 seconds | `trigger(processingTime="10 seconds")` | Near-real-time, continuous micro-batch |
| 1–15 minutes | `trigger(processingTime="5 minutes")` | Low-cost micro-batch |
| Scheduled batch | `trigger(availableNow=True)` | Process all backlog then stop; use with Databricks Workflow |
| True continuous | `trigger(continuous="1 second")` | Ultra-low latency; requires Continuous Processing mode |

State the chosen trigger and explain the cost/latency trade-off.

## Step 3 — Generate the streaming pipeline

### Source configuration

**Kafka / Event Hubs:**
```python
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", dbutils.secrets.get("scope", "kafka-brokers"))
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100_000)
    .option("failOnDataLoss", "false")
    .load()
)
# Deserialise value bytes → string → parse JSON schema
```

**Auto Loader (file-based):**
```python
raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "<format>")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
)
```

### Transform

- Parse and cast all fields using an explicit `StructType` — never trust inferred types on a stream
- Add metadata columns: `_ingested_at = F.current_timestamp()`, `_kafka_offset` / `_source_file` where applicable
- Deduplication: `dropDuplicates(["business_key"])` with watermark to bound state size
- Watermark for late data: `.withWatermark("event_time", "10 minutes")`
- Windowed aggregations: `groupBy(F.window("event_time", "5 minutes"), "key").agg(...)`

### Sink — `foreachBatch` upsert to Delta (recommended for upsert semantics)

```python
def upsert_to_delta(batch_df, batch_id):
    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(batch_df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

query = (
    transformed_stream.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", f"{checkpoint_base}/checkpoint")
    .trigger(availableNow=True)   # swap trigger as needed
    .start()
)
query.awaitTermination()
```

For append-only sinks (Bronze landing), use `.format("delta").outputMode("append")` directly.

### Checkpoint and restart behaviour

- Checkpoint path must be stable across job runs — store in ADLS/S3, never in DBFS `/tmp`
- If schema changes break the stream: delete `{checkpoint_base}/schema` only, not the full checkpoint
- For full restart from source: delete entire checkpoint directory (explain the offset reset risk)

## Step 4 — Stateful pattern details (generate only what applies)

**Deduplication with watermark:**
```python
deduped = (
    stream
    .withWatermark("event_time", "1 hour")
    .dropDuplicates(["event_id", "event_time"])
)
```

**Stream–static join (enrich with a dimension table):**
```python
dim = spark.table("catalog.schema.dim_customer")  # re-read each micro-batch via foreachBatch
enriched = stream_df.join(F.broadcast(dim), "customer_id", "left")
```

**Session window:**
```python
.groupBy(session_window("event_time", "30 minutes"), "user_id").agg(...)
```

## Step 5 — Monitoring and alerting

Generate a Databricks Workflow with:
- A streaming job task using the script above
- `max_retries: 3` with exponential backoff
- An email alert on failure
- A second scheduled task running this monitoring query every 15 minutes:

```sql
SELECT lag_seconds, processed_rows_per_sec
FROM (
  SELECT
    UNIX_TIMESTAMP() - MAX(UNIX_TIMESTAMP(_ingested_at)) AS lag_seconds,
    COUNT(*) / 900 AS processed_rows_per_sec
  FROM catalog.schema.target_table
  WHERE _ingested_at >= current_timestamp() - INTERVAL 15 MINUTES
)
WHERE lag_seconds > 300  -- alert if > 5 min behind
```

## Quality bars

- Checkpoint path is on persistent cloud storage, not ephemeral DBFS
- Watermark is set whenever late data or stateful operations are used
- Secrets accessed via `dbutils.secrets.get()` — no credentials in code
- Schema is explicit — no `inferSchema` or untyped byte arrays left unparsed
- `foreachBatch` used for any sink requiring upsert, exactly-once, or multi-table writes
