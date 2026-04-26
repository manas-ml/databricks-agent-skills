---
name: incremental-load-design
description: Design and implement an incremental data load pattern on Databricks. Use when the user asks how to load only new or changed data, implement CDC, set up a watermark-based load, use Auto Loader, build an SCD Type 2, or avoid full table reloads on Databricks.
---

# Incremental Load Design

Design the right incremental load pattern for the user's source system and latency requirement, then generate the full implementation.

## Step 1 — Classify the source and select a pattern

Infer from the user's description (or ask once if unclear):

| Source property | Pattern |
|---|---|
| Source exposes CDC stream (Debezium, Kafka, AWS DMS) | → CDC via `APPLY CHANGES INTO` (DLT) |
| Source has a monotonic watermark column (`updated_at`, `sequence_id`) | → Watermark / high-water mark (batch) |
| Source is a file landing zone (S3/ADLS drop zone) | → Auto Loader (`cloudFiles`) |
| Target is a slowly changing dimension | → SCD Type 1 or SCD Type 2 |
| Latency requirement < 5 minutes | → Structured Streaming (continuous or `availableNow`) |
| Hourly/daily batch is acceptable | → Batch incremental with HWM or `availableNow` trigger |

State the selected pattern and explain in 2-3 sentences why it fits. Note the main trade-off vs. the next best alternative.

## Step 2 — Generate the full implementation

### Pattern A — Watermark / High-Water Mark (batch)

```python
# 1. Read last successful high-water mark from control table
# 2. Query source: WHERE updated_at > last_hwm AND updated_at <= current_run_ts
# 3. Transform
# 4. Upsert to Delta via MERGE INTO
# 5. Write new HWM to control table — only after a successful write (in a finally/except block)
```

Include:
- Control table DDL (`pipeline_name`, `last_hwm`, `updated_at`)
- Atomic HWM update: commit HWM inside a try/except so a failed run does not advance the mark
- Overlap window (e.g., subtract 10 minutes from `last_hwm`) to handle late-arriving records

### Pattern B — Auto Loader (file-based streaming)

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "<format>")
    .option("cloudFiles.schemaLocation", "<checkpoint_path>/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("<source_path>")
```

Include:
- Schema hints for known columns to avoid full inference
- `trigger(availableNow=True)` for scheduled batch mode (processes all backlog then stops)
- Checkpoint path configuration and what happens if the checkpoint is deleted
- `cloudFiles.schemaEvolutionMode` options and when to use each

### Pattern C — CDC via `APPLY CHANGES INTO` (DLT)

```python
dlt.apply_changes(
    target="silver_table",
    source="bronze_cdc_stream",
    keys=["id"],
    sequence_by=col("op_timestamp"),
    apply_as_deletes=expr("op_type = 'D'"),
    stored_as_scd_type=1
)
```

Include:
- SCD Type 2 variant: `stored_as_scd_type=2`; explain the `__START_AT` and `__END_AT` columns added
- How to handle `TRUNCATE` events from the CDC stream
- Bronze table structure for typical Debezium / DMS payloads

### Pattern D — SCD Type 2 (non-DLT, batch MERGE)

Full `MERGE INTO` that:
- Closes existing active records (`end_date = current_date`, `is_current = false`) when a column changed (detected via MD5 hash comparison)
- Inserts new versions with `start_date = current_date`, `end_date = null`, `is_current = true`
- Does nothing for unchanged records

## Step 3 — Idempotency and failure safety

For the selected pattern, explain concretely:
- **Re-runs**: what happens if the job runs twice for the same window — must produce no duplicates and no data loss
- **Partial failure**: how the checkpoint or control table prevents a corrupt intermediate state
- **Late-arriving data**: whether the pattern handles it, and if not, what the reprocessing strategy is

## Step 4 — Scheduling recommendation

Suggest a Databricks Workflow JSON trigger:

```json
{
  "schedule": {
    "quartz_cron_expression": "<cron>",
    "timezone_id": "UTC",
    "pause_status": "UNPAUSED"
  }
}
```

Explain the cadence choice. Call out whether a continuous DLT pipeline, a triggered `availableNow` stream, or a plain cron batch job is best for the stated latency.

## Step 5 — Design summary

End with:
- Pattern chosen and 2-sentence rationale
- ASCII flow diagram: `Source → [Extract] → Bronze → [Transform] → Silver`
- Key system columns added and their purpose
- Failure recovery steps in plain language (what an on-call engineer should do if the job fails mid-run)
