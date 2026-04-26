Design an incremental data load pattern on Databricks for the following source and requirement:

$ARGUMENTS

---

**Step 1 — Classify the source and select the right pattern**

Ask (or infer from the arguments) these properties, then map to a pattern:

| Source property | Determines |
|---|---|
| Does the source expose a CDC stream (Debezium, Kafka, DMS)? | → CDC pattern |
| Does the source have a monotonic watermark column (updated_at, sequence_id)? | → Watermark / high-water mark pattern |
| Is the source a file landing zone (S3/ADLS)? | → Auto Loader pattern |
| Is the target a slowly changing dimension? | → SCD Type 1 or SCD Type 2 |
| Is near-real-time latency required (< 5 min)? | → Structured Streaming |
| Is hourly/daily batch acceptable? | → Batch incremental |

Select **one primary pattern** and state why it fits. Note the trade-offs vs. alternatives.

---

**Step 2 — Generate the implementation**

Implement the selected pattern in full PySpark. Include all of the following:

**Pattern A — Watermark / High-Water Mark (batch)**
```python
# 1. Read last high-water mark from control table
# 2. Query source with WHERE updated_at > last_hwm AND updated_at <= current_run_ts
# 3. Transform
# 4. Upsert to Delta target via MERGE INTO
# 5. Write new high-water mark to control table (only on success)
```
Control table DDL and the atomic HWM update pattern (update only after successful write).

**Pattern B — Auto Loader (file-based streaming)**
```python
spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "<format>")
  .option("cloudFiles.schemaLocation", "<checkpoint_path>/schema")
  .option("cloudFiles.inferColumnTypes", "true")
  .load("<source_path>")
```
Include: schema hints, `cloudFiles.schemaEvolutionMode`, checkpoint path, `trigger(availableNow=True)` for scheduled batch mode vs. continuous streaming.

**Pattern C — CDC via `APPLY CHANGES INTO` (DLT)**
```python
dlt.apply_changes(
  target = "silver_table",
  source = "bronze_cdc_stream",
  keys = ["id"],
  sequence_by = col("op_timestamp"),
  apply_as_deletes = expr("op_type = 'D'"),
  stored_as_scd_type = 1  # or 2
)
```
Include SCD Type 2 variant with `stored_as_scd_type=2` and the extra columns it adds (`__START_AT`, `__END_AT`).

**Pattern D — SCD Type 2 (non-DLT, batch)**
Full `MERGE INTO` statement that:
- Updates `end_date` and `is_current = false` on changed records
- Inserts new version with `start_date = current_date`, `is_current = true`
- Does nothing on unchanged records (using hash comparison)

---

**Step 3 — Idempotency and failure safety**

Show how the pattern handles:
- **Re-runs**: what happens if the job runs twice for the same window — no duplicates, no data loss
- **Partial failure**: how the control table / checkpoint prevents corrupt state
- **Late-arriving data**: whether the pattern handles it and how (watermark tolerance, reprocessing window)

---

**Step 4 — Scheduling recommendation**

Suggest a Databricks Workflow trigger:
```json
{
  "schedule": {
    "quartz_cron_expression": "<cron>",
    "timezone_id": "UTC",
    "pause_status": "UNPAUSED"
  }
}
```
Explain the cadence choice and whether a continuous DLT pipeline or triggered `availableNow` is better for the stated latency requirement.

---

**Step 5 — Output summary**

End with a one-page design summary:
- Pattern chosen and rationale
- Source → Bronze → Silver flow diagram (ASCII)
- Key columns added (`_ingested_at`, `_source_file`, `is_current`, etc.)
- Failure recovery steps in plain language
