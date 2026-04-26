---
name: pipeline-observability
description: Instrument a Databricks ETL pipeline with observability — run metrics, cost attribution, SLA alerting, and monitoring dashboards using Databricks system tables. Use when the user wants to monitor pipeline health, track SLA breaches, alert on job failures or slowness, understand compute cost, or build a DataOps monitoring dashboard on Databricks.
---

# Pipeline Observability

Generate a full observability stack for a Databricks ETL pipeline: metrics logging, cost tracking, SLA alerts, and a monitoring dashboard.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Pipeline / job name and the target Delta tables it writes to
- SLA threshold (e.g., "job must complete in under 30 minutes")
- Alert channel: email, Slack webhook, or PagerDuty
- Whether cost attribution per job is needed (requires `system.billing.usage` access)
- Databricks Runtime version (system tables require DBR 12.2+, Unity Catalog enabled)

## Step 2 — PipelineMetricsLogger class

Add this to the ETL notebook as the final cell. It captures runtime context automatically via the Databricks notebook context API.

```python
import time
from dataclasses import dataclass, field
from pyspark.sql import functions as F, Row

@dataclass
class PipelineMetricsLogger:
    pipeline_name: str
    target_table: str
    metrics_table: str = "catalog.observability.pipeline_run_metrics"
    _start_time: float = field(default_factory=time.time, init=False)
    _rows_read: int = 0
    _rows_written: int = 0

    def record_read(self, df) -> "PipelineMetricsLogger":
        self._rows_read = df.count()
        return self

    def record_write(self, df) -> "PipelineMetricsLogger":
        self._rows_written = df.count()
        return self

    def flush(self, status: str = "SUCCESS", error_msg: str = None):
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        run_id   = ctx.currentRunId().get() if ctx.currentRunId().isDefined() else "local"
        cluster_id = ctx.clusterId().get()   if ctx.clusterId().isDefined()   else "local"
        duration_s = round(time.time() - self._start_time, 2)

        row = Row(
            pipeline_name = self.pipeline_name,
            target_table  = self.target_table,
            run_id        = str(run_id),
            cluster_id    = cluster_id,
            status        = status,
            duration_seconds = duration_s,
            rows_read     = self._rows_read,
            rows_written  = self._rows_written,
            error_message = error_msg,
            logged_at     = F.current_timestamp()
        )
        spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(self.metrics_table)

# Usage pattern — wrap the entire pipeline:
logger = PipelineMetricsLogger("orders_etl", "catalog.silver.orders")
try:
    source_df = spark.read...
    logger.record_read(source_df)
    result_df = transform(source_df)
    result_df.write...
    logger.record_write(result_df)
    logger.flush("SUCCESS")
except Exception as e:
    logger.flush("FAILED", error_msg=str(e))
    raise
```

## Step 3 — Control table DDL

```sql
CREATE TABLE IF NOT EXISTS catalog.observability.pipeline_run_metrics (
  pipeline_name    STRING    NOT NULL COMMENT 'ETL job or pipeline name',
  target_table     STRING    COMMENT 'Primary Delta table written to',
  run_id           STRING    COMMENT 'Databricks job run ID',
  cluster_id       STRING    COMMENT 'Cluster that executed the run',
  status           STRING    NOT NULL COMMENT 'SUCCESS | FAILED | PARTIAL',
  duration_seconds DOUBLE    COMMENT 'Wall-clock run time in seconds',
  rows_read        BIGINT    COMMENT 'Rows read from source',
  rows_written     BIGINT    COMMENT 'Rows written to target',
  error_message    STRING    COMMENT 'Exception message on failure; NULL on success',
  logged_at        TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'ETL pipeline run metrics — one row per pipeline execution'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed'       = 'true'
);

CREATE TABLE IF NOT EXISTS catalog.observability.pipeline_alerts (
  alert_id      STRING    DEFAULT uuid() COMMENT 'Unique alert identifier',
  pipeline_name STRING    NOT NULL,
  alert_type    STRING    COMMENT 'SLA_BREACH | COST_SPIKE | ZERO_ROWS | FAILURE',
  message       STRING,
  run_id        STRING,
  fired_at      TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  acknowledged  BOOLEAN   DEFAULT false
)
USING DELTA;
```

## Step 4 — DBSQL alert queries (schedule these in Databricks SQL Alerts)

### SLA breach — job ran longer than threshold

```sql
-- Alert: fire when any pipeline exceeds its SLA in the last run
SELECT pipeline_name, duration_seconds, run_id, logged_at
FROM catalog.observability.pipeline_run_metrics
WHERE logged_at >= current_timestamp() - INTERVAL 2 HOURS
  AND status = 'SUCCESS'
  AND duration_seconds > 1800  -- 30-minute SLA; adjust per pipeline
QUALIFY ROW_NUMBER() OVER (PARTITION BY pipeline_name ORDER BY logged_at DESC) = 1
```

### Zero rows written — silent failure

```sql
SELECT pipeline_name, rows_written, run_id, logged_at
FROM catalog.observability.pipeline_run_metrics
WHERE logged_at >= current_timestamp() - INTERVAL 2 HOURS
  AND status = 'SUCCESS'
  AND rows_written = 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY pipeline_name ORDER BY logged_at DESC) = 1
```

### Cost spike — DBU usage above budget (uses system tables)

```sql
SELECT j.job_id, j.run_name, SUM(u.usage_quantity) AS total_dbus
FROM system.billing.usage u
JOIN system.lakeflow.job_run_timeline j
  ON u.usage_metadata.job_id = j.job_id
WHERE u.usage_date = current_date()
  AND u.sku_name LIKE '%JOBS%'
GROUP BY 1, 2
HAVING total_dbus > 50  -- adjust DBU budget threshold
```

### Pipeline success rate trend

```sql
SELECT
  date_trunc('day', logged_at) AS run_date,
  pipeline_name,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
  ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate_pct,
  ROUND(PERCENTILE(duration_seconds, 0.5), 0) AS p50_duration_s,
  ROUND(PERCENTILE(duration_seconds, 0.95), 0) AS p95_duration_s
FROM catalog.observability.pipeline_run_metrics
WHERE logged_at >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY 1, 2
ORDER BY 1 DESC, 2
```

## Step 5 — Post-job alerting task in Databricks Workflow

Add a second task to the existing job that fires after the main ETL task:

```yaml
# Add to databricks.yml under tasks:
- task_key: observability_check
  depends_on:
    - task_key: main
  run_if: ALL_DONE   # runs even if main task failed
  notebook_task:
    notebook_path: ./notebooks/observability_check
    base_parameters:
      pipeline_name: "orders_etl"
      sla_seconds: "1800"
      slack_webhook_secret: "slack-webhook-url"
  job_cluster_key: etl_cluster
```

Alerting notebook body:

```python
import requests

pipeline_name = dbutils.widgets.get("pipeline_name")
sla_seconds   = int(dbutils.widgets.get("sla_seconds"))
webhook_url   = dbutils.secrets.get("scope", dbutils.widgets.get("slack_webhook_secret"))

last_run = spark.sql(f"""
  SELECT status, duration_seconds, rows_written, run_id
  FROM catalog.observability.pipeline_run_metrics
  WHERE pipeline_name = '{pipeline_name}'
  ORDER BY logged_at DESC LIMIT 1
""").first()

alerts = []
if last_run["status"] == "FAILED":
    alerts.append(f":red_circle: *{pipeline_name}* FAILED — run_id `{last_run['run_id']}`")
if last_run["duration_seconds"] > sla_seconds:
    alerts.append(f":warning: *{pipeline_name}* SLA breach — {last_run['duration_seconds']}s > {sla_seconds}s")
if last_run["rows_written"] == 0 and last_run["status"] == "SUCCESS":
    alerts.append(f":ghost: *{pipeline_name}* wrote 0 rows — possible silent failure")

if alerts:
    requests.post(webhook_url, json={"text": "\n".join(alerts)})
    spark.sql(f"""
      INSERT INTO catalog.observability.pipeline_alerts (pipeline_name, alert_type, message, run_id)
      VALUES ('{pipeline_name}', 'COMPOSITE', '{"; ".join(alerts)}', '{last_run["run_id"]}')
    """)
```

## Step 6 — Lakeview dashboard panels (DBSQL)

Generate queries for these 4 dashboard panels:
1. **Daily success rate by pipeline** — bar chart, last 30 days
2. **p50 / p95 run duration trend** — line chart per pipeline
3. **Rows written per run** — area chart to spot zero-row days
4. **Top 5 most expensive jobs by DBU** — table from `system.billing.usage`

## Quality bars

- `PipelineMetricsLogger.flush()` is called in a `finally` block — metrics are always written even on failure
- Secrets accessed via `dbutils.secrets.get()` — no tokens or webhook URLs in code
- Alert queries use `QUALIFY ROW_NUMBER()` to evaluate only the most recent run, not historical averages
- All observability tables live in a dedicated `observability` schema, separate from business data schemas
