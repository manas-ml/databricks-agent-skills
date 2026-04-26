---
name: source-target-reconciliation
description: Generate a source-to-target reconciliation report and automated checks for a Databricks ETL pipeline. Use when the user needs to reconcile row counts, validate totals or aggregations, detect missing or extra records, compare two Delta tables, or produce a formal data completeness and correctness proof between a source system and a Delta Lake target.
---

# Source-to-Target Reconciliation

Generate a `ReconciliationRunner` class, post-ETL Workflow task, control tables, and DBSQL trending reports to formally prove ETL correctness.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Source system type: JDBC (Oracle/SQL Server/PostgreSQL), another Delta table, or a file-based count
- Target Delta table (`catalog.schema.table`)
- Business key column(s) used to match source and target records
- Numeric columns to reconcile as aggregations (SUM, COUNT, AVG)
- Tolerance threshold for numeric comparisons (e.g., 0.01% variance allowed for floating-point)
- Enforcement mode: block downstream load on failure, or report-only

## Step 2 — ReconciliationRunner class

```python
from dataclasses import dataclass, field
from pyspark.sql import DataFrame, functions as F
from typing import Optional

@dataclass
class ReconciliationRunner:
    source_df: DataFrame
    target_table: str
    key_cols: list[str]
    recon_table: str = "catalog.observability.recon_results"
    breaks_table: str = "catalog.observability.recon_breaks"
    pipeline_name: str = "unknown"
    run_id: str = "unknown"
    _results: list = field(default_factory=list, init=False)
    _has_breaks: bool = field(default=False, init=False)

    def _target(self) -> DataFrame:
        return spark.table(self.target_table)

    def row_count_match(self, tolerance_pct: float = 0.0) -> "ReconciliationRunner":
        src_count = self.source_df.count()
        tgt_count = self._target().count()
        variance  = abs(src_count - tgt_count) / max(src_count, 1) * 100
        passed    = variance <= tolerance_pct
        self._results.append({
            "check": "row_count_match", "passed": passed,
            "source_value": str(src_count), "target_value": str(tgt_count),
            "variance_pct": round(variance, 4), "tolerance_pct": tolerance_pct
        })
        if not passed:
            self._has_breaks = True
        return self

    def aggregate_match(self, column: str, agg_func: str = "SUM",
                        tolerance_pct: float = 0.01) -> "ReconciliationRunner":
        agg = getattr(F, agg_func.lower())
        src_val = self.source_df.agg(agg(column)).first()[0] or 0
        tgt_val = self._target().agg(agg(column)).first()[0] or 0
        variance = abs(src_val - tgt_val) / max(abs(src_val), 1) * 100
        passed   = variance <= tolerance_pct
        self._results.append({
            "check": f"{agg_func}({column})", "passed": passed,
            "source_value": str(round(src_val, 6)), "target_value": str(round(tgt_val, 6)),
            "variance_pct": round(variance, 6), "tolerance_pct": tolerance_pct
        })
        if not passed:
            self._has_breaks = True
        return self

    def key_completeness(self) -> "ReconciliationRunner":
        src_keys = self.source_df.select(*self.key_cols).distinct()
        tgt_keys = self._target().select(*self.key_cols).distinct()
        missing_in_target = src_keys.subtract(tgt_keys)
        extra_in_target   = tgt_keys.subtract(src_keys)
        missing_count = missing_in_target.count()
        extra_count   = extra_in_target.count()
        passed = missing_count == 0 and extra_count == 0
        self._results.append({
            "check": "key_completeness", "passed": passed,
            "source_value": f"missing_in_target={missing_count}",
            "target_value": f"extra_in_target={extra_count}",
            "variance_pct": None, "tolerance_pct": 0
        })
        if not passed:
            self._has_breaks = True
            # Write break records for investigation
            (missing_in_target.withColumn("break_type", F.lit("MISSING_IN_TARGET"))
             .union(extra_in_target.withColumn("break_type", F.lit("EXTRA_IN_TARGET")))
             .withColumn("pipeline_name", F.lit(self.pipeline_name))
             .withColumn("run_id", F.lit(self.run_id))
             .withColumn("detected_at", F.current_timestamp())
             .write.format("delta").mode("append").saveAsTable(self.breaks_table))
        return self

    def hash_diff_sample(self, n_rows: int = 100) -> "ReconciliationRunner":
        common_cols = [c for c in self.source_df.columns if c in self._target().columns]
        hash_expr   = F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in sorted(common_cols)]))
        src_sample  = self.source_df.withColumn("_row_hash", hash_expr).select(*self.key_cols, "_row_hash").limit(n_rows)
        tgt_sample  = self._target().withColumn("_row_hash", hash_expr).select(*self.key_cols, "_row_hash").limit(n_rows)
        diff_count  = src_sample.join(tgt_sample.withColumnRenamed("_row_hash","_tgt_hash"), self.key_cols) \
                                .filter("_row_hash <> _tgt_hash").count()
        passed = diff_count == 0
        self._results.append({
            "check": f"hash_diff_sample(n={n_rows})", "passed": passed,
            "source_value": f"mismatched_rows={diff_count}", "target_value": f"sample_size={n_rows}",
            "variance_pct": None, "tolerance_pct": 0
        })
        if not passed:
            self._has_breaks = True
        return self

    def run(self, fail_on_break: bool = True) -> dict:
        from pyspark.sql import Row
        rows = [Row(
            pipeline_name=self.pipeline_name, run_id=self.run_id,
            check_name=r["check"], passed=r["passed"],
            source_value=r["source_value"], target_value=r["target_value"],
            variance_pct=r.get("variance_pct"), tolerance_pct=r.get("tolerance_pct"),
            checked_at=F.current_timestamp()
        ) for r in self._results]
        spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable(self.recon_table)

        passed = sum(1 for r in self._results if r["passed"])
        failed = len(self._results) - passed
        print(f"Reconciliation: {passed} passed, {failed} failed")
        for r in self._results:
            icon = "✓" if r["passed"] else "✗"
            print(f"  {icon} {r['check']}: source={r['source_value']} target={r['target_value']}")

        if fail_on_break and self._has_breaks:
            raise ValueError(f"Reconciliation failed: {failed} check(s) did not pass. "
                             f"See {self.recon_table} for details.")
        return {"passed": passed, "failed": failed, "has_breaks": self._has_breaks}
```

### Usage example

```python
results = (
    ReconciliationRunner(
        source_df=source_df,
        target_table="catalog.silver.orders",
        key_cols=["order_id"],
        pipeline_name="orders_etl",
        run_id=dbutils.widgets.get("run_id")
    )
    .row_count_match(tolerance_pct=0.0)
    .aggregate_match("amount", agg_func="SUM", tolerance_pct=0.01)
    .aggregate_match("amount", agg_func="COUNT")
    .key_completeness()
    .hash_diff_sample(n_rows=500)
    .run(fail_on_break=True)
)
```

## Step 3 — Control table DDL

```sql
CREATE TABLE IF NOT EXISTS catalog.observability.recon_results (
  pipeline_name  STRING    NOT NULL,
  run_id         STRING,
  check_name     STRING    NOT NULL COMMENT 'row_count_match | SUM(col) | key_completeness | hash_diff_sample',
  passed         BOOLEAN   NOT NULL,
  source_value   STRING    COMMENT 'Computed value from source system',
  target_value   STRING    COMMENT 'Computed value from target Delta table',
  variance_pct   DOUBLE    COMMENT 'Percentage deviation; NULL for key/hash checks',
  tolerance_pct  DOUBLE    COMMENT 'Allowed tolerance threshold',
  checked_at     TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Source-to-target reconciliation results — one row per check per run'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

CREATE TABLE IF NOT EXISTS catalog.observability.recon_breaks (
  pipeline_name  STRING    NOT NULL,
  run_id         STRING,
  break_type     STRING    COMMENT 'MISSING_IN_TARGET | EXTRA_IN_TARGET | HASH_MISMATCH',
  detected_at    TIMESTAMP NOT NULL DEFAULT current_timestamp()
  -- key columns appended dynamically by ReconciliationRunner.key_completeness()
)
USING DELTA
COMMENT 'Individual records that failed reconciliation key or hash checks';
```

## Step 4 — Post-ETL recon Workflow task

```yaml
# Add to databricks.yml after the main ETL task:
- task_key: reconciliation
  depends_on:
    - task_key: main
  run_if: ALL_SUCCESS   # only reconcile if ETL succeeded
  notebook_task:
    notebook_path: ./notebooks/reconciliation
    base_parameters:
      source_jdbc_url: "{{secrets/scope/jdbc-url}}"
      source_query: "SELECT * FROM orders WHERE updated_at >= '{{load_date}}'"
      target_table: "catalog.silver.orders"
      key_cols: "order_id"
      fail_on_break: "true"
  job_cluster_key: etl_cluster
```

## Step 5 — DBSQL trending report

```sql
-- Break rate trend by pipeline — schedule as a DBSQL alert
SELECT
  date_trunc('day', checked_at) AS check_date,
  pipeline_name,
  COUNT(*) AS total_checks,
  SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failed_checks,
  ROUND(100.0 * SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) / COUNT(*), 1) AS break_rate_pct
FROM catalog.observability.recon_results
WHERE checked_at >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY 1, 2
HAVING break_rate_pct > 0
ORDER BY check_date DESC, break_rate_pct DESC;
```

## Quality bars

- `run(fail_on_break=True)` raises before any downstream Gold load executes — prevents corrupt propagation
- Break records written to `recon_breaks` even when the run() exception is caught — always auditable
- `hash_diff_sample` uses sorted column order for the hash so column reordering in source doesn't create false positives
- JDBC source queries use the same date filter as the ETL load to compare apples to apples
