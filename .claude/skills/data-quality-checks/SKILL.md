---
name: data-quality-checks
description: Generate data quality checks and validation rules for a Databricks dataset or pipeline stage. Use when the user asks to add data quality checks, validation, DQ rules, expectations, constraints, or data testing to a Delta table, DLT pipeline, or PySpark job on Databricks.
---

# Data Quality Checks

Generate comprehensive data quality rules for a dataset or pipeline stage on Databricks.

## Step 1 — Gather context

Infer from the user's description or ask once if missing:
- Table name and medallion layer (Bronze, Silver, Gold, or source landing)
- Schema / column list (ask the user to paste `DESCRIBE TABLE <table>` output if unknown)
- Primary / business key columns
- Known business rules (e.g., "amount must be positive", "status must be in ('OPEN','CLOSED')")
- Enforcement mode: fail-fast (block the pipeline), quarantine (isolate bad rows), or warn-only

## Step 2 — DLT expectations (for pipelines using Delta Live Tables)

For each rule, choose the decorator that matches the desired enforcement:

| Severity | Decorator | Behaviour |
|---|---|---|
| Critical — key integrity | `@dlt.expect_or_fail` | Stops the entire pipeline |
| High — invalid rows must be removed | `@dlt.expect_or_drop` | Drops the row, tracks the violation count |
| Warning — monitor but keep the row | `@dlt.expect` | Keeps the row, logs a warning metric |

Generate expectations covering every category below. Populate column names and thresholds from the user's schema:

```python
# Null / completeness
@dlt.expect_or_fail("non_null_primary_key", "id IS NOT NULL")
@dlt.expect_or_drop("non_null_required_fields", "customer_id IS NOT NULL AND order_date IS NOT NULL")

# Referential integrity (via a live view join)
@dlt.expect_or_drop("valid_customer_id", "customer_id IN (SELECT id FROM LIVE.dim_customer)")

# Range / domain
@dlt.expect_or_drop("positive_amount", "amount >= 0")
@dlt.expect("reasonable_amount", "amount < 1000000")

# Format / pattern
@dlt.expect_or_drop("valid_email", "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")
@dlt.expect_or_drop("valid_date_range", "order_date >= '2000-01-01' AND order_date <= current_date()")

# Freshness
@dlt.expect("data_is_fresh", "updated_at >= current_timestamp() - INTERVAL 25 HOURS")
```

Note: DLT has no native unique-key constraint — show a deduplication step using `dropDuplicates` before the silver table write, combined with a warn-level expectation to track duplicates.

## Step 3 — Standalone PySpark checks (for jobs outside DLT)

Produce a complete, runnable `DataQualityChecker` class:

```python
class DataQualityChecker:
    def __init__(self, df: DataFrame, table_name: str, run_id: str): ...

    def check_nulls(self, columns: list[str], fail_on_error: bool = True) -> "DataQualityChecker": ...
    def check_uniqueness(self, key_columns: list[str]) -> "DataQualityChecker": ...
    def check_range(self, column: str, min_val=None, max_val=None) -> "DataQualityChecker": ...
    def check_set_membership(self, column: str, valid_values: list) -> "DataQualityChecker": ...
    def check_referential_integrity(self, column: str, ref_df: DataFrame, ref_column: str) -> "DataQualityChecker": ...
    def check_regex(self, column: str, pattern: str) -> "DataQualityChecker": ...
    def check_row_count(self, min_rows: int = 1) -> "DataQualityChecker": ...
    def check_freshness(self, ts_column: str, max_age_hours: int = 25) -> "DataQualityChecker": ...
    def quarantine(self, bad_rows_table: str) -> "DataQualityChecker": ...
    def run(self) -> dict: ...
```

- Each method returns `self` for chaining
- `run()` executes all registered checks, writes results to the `dq_results` control table, writes failing rows to the quarantine table if `.quarantine()` was called, and raises a `ValueError` if any critical check failed and `fail_on_error=True`

Show a complete usage example:

```python
results = (
    DataQualityChecker(df, "silver.orders", run_id=dbutils.widgets.get("run_id"))
    .check_nulls(["order_id", "customer_id"], fail_on_error=True)
    .check_uniqueness(["order_id"])
    .check_range("amount", min_val=0)
    .check_set_membership("status", ["OPEN", "CLOSED", "CANCELLED"])
    .check_freshness("updated_at", max_age_hours=25)
    .quarantine("catalog.schema.orders_quarantine")
    .run()
)
```

## Step 4 — Control table DDL

```sql
CREATE TABLE IF NOT EXISTS catalog.schema.dq_results (
  run_id       STRING    COMMENT 'Job or notebook run ID',
  table_name   STRING    COMMENT 'Table that was checked',
  rule_name    STRING    COMMENT 'Name of the DQ rule',
  passed       BOOLEAN   COMMENT 'True if the check passed',
  failed_count BIGINT    COMMENT 'Number of rows that failed this rule',
  checked_at   TIMESTAMP COMMENT 'Timestamp when the check ran'
)
USING DELTA
COMMENT 'Data quality check results log'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
```

## Step 5 — Summary and monitoring

Output a table of every rule generated:

| Rule | Column(s) | Check Type | Enforcement | Target Pass Rate |
|---|---|---|---|---|
| non_null_primary_key | id | Null check | Fail pipeline | 100% |
| ... | | | | |

End with a monitoring query the team can schedule to alert when pass rate drops below a threshold:

```sql
SELECT table_name, rule_name, COUNT(*) AS runs,
       SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*) AS pass_rate
FROM catalog.schema.dq_results
WHERE checked_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY 1, 2
HAVING pass_rate < 0.95
ORDER BY pass_rate ASC;
```
