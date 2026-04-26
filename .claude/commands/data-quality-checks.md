Generate comprehensive data quality checks for the following dataset or pipeline stage on Databricks:

$ARGUMENTS

---

**Step 1 — Gather context (infer from arguments or ask once)**
- Table name and layer (Bronze, Silver, Gold, or source landing)
- Schema / column list (or ask user to paste `DESCRIBE TABLE` output)
- Primary / business keys
- Known business rules (e.g., "amount must be positive", "status in ('OPEN','CLOSED')")
- Enforcement mode: fail-fast, quarantine bad rows, or warn-only

---

**Step 2 — Generate DLT expectations (if inside a DLT pipeline)**

For each rule, choose the right decorator:

| Severity | DLT decorator | Behaviour |
|---|---|---|
| Critical (key integrity) | `@dlt.expect_or_fail` | Pipeline fails |
| High (drop bad rows) | `@dlt.expect_or_drop` | Row dropped, metric tracked |
| Warning (monitor only) | `@dlt.expect` | Row kept, violation logged |

Generate expectations covering:

```python
# Null / completeness checks
@dlt.expect_or_fail("non_null_primary_key", "id IS NOT NULL")
@dlt.expect_or_drop("non_null_required_fields", "customer_id IS NOT NULL AND order_date IS NOT NULL")

# Referential integrity (join-based check via a view)
@dlt.expect_or_drop("valid_customer", "customer_id IN (SELECT id FROM LIVE.dim_customer)")

# Range / domain checks
@dlt.expect_or_drop("positive_amount", "amount >= 0")
@dlt.expect("reasonable_amount", "amount < 1000000")  # warn only

# Format / pattern checks
@dlt.expect_or_drop("valid_email", "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")
@dlt.expect_or_drop("valid_date", "order_date >= '2000-01-01' AND order_date <= current_date()")

# Uniqueness (using a dedup view)
# Note: DLT has no native unique constraint — show the dropDuplicates approach

# Freshness
@dlt.expect("data_is_fresh", "updated_at >= current_timestamp() - INTERVAL 25 HOURS")
```

---

**Step 3 — Generate standalone PySpark checks (if outside DLT)**

Produce a reusable `DataQualityChecker` class:

```python
class DataQualityChecker:
    def __init__(self, df, table_name, run_id):
        ...

    def check_nulls(self, columns: list[str], fail_on_error=True) -> "DataQualityChecker": ...
    def check_uniqueness(self, key_columns: list[str]) -> "DataQualityChecker": ...
    def check_range(self, column, min_val=None, max_val=None) -> "DataQualityChecker": ...
    def check_set_membership(self, column, valid_values: list) -> "DataQualityChecker": ...
    def check_referential_integrity(self, column, ref_df, ref_column) -> "DataQualityChecker": ...
    def check_regex(self, column, pattern) -> "DataQualityChecker": ...
    def check_row_count(self, min_rows=1) -> "DataQualityChecker": ...
    def check_freshness(self, ts_column, max_age_hours=25) -> "DataQualityChecker": ...
    def quarantine(self, bad_rows_table: str) -> "DataQualityChecker": ...
    def run(self) -> dict: ...  # returns {passed: int, failed: int, quarantined: int, results: [...]}
```

Each method returns `self` for chaining. The `run()` method:
- Logs results to a `dq_results` control table (`table_name`, `rule_name`, `passed`, `failed_count`, `run_id`, `checked_at`)
- Raises an exception if `fail_on_error=True` and any critical check fails
- Writes failing rows to a quarantine Delta table if `.quarantine()` was called

Show a usage example:

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

---

**Step 4 — Control table DDL**

```sql
CREATE TABLE IF NOT EXISTS catalog.schema.dq_results (
  run_id       STRING  COMMENT 'Job run ID',
  table_name   STRING  COMMENT 'Checked table',
  rule_name    STRING  COMMENT 'Name of the DQ rule',
  passed       BOOLEAN COMMENT 'Whether the check passed',
  failed_count BIGINT  COMMENT 'Number of rows failing this rule',
  checked_at   TIMESTAMP COMMENT 'When the check ran'
)
USING DELTA
COMMENT 'Data quality check results log'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
```

---

**Step 5 — Summary**

Output a markdown table of every rule generated:

| Rule | Column(s) | Type | Enforcement | Expected Pass Rate |
|---|---|---|---|---|
| non_null_primary_key | id | Null check | Fail pipeline | 100% |
| ... | | | | |

End with a recommended monitoring query to alert when pass rate drops below threshold.
