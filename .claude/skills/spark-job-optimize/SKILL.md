---
name: spark-job-optimize
description: Diagnose and optimize a Spark job or PySpark code for performance on Databricks. Use when the user shares PySpark code, a Spark job description, or a job that is running slowly, OOMing, skewing, or shuffling excessively and asks for optimization, performance improvement, or a review.
---

# Spark Job Optimizer

Diagnose and rewrite a Spark job or PySpark code snippet for maximum performance on Databricks.

## Step 1 — Diagnose: scan for anti-patterns

Check the provided code or job description for each issue. Report every finding as:
`[SEVERITY: HIGH|MED|LOW] <Issue> — <one-line explanation of why it hurts>`

| Anti-pattern | What to check |
|---|---|
| Driver collect | `.collect()`, `.toPandas()`, `.toLocalIterator()` on large DataFrames |
| Python UDFs | `@udf`, `udf(lambda ...)` — serialization overhead; replace with Spark SQL or Pandas UDF |
| Unbounded shuffle | Joins/aggregations without broadcast hint when one side is small |
| Wrong partition count | `spark.sql.shuffle.partitions` left at default 200 regardless of data size |
| Repeated recomputation | Same DataFrame transformed multiple times without `.cache()` or `.persist()` |
| Skewed join/group-by | One key dominates (NULL keys, hot user IDs, high-cardinality skew) |
| Reading too much data | No partition pruning, no predicate pushdown, selecting all columns (`*`) |
| Schema inference | `inferSchema=True` on CSV/JSON in production — slow and non-deterministic |
| Small file problem | Writing thousands of tiny files; missing `OPTIMIZE` after write |
| Unbounded window | `PARTITION BY` on high-cardinality column, missing `ROWS BETWEEN` bound |
| Unneeded repartition | `repartition(n)` before a write that already triggers a shuffle |
| Delta anti-patterns | Missing `OPTIMIZE`/`ZORDER`, no partition pruning on Delta, blind full `overwrite` of large tables |

## Step 2 — Rewrite: produce an optimized version

Apply all HIGH and MED severity fixes. For every change, add an inline comment:
`# OPTIMIZATION: <reason>`

Apply these fixes:

- **Broadcast small tables**: `F.broadcast(small_df)` when the smaller side is under the auto-broadcast threshold; tune `spark.sql.autoBroadcastJoinThreshold`
- **Replace Python UDFs**: rewrite with `F.when()`, `F.regexp_extract()`, `F.transform()`, `F.aggregate()`, or `spark.sql()`; use `@pandas_udf` only when vectorized logic is unavoidable
- **Tune shuffle partitions**: `spark.conf.set("spark.sql.shuffle.partitions", <2-3x total cores>)`; explain the sizing rationale
- **Add cache/persist**: `.cache()` before the first action on DataFrames used more than once; `.unpersist()` when done
- **Skew handling**: enable AQE skew join (`spark.sql.adaptive.skewJoin.enabled=true`); show salting for extreme cases
- **Predicate pushdown**: move `.filter()` before joins; use partition column predicates to prune files
- **Output file sizing**: `COALESCE` or `REPARTITION` to target ~128 MB per output file
- **Delta optimizations**: `OPTIMIZE <table> ZORDER BY (<col>)` after large writes; use `replaceWhere` instead of full table overwrite when possible

## Step 3 — Recommended Spark config block

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "<recommended_value>")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

Explain each setting in one sentence. Recommend where to set it: cluster config vs. job config vs. notebook cell.

## Step 4 — Summary table

| # | Issue | Severity | Fix Applied | Expected Impact |
|---|---|---|---|---|
| 1 | Python UDF on `clean_phone` | HIGH | Replaced with `F.regexp_replace` | ~4x faster on this column |
| 2 | ... | | | |

End with an overall estimate: e.g., "Expected 3-5x runtime reduction and ~60% shuffle data reduction."
