Diagnose and optimize the following Spark job or PySpark code for performance on Databricks:

$ARGUMENTS

---

**Step 1 — Diagnose: scan for these anti-patterns**

Check the provided code or job description for each issue below. For every issue found, report it as:
`[SEVERITY: HIGH|MED|LOW] <Issue> — <one-line explanation of why it hurts>`

| Anti-pattern | What to check |
|---|---|
| Driver collect | `.collect()`, `.toPandas()`, `.toLocalIterator()` on large DataFrames |
| Python UDFs | `@udf`, `udf(lambda ...)` — replaced by Spark SQL / Pandas UDF |
| Unbounded shuffle | Joins or aggregations without broadcast hint on small tables |
| Wrong partition count | `spark.sql.shuffle.partitions` default 200 vs. actual data size |
| Repeated recomputation | Same DataFrame transformed multiple times without `.cache()` or `.persist()` |
| Skewed join/group-by | One key dominates (e.g., NULL keys, hot user IDs) |
| Reading too much data | No partition pruning, no predicate pushdown, reading all columns |
| Schema inference | `inferSchema=True` on CSV/JSON in production paths |
| Small file problem | Many tiny files causing excessive task overhead |
| Inefficient window functions | `PARTITION BY` on high-cardinality column or missing `ROWS BETWEEN` bound |
| Unneeded repartition | `repartition(n)` before a write that already shuffles |
| Delta anti-patterns | Missing `OPTIMIZE`/`ZORDER`, no partition pruning on Delta, blind `overwrite` of large tables |

---

**Step 2 — Rewrite: produce an optimized version**

Apply all HIGH and MED severity fixes. For each change, add a one-line inline comment:
`# OPTIMIZATION: <reason>`

Key rewrites to apply:

- **Broadcast small tables**: wrap the smaller side with `broadcast()` hint when < 200 MB (configurable via `spark.sql.autoBroadcastJoinThreshold`)
- **Replace Python UDFs**: rewrite with `F.when()`, `F.regexp_extract()`, `F.transform()`, `F.aggregate()`, or Spark SQL expressions; use `@pandas_udf` only if vectorized logic is unavoidable
- **Fix shuffle partitions**: set `spark.conf.set("spark.sql.shuffle.partitions", <2-3x core count>)` and explain the sizing
- **Add cache/persist**: insert `.cache()` before the first action on DataFrames used more than once; add `.unpersist()` at end of scope
- **Skew handling**: add `spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")` and AQE settings; for extreme skew, show salting pattern
- **Predicate pushdown**: move `.filter()` calls before joins; use partition column filters
- **Partition sizing**: show `REPARTITION` or `COALESCE` to target ~128 MB per output file
- **Delta optimizations**: add `OPTIMIZE <table> ZORDER BY (<col>)` after large writes; use `replaceWhere` instead of full overwrites

---

**Step 3 — Spark config block**

Output a recommended Spark config block to add to the cluster or job:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "<recommended_value>")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

Explain each setting in one sentence.

---

**Step 4 — Summary table**

| # | Issue | Severity | Fix Applied | Expected Impact |
|---|---|---|---|---|
| 1 | ... | HIGH | ... | ~Xx faster / ~X% less shuffle |

End with an estimated overall improvement (e.g., "Expected 3-5x runtime reduction, ~60% shuffle reduction").
