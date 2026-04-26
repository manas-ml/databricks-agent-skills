---
name: star-schema-builder
description: Design and generate a dimensional star schema data model in Delta Lake on Databricks. Use when the user asks to build a data warehouse, dimensional model, star schema, fact table, dimension table, or analytical data model for reporting or BI on Databricks.
---

# Star Schema Builder

Design a dimensional star schema and generate all DDL, surrogate key logic, and PySpark load scripts for Delta Lake on Databricks.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Business domain and reporting subject (e.g., "sales analytics", "customer orders")
- Source tables or entities available (paste schema or column names)
- Known facts to measure (e.g., revenue, quantity, duration)
- Known dimensions (e.g., customer, product, date, geography)
- Slowly Changing Dimension (SCD) requirements per dimension: Type 1 (overwrite) or Type 2 (history)
- Target Unity Catalog: `catalog.schema` for the warehouse layer

## Step 2 — Design the model

### Grain definition
State the grain of the fact table explicitly:
> "Each row in `fact_orders` represents one order line item on one order date for one product sold to one customer."

### Entity diagram (ASCII)

```
         dim_date ──────────┐
         dim_customer ──────┤
                            ▼
         dim_product ──► fact_orders ◄── dim_geography
         dim_channel ──────┘
```

List every fact measure and every foreign key in the fact table.

### SCD strategy per dimension

| Dimension | SCD Type | Reason |
|---|---|---|
| dim_customer | Type 2 | Track address and segment changes over time |
| dim_product | Type 1 | Price overwritten; no history needed |
| dim_date | Static | Pre-populated, never changes |

## Step 3 — Generate DDL for every table

### Dimension tables

```sql
-- dim_customer (SCD Type 2)
CREATE TABLE IF NOT EXISTS catalog.schema.dim_customer (
  customer_sk      BIGINT  GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  customer_id      STRING  NOT NULL COMMENT 'Natural/business key from source',
  full_name        STRING,
  email            STRING,
  segment          STRING,
  city             STRING,
  country_code     STRING,
  -- SCD Type 2 control columns
  effective_start  DATE    NOT NULL COMMENT 'Date this version became active',
  effective_end    DATE    COMMENT 'Date this version was superseded; NULL = current',
  is_current       BOOLEAN NOT NULL DEFAULT true,
  -- Audit
  _source          STRING  COMMENT 'Source system',
  _loaded_at       TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Customer dimension with full change history (SCD Type 2)'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);
```

```sql
-- dim_date (static, pre-populated)
CREATE TABLE IF NOT EXISTS catalog.schema.dim_date (
  date_sk    INT     NOT NULL COMMENT 'Surrogate key: YYYYMMDD integer',
  date_actual DATE   NOT NULL,
  year        INT,
  quarter     INT,
  month       INT,
  month_name  STRING,
  week        INT,
  day_of_week INT,
  day_name    STRING,
  is_weekend  BOOLEAN,
  is_holiday  BOOLEAN
)
USING DELTA
COMMENT 'Date dimension, pre-populated from 2000-01-01 to 2050-12-31';
```

### Fact table

```sql
CREATE TABLE IF NOT EXISTS catalog.schema.fact_orders (
  order_line_sk    BIGINT  GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  -- Foreign keys (surrogate)
  customer_sk      BIGINT  NOT NULL,
  product_sk       BIGINT  NOT NULL,
  date_sk          INT     NOT NULL,
  geography_sk     BIGINT  NOT NULL,
  channel_sk       BIGINT  NOT NULL,
  -- Degenerate dimensions
  order_id         STRING  NOT NULL COMMENT 'Order header key from source',
  order_line_id    STRING  NOT NULL,
  -- Measures
  quantity         INT,
  unit_price       DECIMAL(18,4),
  discount_amount  DECIMAL(18,4),
  net_revenue      DECIMAL(18,4),
  cost             DECIMAL(18,4),
  gross_margin     DECIMAL(18,4),
  -- Audit
  _loaded_at       TIMESTAMP DEFAULT current_timestamp(),
  _source          STRING
)
USING DELTA
PARTITIONED BY (date_sk)
COMMENT 'Order line item fact table; grain = one line per order per product per date'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.dataSkippingNumIndexedCols' = '8'
);
```

## Step 4 — PySpark load scripts

### Dimension load (SCD Type 2)

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F
import hashlib

def load_scd2_dimension(source_df, target_table: str, natural_key: str, tracked_cols: list[str]):
    hash_cols = F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in tracked_cols]))
    source_df = source_df.withColumn("_row_hash", hash_cols)

    dim = DeltaTable.forName(spark, target_table)

    # Close changed records
    dim.alias("t").merge(
        source_df.alias("s"),
        f"t.{natural_key} = s.{natural_key} AND t.is_current = true AND t._row_hash <> s._row_hash"
    ).whenMatchedUpdate(set={
        "effective_end": "current_date()",
        "is_current": "false"
    }).execute()

    # Insert new versions (new records + changed records)
    existing_current = spark.table(target_table).filter("is_current = true").select(natural_key, "_row_hash")
    new_rows = source_df.join(existing_current, natural_key, "left_anti")
    changed_rows = source_df.join(
        existing_current.withColumnRenamed("_row_hash", "_existing_hash"),
        natural_key
    ).filter("_row_hash <> _existing_hash")

    rows_to_insert = new_rows.union(changed_rows.drop("_existing_hash")) \
        .withColumn("effective_start", F.current_date()) \
        .withColumn("effective_end", F.lit(None).cast("date")) \
        .withColumn("is_current", F.lit(True))

    rows_to_insert.write.format("delta").mode("append").saveAsTable(target_table)
```

### Surrogate key lookup for fact load

```python
def resolve_sk(fact_df, dim_table: str, natural_key: str, sk_col: str, join_col: str):
    dim = spark.table(dim_table).filter("is_current = true").select(sk_col, natural_key)
    return fact_df.join(F.broadcast(dim), fact_df[join_col] == dim[natural_key], "left") \
                  .drop(natural_key)
```

### Date dimension population

```python
def populate_dim_date(start="2000-01-01", end="2050-12-31"):
    dates = spark.sql(f"SELECT sequence(to_date('{start}'), to_date('{end}'), interval 1 day) AS d") \
                 .selectExpr("explode(d) AS date_actual")
    return dates.withColumn("date_sk", F.date_format("date_actual", "yyyyMMdd").cast("int")) \
                .withColumn("year", F.year("date_actual")) \
                .withColumn("quarter", F.quarter("date_actual")) \
                .withColumn("month", F.month("date_actual")) \
                .withColumn("month_name", F.date_format("date_actual", "MMMM")) \
                .withColumn("week", F.weekofyear("date_actual")) \
                .withColumn("day_of_week", F.dayofweek("date_actual")) \
                .withColumn("day_name", F.date_format("date_actual", "EEEE")) \
                .withColumn("is_weekend", F.dayofweek("date_actual").isin([1, 7])) \
                .withColumn("is_holiday", F.lit(False))
```

## Step 5 — Output summary

End with:
- Complete list of tables generated (DDL provided for each)
- Load order (dimensions before fact; dim_date first)
- ZORDER recommendation per table (e.g., fact: `ZORDER BY (customer_sk, date_sk)`)
- A sample BI query joining fact to all dimensions to verify the model
