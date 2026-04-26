---
name: data-vault-builder
description: Design and generate a Data Vault 2.0 model on Databricks Delta Lake. Use when the user asks to build a Data Vault, DV2 model, hub, link, satellite, raw vault, business vault, or an auditable historical data model on Databricks.
---

# Data Vault 2.0 Builder

Design a Data Vault 2.0 model and generate Hub, Link, and Satellite DDL plus PySpark load scripts for Delta Lake on Databricks.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Source entities and their business keys (e.g., "Customer identified by customer_id", "Order identified by order_number")
- Relationships between entities (e.g., "An Order belongs to a Customer and contains Products")
- Descriptive attributes per entity (columns that describe the entity but are not keys)
- Source system name (used as `RECORD_SOURCE`)
- Target Unity Catalog: `catalog.schema` for the raw vault layer
- Whether a Business Vault layer (computed satellite, bridge table) is needed

## Step 2 — Design the vault model

### Entity classification

For each entity the user describes, classify into vault object types:

| Object | Purpose | Example |
|---|---|---|
| **Hub** | Stores unique business keys; one row per unique entity | `hub_customer`, `hub_order`, `hub_product` |
| **Link** | Records relationships between Hubs; one row per unique relationship | `link_order_customer`, `link_order_product` |
| **Satellite** | Stores descriptive attributes with full history; one row per change | `sat_customer_details`, `sat_order_status` |

Draw the vault diagram (ASCII):

```
hub_customer ──────── link_order_customer ──────── hub_order
     │                                                  │
sat_customer_details                          sat_order_status
                                                         │
                              link_order_product ────── hub_product
                                                         │
                                              sat_product_details
```

## Step 3 — Generate DDL for every vault object

### Hub table

```sql
CREATE TABLE IF NOT EXISTS catalog.schema.hub_customer (
  customer_hk      STRING  NOT NULL COMMENT 'SHA-256 hash of business key',
  customer_id      STRING  NOT NULL COMMENT 'Business key from source',
  load_date        TIMESTAMP NOT NULL COMMENT 'When this record was first loaded',
  record_source    STRING  NOT NULL COMMENT 'Source system identifier',
  CONSTRAINT hub_customer_pk PRIMARY KEY (customer_hk)
)
USING DELTA
COMMENT 'Hub: unique customer business keys'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
```

Rules for Hubs:
- One row per unique business key — never update or delete
- `customer_hk` = `SHA2(UPPER(TRIM(customer_id)), 256)`
- `load_date` = earliest timestamp this key was seen
- `record_source` = name of the source system (e.g., `"CRM"`, `"ERP"`)

### Link table

```sql
CREATE TABLE IF NOT EXISTS catalog.schema.link_order_customer (
  order_customer_hk  STRING  NOT NULL COMMENT 'SHA-256 hash of all hub HKs combined',
  order_hk           STRING  NOT NULL COMMENT 'FK to hub_order',
  customer_hk        STRING  NOT NULL COMMENT 'FK to hub_customer',
  load_date          TIMESTAMP NOT NULL,
  record_source      STRING  NOT NULL,
  CONSTRAINT link_order_customer_pk PRIMARY KEY (order_customer_hk)
)
USING DELTA
COMMENT 'Link: relationship between orders and customers';
```

Rules for Links:
- One row per unique combination of hub hash keys — insert-only
- `order_customer_hk` = `SHA2(CONCAT(order_hk, '||', customer_hk), 256)`
- Order of hash key concatenation must be consistent and documented
- Never null out a relationship — use an effectivity satellite instead

### Satellite table

```sql
CREATE TABLE IF NOT EXISTS catalog.schema.sat_customer_details (
  customer_hk      STRING     NOT NULL COMMENT 'FK to hub_customer',
  load_date        TIMESTAMP  NOT NULL COMMENT 'When this version was loaded',
  load_end_date    TIMESTAMP  COMMENT 'When this version was superseded; NULL = current',
  hash_diff        STRING     NOT NULL COMMENT 'SHA-256 hash of all tracked attribute values',
  record_source    STRING     NOT NULL,
  -- Descriptive attributes
  full_name        STRING,
  email            STRING,
  phone            STRING,
  segment          STRING,
  city             STRING,
  country_code     STRING
)
USING DELTA
COMMENT 'Satellite: customer descriptive attributes with full history'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

Rules for Satellites:
- One row per change in any tracked attribute — insert-only for new versions
- `hash_diff` = `SHA2(CONCAT_WS('||', COALESCE(col1,''), COALESCE(col2,''), ...), 256)` — load only when hash_diff differs from the current record
- `load_end_date` is set when a new version arrives (end-date the previous row)
- Split satellites by rate of change if some columns change frequently and others rarely

## Step 4 — PySpark load scripts

### Hash key utility

```python
from pyspark.sql import functions as F

def make_hash_key(*cols):
    return F.sha2(
        F.concat_ws("||", *[F.upper(F.trim(F.col(c).cast("string"))) for c in cols]),
        256
    )
```

### Hub load (insert-new-only)

```python
def load_hub(source_df, hub_table: str, bk_col: str, hk_col: str, record_source: str):
    new_keys = (
        source_df
        .withColumn(hk_col, make_hash_key(bk_col))
        .withColumn("load_date", F.current_timestamp())
        .withColumn("record_source", F.lit(record_source))
        .select(hk_col, bk_col, "load_date", "record_source")
        .dropDuplicates([hk_col])
    )
    existing = spark.table(hub_table).select(hk_col)
    inserts = new_keys.join(existing, hk_col, "left_anti")
    inserts.write.format("delta").mode("append").saveAsTable(hub_table)
```

### Link load (insert-new-only)

```python
def load_link(source_df, link_table: str, hub_hk_cols: list[str], link_hk_col: str, record_source: str):
    new_links = (
        source_df
        .withColumn(link_hk_col, make_hash_key(*hub_hk_cols))
        .withColumn("load_date", F.current_timestamp())
        .withColumn("record_source", F.lit(record_source))
        .select(link_hk_col, *hub_hk_cols, "load_date", "record_source")
        .dropDuplicates([link_hk_col])
    )
    existing = spark.table(link_table).select(link_hk_col)
    inserts = new_links.join(existing, link_hk_col, "left_anti")
    inserts.write.format("delta").mode("append").saveAsTable(link_table)
```

### Satellite load (insert-on-change, end-date previous)

```python
def load_satellite(source_df, sat_table: str, hk_col: str, attribute_cols: list[str], record_source: str):
    new_data = (
        source_df
        .withColumn("hash_diff", make_hash_key(*attribute_cols))
        .withColumn("load_date", F.current_timestamp())
        .withColumn("load_end_date", F.lit(None).cast("timestamp"))
        .withColumn("record_source", F.lit(record_source))
        .select(hk_col, "load_date", "load_end_date", "hash_diff", "record_source", *attribute_cols)
    )

    current = spark.table(sat_table).filter("load_end_date IS NULL").select(hk_col, "hash_diff")
    changed = new_data.join(
        current.withColumnRenamed("hash_diff", "_existing_hash"), hk_col, "left"
    ).filter("_existing_hash IS NULL OR hash_diff <> _existing_hash").drop("_existing_hash")

    if changed.count() == 0:
        return

    # End-date superseded records
    (
        DeltaTable.forName(spark, sat_table).alias("t")
        .merge(changed.alias("s"), f"t.{hk_col} = s.{hk_col} AND t.load_end_date IS NULL AND t.hash_diff <> s.hash_diff")
        .whenMatchedUpdate(set={"load_end_date": "s.load_date"})
        .execute()
    )
    # Insert new versions
    changed.write.format("delta").mode("append").saveAsTable(sat_table)
```

## Step 5 — Business Vault (optional)

If the user needs reporting-ready derived objects, generate a **Point-in-Time (PIT) table** that joins satellites at a consistent snapshot timestamp:

```sql
-- PIT table for customer at daily grain
CREATE OR REPLACE TABLE catalog.schema.pit_customer AS
SELECT
  d.date_actual AS snapshot_date,
  h.customer_hk,
  h.customer_id,
  s.full_name,
  s.email,
  s.segment
FROM catalog.schema.hub_customer h
CROSS JOIN catalog.schema.dim_date d
LEFT JOIN catalog.schema.sat_customer_details s
  ON s.customer_hk = h.customer_hk
  AND s.load_date <= d.date_actual
  AND (s.load_end_date IS NULL OR s.load_end_date > d.date_actual)
WHERE d.date_actual BETWEEN '2020-01-01' AND current_date();
```

## Step 6 — Output summary

End with:
- Complete list of vault objects generated (Hubs, Links, Satellites)
- Load order: Hubs first → Links → Satellites (Hubs must exist before Links can reference them)
- Hash key concatenation conventions used (document for the team)
- ZORDER recommendation: Satellites → `ZORDER BY (customer_hk, load_date)`
- Table showing each source attribute and which satellite it belongs to
