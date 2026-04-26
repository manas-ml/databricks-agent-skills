---
name: databricks-lakebase
description: Provision and use Databricks Lakebase — managed PostgreSQL databases alongside Delta Lake. Use when the user asks about Lakebase, managed Postgres on Databricks, OLTP workloads on Databricks, connecting PostgreSQL to notebooks, CDC from Postgres into Delta Lake, or hybrid OLTP and OLAP patterns on the Databricks platform.
---

# Databricks Lakebase

Provision managed PostgreSQL instances (Lakebase) and integrate them with Delta Lake pipelines on Databricks.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Use case: new Lakebase instance, connect from notebook, CDC into Delta Lake, or hybrid OLTP+OLAP query
- Database name and schema needed
- Whether CDC (Change Data Capture) into Delta Lake is required
- Access pattern: interactive queries, application writes, or ETL reads

## Step 2 — Provision a Lakebase instance

### Via Databricks SDK

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ProvisionedThroughputRequest

w = WorkspaceClient()

# Create a Lakebase (managed PostgreSQL) instance
lakebase = w.lakebase.create(
    name="sales-lakebase",
    spec={
        "instance_profile_arn": None,   # AWS: IAM role; Azure: managed identity
        "capacity": "SMALL",            # SMALL | MEDIUM | LARGE
        "storage_gb": 100,
        "tags": {"env": "prod", "domain": "sales"}
    }
)
print(f"Lakebase ID: {lakebase.id}, Status: {lakebase.status}")
```

### Via SQL (Unity Catalog integration)

```sql
-- Lakebase instances registered in Unity Catalog
SHOW LAKEBASE INSTANCES;

-- Connect to a Lakebase instance
USE LAKEBASE sales-lakebase;

-- Create a database and schema
CREATE DATABASE IF NOT EXISTS sales_app;
\c sales_app;
CREATE SCHEMA IF NOT EXISTS transactions;
```

## Step 3 — Connect from a Databricks notebook

```python
import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get connection details from Unity Catalog
lakebase_info = w.lakebase.get("sales-lakebase")
host     = lakebase_info.connection_info.host
port     = lakebase_info.connection_info.port
db_name  = "sales_app"

# Credentials from Databricks Secrets
pg_user  = dbutils.secrets.get("lakebase-secrets", "pg-user")
pg_pass  = dbutils.secrets.get("lakebase-secrets", "pg-password")

conn = psycopg2.connect(
    host=host, port=port, dbname=db_name,
    user=pg_user, password=pg_pass,
    sslmode="require"
)

# Read from Postgres into a Spark DataFrame
pg_jdbc_url = f"jdbc:postgresql://{host}:{port}/{db_name}"

orders_df = (
    spark.read
    .format("jdbc")
    .option("url", pg_jdbc_url)
    .option("dbtable", "transactions.orders")
    .option("user", pg_user)
    .option("password", pg_pass)
    .option("driver", "org.postgresql.Driver")
    .option("fetchsize", "10000")
    .option("numPartitions", "8")
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .load()
)
```

## Step 4 — Create and populate tables in Lakebase

```python
import psycopg2

with psycopg2.connect(...) as conn:
    with conn.cursor() as cur:
        # Create application table with CDC support
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions.orders (
                order_id      SERIAL PRIMARY KEY,
                customer_id   VARCHAR(50)   NOT NULL,
                status        VARCHAR(20)   NOT NULL DEFAULT 'OPEN',
                amount        NUMERIC(18,4) NOT NULL,
                created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
                updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
            );

            -- Index for common query patterns
            CREATE INDEX IF NOT EXISTS idx_orders_customer ON transactions.orders (customer_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status   ON transactions.orders (status, created_at);

            -- Enable logical replication for CDC (required for WAL-based CDC)
            ALTER TABLE transactions.orders REPLICA IDENTITY FULL;
        """)
        conn.commit()

# Write Spark DataFrame to Lakebase
orders_df.write \
    .format("jdbc") \
    .option("url", pg_jdbc_url) \
    .option("dbtable", "transactions.orders") \
    .option("user", pg_user) \
    .option("password", pg_pass) \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", "5000") \
    .mode("append") \
    .save()
```

## Step 5 — CDC from Lakebase into Delta Lake

Replicate real-time changes from Lakebase (PostgreSQL) into a Delta Lake Silver table using Debezium or Auto Loader with the JDBC streaming source.

### Option A — JDBC incremental batch (watermark-based)

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Read last watermark
last_hwm = spark.sql("""
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01') AS hwm
    FROM catalog.observability.pipeline_control
    WHERE pipeline_name = 'lakebase_orders_cdc'
      AND status = 'SUCCESS'
""").first()["hwm"]

# Incremental read from Lakebase
new_rows = (
    spark.read.format("jdbc")
    .option("url", pg_jdbc_url)
    .option("dbtable", f"(SELECT * FROM transactions.orders WHERE updated_at > '{last_hwm}') t")
    .option("user", pg_user).option("password", pg_pass)
    .option("driver", "org.postgresql.Driver")
    .load()
)

# Upsert into Delta Silver
if new_rows.count() > 0:
    (
        DeltaTable.forName(spark, "catalog.silver.orders").alias("t")
        .merge(new_rows.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
```

### Option B — DLT pipeline with JDBC Auto Loader (near-real-time)

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(comment="Raw CDC stream from Lakebase orders table")
def bronze_lakebase_orders():
    return (
        spark.readStream
        .format("jdbc")
        .option("url", pg_jdbc_url)
        .option("dbtable", "transactions.orders")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
```

## Step 6 — Hybrid OLTP + OLAP query pattern

Query Lakebase (transactional) and Delta Lake (analytical) in a single Databricks SQL query using Lakehouse Federation:

```sql
-- Register Lakebase as a foreign catalog
CREATE CONNECTION lakebase_sales
  TYPE POSTGRESQL
  OPTIONS (
    host '<lakebase-host>',
    port '5432',
    database 'sales_app',
    user secret ('lakebase-secrets', 'pg-user'),
    password secret ('lakebase-secrets', 'pg-password')
  );

CREATE FOREIGN CATALOG lakebase_sales_catalog
  USING CONNECTION lakebase_sales
  OPTIONS (database 'sales_app');

-- Join live Postgres data with historical Delta analytics
SELECT
  pg.order_id,
  pg.status AS current_status,
  pg.amount,
  d.customer_name,
  d.region,
  hist.total_lifetime_value
FROM lakebase_sales_catalog.transactions.orders pg
JOIN catalog.gold.dim_customer d    ON pg.customer_id = d.customer_id AND d.is_current = true
JOIN catalog.gold.customer_metrics hist ON pg.customer_id = hist.customer_id
WHERE pg.status = 'OPEN'
  AND pg.created_at >= current_date() - INTERVAL 1 DAY;
```

## Quality bars

- Always use `sslmode=require` for JDBC connections to Lakebase — plaintext connections are rejected
- Credentials stored in Databricks Secrets only — never in notebook code or job parameters
- Enable `REPLICA IDENTITY FULL` on Postgres tables before setting up CDC — partial identity causes UPDATE events to lose before-image data
- Use watermark-based incremental reads for batch CDC — full table scans are expensive and slow at scale
- Register Lakebase as a Lakehouse Federation foreign catalog for unified SQL queries across OLTP and OLAP without data movement
