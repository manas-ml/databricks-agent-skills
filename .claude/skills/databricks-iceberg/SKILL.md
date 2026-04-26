---
name: databricks-iceberg
description: Work with Apache Iceberg tables on Databricks — managed and foreign Iceberg tables, UniForm (read Delta as Iceberg), Iceberg REST Catalog, and interoperability with Snowflake, Trino, and external Spark engines. Use when the user asks about Iceberg tables on Databricks, UniForm, reading Delta tables as Iceberg, Iceberg REST Catalog, cross-engine data sharing, or migrating Parquet/Hive tables to Iceberg.
---

# Databricks Iceberg

Create and manage Apache Iceberg tables on Databricks — natively managed, foreign (external Iceberg), UniForm-enabled Delta tables, and Iceberg REST Catalog interoperability.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Use case: create a managed Iceberg table, register a foreign Iceberg table, enable UniForm on an existing Delta table, or query Iceberg via REST Catalog from an external engine
- External engine needing access (Snowflake, Trino, Spark outside Databricks, PyIceberg)
- Unity Catalog catalog and schema
- Whether the table already exists as Delta (UniForm) or needs to be created fresh

## Step 2 — Create a managed Iceberg table in Unity Catalog

Databricks supports creating native Iceberg tables managed by Unity Catalog:

```sql
-- Create a managed Iceberg table (stored in UC managed location)
CREATE TABLE catalog.silver.orders_iceberg (
  order_id     STRING  NOT NULL,
  customer_id  STRING  NOT NULL,
  amount       DECIMAL(18,4),
  status       STRING,
  order_date   TIMESTAMP,
  region       STRING
)
USING ICEBERG
PARTITIONED BY (days(order_date))
TBLPROPERTIES (
  'write.target-file-size-bytes' = '134217728',   -- 128 MB target file size
  'write.parquet.compression-codec' = 'zstd'
);
```

Write to it like any Spark table:

```python
df.write.format("iceberg").mode("append").saveAsTable("catalog.silver.orders_iceberg")

# Upsert with Iceberg MERGE
spark.sql("""
  MERGE INTO catalog.silver.orders_iceberg t
  USING updates s ON t.order_id = s.order_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
```

## Step 3 — Enable UniForm on an existing Delta table

UniForm allows external engines to read a Delta table using the Iceberg or Hudi protocol without data duplication — the table stays Delta, but Iceberg metadata is generated alongside it.

```sql
-- Enable UniForm on an existing Delta table
ALTER TABLE catalog.gold.fact_orders
  SET TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg'
  );

-- Verify UniForm is active
DESCRIBE EXTENDED catalog.gold.fact_orders;
-- Look for: delta.universalFormat.enabledFormats = iceberg
```

For new tables:

```sql
CREATE TABLE catalog.gold.fact_orders_uniform (
  order_id    STRING NOT NULL,
  amount      DECIMAL(18,4),
  order_date  DATE
)
USING DELTA
TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.enableChangeDataFeed'           = 'true'
);
```

**UniForm limitations:**
- Deletion vectors must be disabled: `'delta.enableDeletionVectors' = 'false'`
- Column mapping mode must be `none` or `name` (not `id`)
- UniForm Iceberg metadata is updated asynchronously after each Delta commit

## Step 4 — Register a foreign Iceberg table

Register an existing Iceberg table (stored in S3/ADLS) in Unity Catalog as a foreign table:

```sql
-- First, ensure external location covering the table path exists
CREATE EXTERNAL LOCATION iceberg_data_location
  URL 's3://my-bucket/iceberg-warehouse/'
  WITH (STORAGE CREDENTIAL my_aws_credential);

-- Register the foreign Iceberg table
CREATE TABLE catalog.external.vendor_orders
  USING ICEBERG
  LOCATION 's3://my-bucket/iceberg-warehouse/vendor_orders/'
  TBLPROPERTIES ('iceberg.catalog' = 'hadoop');  -- or 'rest' for REST Catalog

-- Query it like any table
SELECT COUNT(*) FROM catalog.external.vendor_orders;
```

## Step 5 — Iceberg REST Catalog (share with external engines)

Databricks exposes a Unity Catalog-compatible Iceberg REST Catalog endpoint, allowing Snowflake, Trino, PyIceberg, and other engines to read Delta/Iceberg tables via the open Iceberg REST spec.

### Get the REST Catalog endpoint

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# The REST Catalog URL is derived from the workspace URL
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
rest_catalog_url = f"{workspace_url}/api/2.1/unity-catalog/iceberg"
print(f"Iceberg REST Catalog: {rest_catalog_url}")
```

### Connect from Snowflake

```sql
-- In Snowflake: create an Iceberg catalog integration pointing to Databricks REST Catalog
CREATE OR REPLACE CATALOG INTEGRATION databricks_iceberg
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'prod_sales.gold'
  REST_CONFIG = (
    CATALOG_URI = 'https://<workspace>.azuredatabricks.net/api/2.1/unity-catalog/iceberg',
    WAREHOUSE = '<catalog_name>'
  )
  REST_AUTHENTICATION = (
    TYPE = BEARER
    BEARER_TOKEN = '<databricks-pat>'
  )
  ENABLED = TRUE;

-- Query Delta table (UniForm enabled) from Snowflake as Iceberg
CREATE OR REPLACE ICEBERG TABLE snowflake_db.public.fact_orders
  CATALOG = 'databricks_iceberg'
  EXTERNAL_VOLUME = '<your_external_volume>'
  CATALOG_TABLE_NAME = 'fact_orders';

SELECT * FROM snowflake_db.public.fact_orders LIMIT 10;
```

### Connect from PyIceberg (Python)

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    name="databricks",
    uri="https://<workspace>.azuredatabricks.net/api/2.1/unity-catalog/iceberg",
    warehouse="prod_sales",          # Unity Catalog catalog name
    token="<databricks-pat>",
)

# List tables
tables = catalog.list_tables("gold")
print(tables)

# Load and scan a table
table = catalog.load_table("gold.fact_orders")
df = table.scan(limit=1000).to_pandas()
```

### Connect from Trino / Presto

```properties
# Trino catalog properties file: etc/catalog/databricks.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://<workspace>.azuredatabricks.net/api/2.1/unity-catalog/iceberg
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=<databricks-pat>
iceberg.rest-catalog.warehouse=prod_sales
```

## Step 6 — Migrate a Parquet/Hive table to Iceberg

```python
# Read existing Parquet table
parquet_df = spark.read.parquet("s3://bucket/legacy/orders/")

# Write as managed Iceberg table
parquet_df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .option("write.target-file-size-bytes", "134217728") \
    .saveAsTable("catalog.silver.orders_iceberg")

# Validate row count matches
source_count = parquet_df.count()
target_count = spark.table("catalog.silver.orders_iceberg").count()
assert source_count == target_count, f"Migration mismatch: {source_count} vs {target_count}"
print(f"Migration complete: {target_count:,} rows")
```

## Quality bars

- Enable UniForm on Delta tables that need cross-engine sharing — avoids data duplication entirely vs. converting to native Iceberg
- Disable deletion vectors (`'delta.enableDeletionVectors' = 'false'`) before enabling UniForm — incompatible features
- Always validate row counts after migrating Parquet/Hive to Iceberg
- Use the Iceberg REST Catalog for external engine access — do not share raw storage paths directly
- For Snowflake/Trino integration, use a dedicated service principal PAT — not a personal access token
