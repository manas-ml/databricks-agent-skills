---
name: databricks-unity-catalog
description: Set up, govern, and query Unity Catalog on Databricks — catalog hierarchy, access control, system tables for lineage and audit, data classification, and governance best practices. Use when the user asks about Unity Catalog setup, catalog/schema/table permissions, GRANT statements, data lineage, audit logs, billing analysis, row/column security, or UC system tables.
---

# Databricks Unity Catalog

Design and implement Unity Catalog governance — hierarchy setup, access control, system table queries for lineage/audit/billing, and data classification.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Task type: catalog setup, access control, lineage query, audit query, billing analysis, or data classification
- Environment (dev/staging/prod) and cloud (AWS/Azure/GCP)
- Data domain and personas needing access (engineers, analysts, consumers, service principals)
- Whether external tables, volumes, or Delta Sharing are involved

## Step 2 — Catalog hierarchy setup

### Recommended structure per environment

```sql
-- One catalog per environment per domain
CREATE CATALOG IF NOT EXISTS dev_sales
  COMMENT 'Sales domain — development environment';

CREATE CATALOG IF NOT EXISTS prod_sales
  COMMENT 'Sales domain — production environment';

-- Schemas per medallion layer
CREATE SCHEMA IF NOT EXISTS prod_sales.bronze  COMMENT 'Raw ingestion layer';
CREATE SCHEMA IF NOT EXISTS prod_sales.silver  COMMENT 'Cleansed and typed layer';
CREATE SCHEMA IF NOT EXISTS prod_sales.gold    COMMENT 'Aggregated business layer';
CREATE SCHEMA IF NOT EXISTS prod_sales.semantic COMMENT 'BI-facing views and metrics';
CREATE SCHEMA IF NOT EXISTS prod_sales.observability COMMENT 'Pipeline metrics and DQ results';

-- Set managed location for each schema (files stored in cloud storage)
ALTER SCHEMA prod_sales.silver
  SET LOCATION 'abfss://silver@storageaccount.dfs.core.windows.net/sales/';
```

### External locations and storage credentials

```sql
-- Register a storage credential (Azure managed identity or AWS IAM role)
CREATE STORAGE CREDENTIAL prod_adls
  WITH AZURE_MANAGED_IDENTITY (DIRECTORY = '/subscriptions/.../resourceGroups/.../providers/Microsoft.ManagedIdentity/...')
  COMMENT 'ADLS Gen2 credential for prod data';

-- Create an external location
CREATE EXTERNAL LOCATION prod_silver_location
  URL 'abfss://silver@storageaccount.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL prod_adls)
  COMMENT 'External location for silver layer';

-- Validate
VALIDATE STORAGE CREDENTIAL prod_adls;
```

## Step 3 — Access control (GRANT hierarchy)

### Least-privilege grant pattern

```sql
-- Layer 1: catalog access (USE only — no data access)
GRANT USE CATALOG ON CATALOG prod_sales TO `data-engineers`;
GRANT USE CATALOG ON CATALOG prod_sales TO `data-analysts`;

-- Layer 2: schema access
GRANT USE SCHEMA ON SCHEMA prod_sales.silver TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA prod_sales.gold   TO `data-analysts`;

-- Layer 3: table/view access
GRANT SELECT ON TABLE prod_sales.gold.fact_orders   TO `data-analysts`;
GRANT SELECT ON TABLE prod_sales.semantic.v_orders  TO `data-analysts`;
GRANT MODIFY ON TABLE prod_sales.silver.orders      TO `data-engineers`;

-- Service principals for ETL jobs
GRANT USE CATALOG ON CATALOG prod_sales        TO `sp-etl-prod`;
GRANT USE SCHEMA  ON SCHEMA  prod_sales.silver TO `sp-etl-prod`;
GRANT MODIFY      ON TABLE   prod_sales.silver.orders TO `sp-etl-prod`;
GRANT SELECT      ON TABLE   prod_sales.bronze.raw_orders TO `sp-etl-prod`;

-- Grant ALL PRIVILEGES to catalog owners
GRANT ALL PRIVILEGES ON CATALOG prod_sales TO `data-platform-admins`;
```

### Revoke and audit grants

```sql
-- View all grants on a table
SHOW GRANTS ON TABLE prod_sales.silver.orders;

-- View all grants to a principal
SHOW GRANTS TO `data-analysts`;

-- Revoke
REVOKE SELECT ON TABLE prod_sales.silver.orders FROM `data-analysts`;
```

## Step 4 — Row and column security

### Row filter (restrict rows by user group)

```sql
CREATE OR REPLACE FUNCTION prod_sales.security.region_row_filter(region_code STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('sales-admins')
  OR region_code = (
    SELECT region_code FROM prod_sales.hr.employee_regions
    WHERE email = current_user() LIMIT 1
  );

ALTER TABLE prod_sales.gold.fact_orders
  SET ROW FILTER prod_sales.security.region_row_filter ON (region_code);
```

### Column mask (hide PII from non-privileged users)

```sql
CREATE OR REPLACE FUNCTION prod_sales.security.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('pii-viewers') THEN email
  ELSE CONCAT(LEFT(email, 2), '***@***.***')
END;

ALTER TABLE prod_sales.silver.customers
  ALTER COLUMN email SET MASK prod_sales.security.mask_email;
```

## Step 5 — System tables reference

Unity Catalog system tables are available in `system.*` — query them for lineage, audit, and cost.

### Data lineage

```sql
-- Column-level lineage: what tables feed into silver.orders?
SELECT
  source_table_full_name,
  target_table_full_name,
  source_column_name,
  target_column_name,
  created_by,
  event_time
FROM system.access.column_lineage
WHERE target_table_full_name = 'prod_sales.silver.orders'
ORDER BY event_time DESC;

-- Table-level lineage graph
SELECT source_table_full_name, target_table_full_name, event_type
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'prod_sales.%'
ORDER BY event_time DESC;
```

### Audit log — who accessed what

```sql
-- Recent access to a sensitive table
SELECT user_name, action_name, request_params, response_status, event_time
FROM system.access.audit
WHERE request_params:table_full_name = 'prod_sales.silver.customers'
  AND event_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- Failed access attempts (unauthorised queries)
SELECT user_name, action_name, response_status, event_time
FROM system.access.audit
WHERE response_status != 'SUCCESS'
  AND event_time >= current_timestamp() - INTERVAL 1 DAY
ORDER BY event_time DESC;
```

### Billing and cost attribution

```sql
-- DBU cost by job over the last 30 days
SELECT
  usage_metadata.job_id,
  usage_metadata.cluster_id,
  SUM(usage_quantity)  AS total_dbus,
  SUM(usage_quantity * list_prices.default_unit_price) AS estimated_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name AND u.usage_date BETWEEN lp.price_start_time::DATE AND COALESCE(lp.price_end_time::DATE, current_date())
WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
  AND u.sku_name LIKE '%JOBS%'
GROUP BY 1, 2
ORDER BY total_dbus DESC
LIMIT 20;

-- Cost by catalog (storage + compute)
SELECT
  usage_metadata.catalog_name,
  SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY 1
ORDER BY total_dbus DESC;
```

### Table and storage inventory

```sql
-- All tables in a catalog with size and row count
SELECT
  table_catalog, table_schema, table_name,
  table_type, data_source_format,
  storage_sub_directory,
  last_altered
FROM system.information_schema.tables
WHERE table_catalog = 'prod_sales'
ORDER BY table_schema, table_name;

-- Storage usage per schema
SELECT
  table_catalog, table_schema,
  COUNT(*) AS table_count,
  SUM(bytes) / 1e9 AS total_gb
FROM system.storage.table_storage
WHERE table_catalog = 'prod_sales'
GROUP BY 1, 2
ORDER BY total_gb DESC;
```

## Step 6 — Data classification with tags

```sql
-- Apply classification tags to sensitive columns
ALTER TABLE prod_sales.silver.customers
  ALTER COLUMN ssn SET TAGS ('pii' = 'true', 'classification' = 'restricted');

ALTER TABLE prod_sales.silver.customers
  ALTER COLUMN email SET TAGS ('pii' = 'true', 'classification' = 'confidential');

-- Tag a table as containing PII
ALTER TABLE prod_sales.silver.customers
  SET TAGS ('contains_pii' = 'true', 'data_owner' = 'privacy-team');

-- Query all PII-tagged columns across the catalog
SELECT table_catalog, table_schema, table_name, column_name, tag_value
FROM system.information_schema.column_tags
WHERE tag_name = 'pii' AND tag_value = 'true'
ORDER BY table_catalog, table_schema, table_name;
```

## Quality bars

- Always grant to groups, never to individual user emails — user membership changes; grants don't
- Every production table must have `SET OWNER TO` pointing to a service principal or group, not a human user
- Row filter and column mask functions live in a dedicated `security` schema, not mixed with business data
- Audit queries over `system.access.audit` scheduled weekly to detect anomalous access patterns
- All storage credentials and external locations validated with `VALIDATE STORAGE CREDENTIAL` before first use
