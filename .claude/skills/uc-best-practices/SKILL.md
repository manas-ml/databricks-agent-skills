---
name: uc-best-practices
description: Provide comprehensive Unity Catalog best practices for Databricks — covering catalog design, naming conventions, access control, data classification, lineage, security, governance, and operational standards. Use when the user asks for Unity Catalog best practices, UC design guidelines, governance standards, permission patterns, metastore setup advice, or a review of their Unity Catalog implementation.
---

# Unity Catalog Best Practices

Provide a comprehensive best-practices review or guide for Unity Catalog on Databricks. If the user shares their current UC setup, review it against these standards. If they ask for general guidance, produce a structured guide covering all relevant sections below.

---

## 1. Metastore and Catalog Design

### One metastore per region — always

A Unity Catalog metastore is a regional control plane. All workspaces in a region should share a single metastore for unified lineage, consistent access control, and centralised governance.

```
Region: East US
  └── Metastore: uc-eastus-prod
        ├── Workspace: data-engineering-ws
        ├── Workspace: data-science-ws
        └── Workspace: analytics-ws
```

**Rules:**
- Never create one metastore per workspace — this fragments lineage and duplicates governance effort
- Never create one metastore per team — teams are modelled as catalogs, not metastores
- Cross-region data sharing uses Delta Sharing, not multi-metastore federation

### Catalog per environment, not per team

```
Catalogs (one per env per domain):
  ├── dev_sales        ← sales domain, dev environment
  ├── staging_sales    ← sales domain, staging environment
  ├── prod_sales       ← sales domain, production environment
  ├── dev_finance
  ├── prod_finance
  └── shared_reference ← shared dimension tables, cross-domain lookup data
```

**Rules:**
- Never use a single catalog for all environments — dev/prod isolation is critical
- Never use a single `prod` catalog for all domains — cross-domain blast radius is too large
- Prefix catalogs with environment: `dev_`, `staging_`, `prod_` — makes programmatic environment targeting trivial
- One `shared_reference` or `shared_dims` catalog for cross-domain shared assets (calendars, geography, currencies)

### Schema per medallion layer

```
prod_sales
  ├── bronze        ← raw ingestion; append-only; source system fidelity
  ├── silver        ← cleansed, typed, validated; current records
  ├── gold          ← aggregated, business-ready; fact and dimension tables
  ├── semantic      ← views, metrics, OBT; BI-facing layer
  ├── ml            ← feature tables, model artifacts, experiment data
  └── observability ← pipeline metrics, DQ results, recon logs, audit
```

**Rules:**
- Never mix medallion layers in a single schema — consumers cannot tell what's safe to query
- Never put temporary or scratch tables in production schemas — use a `_sandbox` or `_tmp` schema that auto-purges
- The `semantic` schema is the only schema exposed to BI tools and end consumers
- The `observability` schema is internal to the data engineering team only

---

## 2. Naming Conventions

Consistent naming is the first line of defence against governance confusion.

### Tables

| Layer | Pattern | Example |
|---|---|---|
| Bronze | `raw_<source>_<entity>` | `raw_salesforce_opportunities` |
| Silver | `<entity>` | `opportunities` |
| Gold fact | `fact_<process>` | `fact_sales_orders` |
| Gold dimension | `dim_<entity>` | `dim_customer` |
| Gold aggregate | `agg_<metric>_<grain>` | `agg_revenue_daily` |
| OBT | `obt_<subject>` | `obt_sales` |
| View | `v_<entity>` | `v_orders` |
| External table | `ext_<entity>` | `ext_legacy_orders` |
| Temporary | `_tmp_<pipeline>_<date>` | `_tmp_orders_load_20240101` |

### Columns

- Primary key: `<entity>_sk` (surrogate) or `<entity>_id` (business/natural key)
- Foreign keys: `<ref_entity>_sk` — always suffix surrogate FK with `_sk`
- Timestamps: `_at` suffix — `created_at`, `updated_at`, `loaded_at`
- Booleans: `is_` prefix — `is_current`, `is_active`, `is_deleted`
- Audit columns: prefix with `_` — `_ingested_at`, `_source_file`, `_row_hash`
- Never use reserved words as column names: `date`, `timestamp`, `user`, `table`, `schema`

### Objects

```
Storage credentials:   <cloud>-<env>-<purpose>-cred     → adls-prod-silver-cred
External locations:    <env>-<layer>-location            → prod-bronze-location
Volumes:               <purpose>-vol                     → raw-files-vol
Service principals:    sp-<env>-<role>                   → sp-prod-etl
Groups:                <domain>-<role>                   → sales-data-engineers
Shares:                <domain>-<consumer>-share          → sales-partner-share
```

---

## 3. Access Control

### Grant to groups, never to users

```sql
-- BAD — user-level grants break when people change roles or leave
GRANT SELECT ON TABLE prod_sales.gold.fact_orders TO `john.doe@company.com`;

-- GOOD — group-level grants; manage membership separately in IdP
GRANT SELECT ON TABLE prod_sales.gold.fact_orders TO `sales-data-analysts`;
```

### Least-privilege grant hierarchy

Always grant the minimum required at each level. A `SELECT` on a table requires `USE CATALOG` and `USE SCHEMA` first — grant those too, but nothing more:

```sql
-- Minimum grants for read-only analyst access to Gold
GRANT USE CATALOG ON CATALOG prod_sales                  TO `sales-data-analysts`;
GRANT USE SCHEMA  ON SCHEMA  prod_sales.gold             TO `sales-data-analysts`;
GRANT USE SCHEMA  ON SCHEMA  prod_sales.semantic         TO `sales-data-analysts`;
GRANT SELECT      ON SCHEMA  prod_sales.semantic         TO `sales-data-analysts`;  -- all views in semantic
GRANT SELECT      ON TABLE   prod_sales.gold.fact_orders TO `sales-data-analysts`;  -- specific table

-- ETL service principal — modify Silver, read Bronze
GRANT USE CATALOG ON CATALOG prod_sales                  TO `sp-prod-etl`;
GRANT USE SCHEMA  ON SCHEMA  prod_sales.bronze           TO `sp-prod-etl`;
GRANT USE SCHEMA  ON SCHEMA  prod_sales.silver           TO `sp-prod-etl`;
GRANT SELECT      ON SCHEMA  prod_sales.bronze           TO `sp-prod-etl`;
GRANT MODIFY      ON SCHEMA  prod_sales.silver           TO `sp-prod-etl`;
```

### Standard persona matrix

Define and document a standard set of personas for every domain:

| Persona | Group pattern | Catalog access | Schema access | Table access |
|---|---|---|---|---|
| Data Engineer | `<domain>-data-engineers` | USE CATALOG | USE SCHEMA (all) | SELECT + MODIFY (silver/bronze) |
| Data Analyst | `<domain>-data-analysts` | USE CATALOG | USE SCHEMA (gold, semantic) | SELECT (gold, semantic) |
| Data Scientist | `<domain>-data-scientists` | USE CATALOG | USE SCHEMA (gold, ml) | SELECT (gold) + MODIFY (ml) |
| BI Consumer | `<domain>-bi-consumers` | USE CATALOG | USE SCHEMA (semantic) | SELECT (semantic only) |
| Data Admin | `<domain>-data-admins` | ALL PRIVILEGES | ALL PRIVILEGES | ALL PRIVILEGES |
| ETL SP | `sp-<env>-etl` | USE CATALOG | USE SCHEMA (bronze, silver) | SELECT bronze + MODIFY silver |

### Ownership rules

```sql
-- Every catalog, schema, and table must have an explicit owner (group, not user)
ALTER CATALOG prod_sales         SET OWNER TO `sales-data-admins`;
ALTER SCHEMA  prod_sales.silver  SET OWNER TO `sales-data-engineers`;
ALTER TABLE   prod_sales.silver.orders SET OWNER TO `sales-data-engineers`;

-- NEVER leave ownership as a human user — breaks when they leave the company
-- Audit orphaned objects monthly:
SELECT table_catalog, table_schema, table_name, table_owner
FROM system.information_schema.tables
WHERE table_owner LIKE '%@%'   -- user email addresses = human owners
  AND table_catalog LIKE 'prod%';
```

---

## 4. Row-Level and Column-Level Security

### Row filter guidelines

```sql
-- GOOD pattern: function reads entitlement from a governed table
CREATE OR REPLACE FUNCTION prod_sales.security.region_row_filter(region_code STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('sales-data-admins')   -- admins bypass
  OR region_code IN (
    SELECT region_code
    FROM prod_sales.security.user_region_entitlements
    WHERE user_email = current_user()
  );

ALTER TABLE prod_sales.gold.fact_orders
  SET ROW FILTER prod_sales.security.region_row_filter ON (region_code);
```

**Rules:**
- Row filter functions must live in a dedicated `security` schema — never mixed with business data
- Always include an admin bypass (`is_account_group_member('data-admins')`) so admins can audit all data
- Test row filters with `EXPLAIN` on the filtered table — verify the filter appears in the query plan
- Never build row filters using hardcoded user lists — use an entitlement table that data owners manage

### Column mask guidelines

```sql
-- GOOD pattern: deterministic masking that preserves format
CREATE OR REPLACE FUNCTION prod_sales.security.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('pii-viewers') THEN ssn
  ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4))   -- preserve last 4 for debugging
END;

ALTER TABLE prod_sales.silver.customers
  ALTER COLUMN ssn SET MASK prod_sales.security.mask_ssn;
```

**Rules:**
- Never mask by returning a static literal like `'REDACTED'` — breaks downstream joins and type contracts
- Preserve format where possible (last 4 digits of SSN, first 2 chars of email) — aids debugging without exposing PII
- Column masks are enforced at the table level, not the view level — apply to source tables, not semantic views
- Document every mask function in a data classification registry table

---

## 5. Data Classification

### Classification taxonomy

Define four tiers applied consistently across all domains:

| Tier | Tag value | Examples | Access |
|---|---|---|---|
| Public | `public` | Aggregated metrics, published reports | All authenticated users |
| Internal | `internal` | Business data, non-PII operational data | Employees only |
| Confidential | `confidential` | Salary, strategy, unreleased financials | Role-based, need-to-know |
| Restricted | `restricted` | PII, PHI, PCI, credentials | Explicit grant + column mask required |

### Apply classification tags

```sql
-- Tag columns
ALTER TABLE prod_sales.silver.customers
  ALTER COLUMN email       SET TAGS ('classification' = 'restricted', 'pii_type' = 'email');
ALTER TABLE prod_sales.silver.customers
  ALTER COLUMN full_name   SET TAGS ('classification' = 'restricted', 'pii_type' = 'name');
ALTER TABLE prod_sales.silver.customers
  ALTER COLUMN customer_id SET TAGS ('classification' = 'internal');

-- Tag the table
ALTER TABLE prod_sales.silver.customers
  SET TAGS ('contains_pii' = 'true', 'data_classification' = 'restricted', 'data_owner' = 'privacy-team');

-- Tag the schema
ALTER SCHEMA prod_sales.silver
  SET TAGS ('env' = 'prod', 'domain' = 'sales', 'medallion_layer' = 'silver');
```

### Query all restricted columns across all catalogs

```sql
SELECT table_catalog, table_schema, table_name, column_name,
       MAX(CASE WHEN tag_name = 'pii_type'         THEN tag_value END) AS pii_type,
       MAX(CASE WHEN tag_name = 'classification'   THEN tag_value END) AS classification
FROM system.information_schema.column_tags
WHERE tag_name IN ('classification', 'pii_type')
  AND table_catalog LIKE 'prod%'
GROUP BY 1, 2, 3, 4
HAVING MAX(CASE WHEN tag_name = 'classification' THEN tag_value END) = 'restricted'
ORDER BY table_catalog, table_schema, table_name, column_name;
```

---

## 6. Storage and External Locations

### External location hierarchy

```
Storage account / S3 bucket
  └── container / prefix
        ├── bronze/      ← external location: prod-bronze-location
        ├── silver/      ← external location: prod-silver-location
        ├── gold/        ← external location: prod-gold-location
        └── checkpoints/ ← external location: prod-checkpoints-location
```

**Rules:**
- One external location per medallion layer — limits blast radius if a credential is compromised
- Never use a single external location for the entire storage account — too broad
- Use managed storage (Unity Catalog-managed location) for Gold and Semantic — avoid manual file management
- Use external locations for Bronze only when the landing zone is shared with non-Databricks systems

```sql
-- Validate storage credential before first use
VALIDATE STORAGE CREDENTIAL prod_adls_cred;

-- Validate external location coverage
VALIDATE EXTERNAL LOCATION prod_bronze_location;

-- List all external locations and their URLs
SHOW EXTERNAL LOCATIONS;
```

### Volume vs. external table

| Use case | Use |
|---|---|
| Landing files (CSV, JSON, PDF) for Auto Loader | **Volume** |
| Reference files (lookup CSVs, config JSONs) | **Volume** |
| Structured data managed by Databricks | **Managed Delta table** |
| Structured data managed by external system | **External Delta table** |
| Data owned by another team's system | **Foreign table via Lakehouse Federation** |

---

## 7. Lineage and Auditing

### Lineage best practices

```sql
-- Check column-level lineage for a Gold table
SELECT
  source_table_full_name,
  source_column_name,
  target_table_full_name,
  target_column_name,
  event_time
FROM system.access.column_lineage
WHERE target_table_full_name = 'prod_sales.gold.fact_orders'
ORDER BY event_time DESC
LIMIT 50;

-- Find all downstream consumers of a Silver table (impact analysis)
SELECT DISTINCT target_table_full_name, created_by
FROM system.access.table_lineage
WHERE source_table_full_name = 'prod_sales.silver.orders'
ORDER BY target_table_full_name;
```

**Rules:**
- Always use `spark.table("catalog.schema.table")` and `saveAsTable(...)` in PySpark — these operations are captured in lineage; `spark.read.load(path)` is not
- Use DLT `dlt.read()` / `dlt.read_stream()` instead of `spark.read` inside DLT pipelines — only DLT reader API is lineage-tracked
- Never write to Delta tables via raw `spark.write.save(path)` in production — use `saveAsTable` for lineage capture

### Audit log monitoring

```sql
-- Weekly audit: users who accessed sensitive tables
SELECT user_name, action_name, request_params:table_full_name AS table_name,
       response_status, DATE(event_time) AS event_date, COUNT(*) AS access_count
FROM system.access.audit
WHERE request_params:table_full_name IN (
  SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name)
  FROM system.information_schema.table_tags
  WHERE tag_name = 'data_classification' AND tag_value = 'restricted'
)
AND event_time >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY 1, 2, 3, 4, 5
ORDER BY event_date DESC, access_count DESC;

-- Detect privilege escalations (new GRANT events)
SELECT user_name, action_name, request_params, event_time
FROM system.access.audit
WHERE action_name IN ('grantPrivilege', 'revokePrivilege', 'updatePermissions')
  AND event_time >= current_timestamp() - INTERVAL 1 DAY
ORDER BY event_time DESC;

-- Failed access attempts (unauthorised queries)
SELECT user_name, action_name, response_error_code, response_error_message, event_time
FROM system.access.audit
WHERE response_status = 'FAIL'
  AND event_time >= current_timestamp() - INTERVAL 1 DAY
ORDER BY event_time DESC;
```

**Schedule** these queries as Databricks SQL Alerts with weekly/daily cadence.

---

## 8. Delta Sharing and External Access

### Share design principles

```sql
-- One share per consumer relationship — never one share for all consumers
CREATE SHARE sales_partner_acme_share
  COMMENT 'Sales data shared with Acme Corp — read-only, Gold layer only';

-- Add only the minimum tables/columns the consumer needs
ALTER SHARE sales_partner_acme_share
  ADD TABLE prod_sales.semantic.v_orders    -- semantic view, not raw Gold
  PARTITION FILTER (region_code = 'WEST'); -- partition filter limits data exposure

-- Create recipient with IP allowlist
CREATE RECIPIENT acme_corp_recipient
  IP_RESTRICTIONS ('203.0.113.0/24')       -- restrict to Acme's IP range
  COMMENT 'Acme Corp — partner analytics team';

GRANT SELECT ON SHARE sales_partner_acme_share TO RECIPIENT acme_corp_recipient;
```

**Rules:**
- Always share Semantic layer views, not Gold tables directly — views enforce column selection and any row filters
- Always apply partition filters on shares to limit data exposure by region, date, or tenant
- Always set IP allowlists on recipients for external organisations
- Audit share access monthly via `system.access.audit` filtering on `action_name = 'deltaSharing*'`
- Never share Bronze or Silver tables externally — only Gold and Semantic

---

## 9. Governance Automation and Drift Detection

### Monthly governance health check (schedule as a job)

```sql
-- 1. Tables missing COMMENT
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.tables
WHERE table_catalog LIKE 'prod%'
  AND table_comment IS NULL
ORDER BY table_catalog, table_schema, table_name;

-- 2. Tables owned by a human user (should be a group or SP)
SELECT table_catalog, table_schema, table_name, table_owner
FROM system.information_schema.tables
WHERE table_catalog LIKE 'prod%'
  AND table_owner LIKE '%@%';

-- 3. Columns missing COMMENT in Gold and Semantic
SELECT table_catalog, table_schema, table_name, column_name
FROM system.information_schema.columns
WHERE table_catalog LIKE 'prod%'
  AND table_schema IN ('gold', 'semantic')
  AND comment IS NULL
ORDER BY table_catalog, table_schema, table_name, column_name;

-- 4. Restricted tables without a row filter
SELECT t.table_catalog, t.table_schema, t.table_name
FROM system.information_schema.tables t
JOIN system.information_schema.table_tags tt
  ON t.table_catalog = tt.table_catalog
  AND t.table_schema = tt.table_schema
  AND t.table_name   = tt.table_name
LEFT JOIN system.information_schema.row_filters rf
  ON t.table_catalog = rf.table_catalog
  AND t.table_schema = rf.table_schema
  AND t.table_name   = rf.table_name
WHERE tt.tag_name = 'data_classification' AND tt.tag_value = 'restricted'
  AND rf.function_name IS NULL;

-- 5. Grants to individual users (should be groups)
SELECT grantee, privilege_type, table_catalog, table_schema, table_name
FROM system.information_schema.table_privileges
WHERE grantee LIKE '%@%'
  AND table_catalog LIKE 'prod%';
```

---

## 10. Common Anti-Patterns — Quick Reference

| Anti-pattern | Impact | Fix |
|---|---|---|
| One catalog for all environments | Dev data pollutes prod; no isolation | Separate catalogs: `dev_`, `staging_`, `prod_` |
| Granting to individual users | Grants orphan when users leave | Always grant to groups |
| Human user as table owner | Ownership breaks on org changes | Set owner to a group or service principal |
| Exposing Silver/Bronze to BI tools | Raw data with unclear semantics consumed directly | Only expose `semantic` schema to BI tools |
| Sharing Gold tables via Delta Sharing | Too much data exposed; no column governance | Share Semantic views with partition filters |
| No COMMENT on Gold/Semantic columns | Business users can't discover meaning | COMMENT on every column in gold and semantic |
| Single external location for all layers | Broad credential scope; large blast radius | One external location per medallion layer |
| Row filter without admin bypass | Data admins locked out of their own tables | Always include `is_account_group_member('data-admins')` bypass |
| Using `spark.read.load(path)` in production | Not captured in UC lineage | Use `spark.table()` and `saveAsTable()` |
| No audit query schedule | Governance drift goes undetected | Weekly SQL Alert on audit tables |
| Mixing tmp tables in prod schemas | Clutters catalog; misleads consumers | Use `_tmp_` prefix in a dedicated sandbox schema |
| Column mask returns static string | Breaks type contracts and downstream joins | Preserve format with partial masking |

---

## Applying This Guide

When reviewing a user's Unity Catalog setup, check each section and flag violations as:
`[SECTION] [SEVERITY: HIGH|MED|LOW] <Issue> — <why it matters> → <fix>`

When generating new Unity Catalog objects (catalogs, schemas, grants, tags), apply all standards above by default without being asked.
