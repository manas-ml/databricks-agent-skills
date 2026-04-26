---
name: semantic-layer-builder
description: Build a semantic layer or logical data model over Databricks Gold tables using Unity Catalog views, governed metrics, row-level security, and BI tool connectivity. Use when the user asks to build a semantic layer, metric layer, reporting views, OBT (one big table), denormalized wide table, governed metrics, or a BI-ready abstraction over Delta Lake tables on Databricks.
---

# Semantic Layer Builder

Generate a governed semantic layer over Databricks Gold tables: Unity Catalog views, a metrics YAML, row-level security, and BI tool connectors.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Source Gold tables (fact + dimensions from `star-schema-builder`, or raw aggregated tables)
- Business domain and target BI tool (Power BI, Tableau, Looker, or Databricks SQL dashboards)
- Named business metrics needed (e.g., "gross margin", "customer lifetime value", "order fill rate")
- Row-level security requirements (e.g., "sales reps see only their region", "managers see all")
- Whether Delta Sharing to external consumers is needed

## Step 2 — Choose: OBT vs. thin view layer

Present the trade-off and implement the user's preferred approach (or both):

| Approach | When to use | Trade-off |
|---|---|---|
| **OBT (One Big Table)** | High-QPS BI tools, Tableau/Power BI DirectQuery at scale | Fast reads, expensive writes; re-materialized on schedule |
| **Thin view layer** | Low-latency dashboards, ad-hoc SQL, Databricks SQL | Zero storage cost, join at query time; slower for wide joins |

### OBT — fully denormalized wide table

```sql
CREATE OR REPLACE TABLE catalog.semantic.obt_orders AS
SELECT
  -- Keys
  f.order_line_sk,
  f.order_id,
  -- Customer attributes (flattened from dim_customer)
  dc.customer_id,
  dc.full_name         AS customer_name,
  dc.segment           AS customer_segment,
  dc.city              AS customer_city,
  dc.country_code,
  -- Product attributes
  dp.product_id,
  dp.product_name,
  dp.category,
  dp.subcategory,
  -- Date attributes
  dd.date_actual       AS order_date,
  dd.year              AS order_year,
  dd.quarter           AS order_quarter,
  dd.month_name        AS order_month,
  -- Measures
  f.quantity,
  f.unit_price,
  f.discount_amount,
  f.net_revenue,
  f.cost,
  f.gross_margin,
  -- Derived metrics
  ROUND(f.gross_margin / NULLIF(f.net_revenue, 0) * 100, 2) AS gross_margin_pct,
  f.net_revenue - f.cost AS contribution_margin
FROM catalog.gold.fact_orders f
JOIN catalog.gold.dim_customer dc ON f.customer_sk = dc.customer_sk AND dc.is_current = true
JOIN catalog.gold.dim_product  dp ON f.product_sk  = dp.product_sk  AND dp.is_current = true
JOIN catalog.gold.dim_date     dd ON f.date_sk      = dd.date_sk
COMMENT 'One Big Table: fully denormalized orders for BI DirectQuery';
```

Schedule daily refresh as a Databricks Job task:
```yaml
- task_key: refresh_obt
  sql_task:
    query: "REFRESH TABLE catalog.semantic.obt_orders"
```

### Thin view layer — logical views over Gold tables

```sql
-- Business-named view with derived metrics
CREATE OR REPLACE VIEW catalog.semantic.v_orders AS
SELECT
  f.order_id,
  f.order_line_sk,
  dc.customer_id,
  dc.full_name                                                    AS customer_name,
  dc.segment                                                      AS customer_segment,
  dp.product_name,
  dp.category,
  dd.date_actual                                                  AS order_date,
  f.quantity,
  f.net_revenue,
  f.cost,
  f.gross_margin,
  ROUND(f.gross_margin / NULLIF(f.net_revenue, 0) * 100, 2)      AS gross_margin_pct,
  f.net_revenue - f.cost                                          AS contribution_margin,
  SUM(f.net_revenue) OVER (PARTITION BY dc.customer_id
                           ORDER BY dd.date_actual
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS customer_cumulative_revenue
FROM catalog.gold.fact_orders f
JOIN catalog.gold.dim_customer dc ON f.customer_sk = dc.customer_sk AND dc.is_current = true
JOIN catalog.gold.dim_product  dp ON f.product_sk  = dp.product_sk  AND dp.is_current = true
JOIN catalog.gold.dim_date     dd ON f.date_sk      = dd.date_sk
COMMENT 'Semantic view: orders with business-named columns and derived metrics';
```

## Step 3 — Metrics layer YAML (dbt-semantic-layer-compatible)

```yaml
# semantic/metrics/orders_metrics.yml
semantic_models:
  - name: orders
    description: "Order line item semantic model over fact_orders"
    model: ref('v_orders')  # or 'catalog.semantic.v_orders' for non-dbt
    defaults:
      agg_time_dimension: order_date

    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: customer_segment
        type: categorical
      - name: category
        type: categorical
      - name: country_code
        type: categorical

    measures:
      - name: total_revenue
        agg: sum
        expr: net_revenue
        description: "Total net revenue after discounts"
      - name: total_cost
        agg: sum
        expr: cost
      - name: order_count
        agg: count_distinct
        expr: order_id
      - name: avg_gross_margin_pct
        agg: average
        expr: gross_margin_pct

metrics:
  - name: gross_margin_pct
    label: "Gross Margin %"
    description: "Gross margin as a percentage of net revenue"
    type: ratio
    type_params:
      numerator: total_gross_margin
      denominator: total_revenue
    filter: "{{ Dimension('order_date') }} >= dateadd('day', -90, current_date)"
```

## Step 4 — Row-level security (Unity Catalog)

### Row filter — sales reps see only their region

```sql
-- 1. Create the filter function
CREATE OR REPLACE FUNCTION catalog.security.orders_row_filter(country_code STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('data_admin')          -- admins see all
  OR country_code = (
      SELECT country_code
      FROM catalog.hr.employee_regions
      WHERE email = current_user()
      LIMIT 1
  );

-- 2. Apply to the semantic view / table
ALTER TABLE catalog.semantic.v_orders
SET ROW FILTER catalog.security.orders_row_filter ON (country_code);
```

### Column mask — hide PII from non-privileged users

```sql
CREATE OR REPLACE FUNCTION catalog.security.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('pii_viewers') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@***.***')
  END;

ALTER TABLE catalog.semantic.v_orders
ALTER COLUMN customer_email SET MASK catalog.security.mask_email;
```

## Step 5 — Delta Sharing (external consumers)

```sql
-- Create a share for the semantic layer
CREATE SHARE IF NOT EXISTS orders_semantic_share
  COMMENT 'External share of the orders semantic layer';

ALTER SHARE orders_semantic_share ADD TABLE catalog.semantic.v_orders
  COMMENT 'Orders semantic view — updated daily';

-- Create a recipient (external organization)
CREATE RECIPIENT IF NOT EXISTS partner_analytics
  COMMENT 'Partner BI team';

-- Grant access
GRANT SELECT ON SHARE orders_semantic_share TO RECIPIENT partner_analytics;

-- Retrieve the activation link for the recipient
DESCRIBE RECIPIENT partner_analytics;
```

## Step 6 — BI tool connection strings

### Power BI (DirectQuery via Databricks connector)

```
Server:   <workspace-url>.azuredatabricks.net
HTTP Path: /sql/1.0/warehouses/<warehouse-id>
Catalog:  catalog
Schema:   semantic
Mode:     DirectQuery (for live data) or Import (for OBT)
```

### Tableau (Databricks connector)

```
Server:    <workspace-url>.azuredatabricks.net
Port:      443
HTTP Path: /sql/1.0/warehouses/<warehouse-id>
Database:  catalog
Schema:    semantic
Initial SQL: SET TIME ZONE 'UTC'
```

### Databricks SQL dashboard sample query

```sql
SELECT
  order_month,
  customer_segment,
  SUM(net_revenue)       AS revenue,
  SUM(gross_margin)      AS gross_margin,
  AVG(gross_margin_pct)  AS avg_margin_pct,
  COUNT(DISTINCT order_id) AS orders
FROM catalog.semantic.v_orders
WHERE order_date >= dateadd(MONTH, -3, current_date())
GROUP BY 1, 2
ORDER BY 1 DESC, revenue DESC
```

## Step 7 — Output summary

End with:
- List of all semantic objects created (OBT tables, views, functions, shares)
- Decision record: OBT vs. thin view (or both) and the rationale
- RLS coverage table: which columns/rows are protected, which groups have full access
- Refresh schedule for OBT (if used)
- One-paragraph handoff note for the BI team explaining how to connect and what metrics are available

## Quality bars

- All views and tables live in a dedicated `semantic` schema — never mixed with raw or Gold schemas
- Row filter and column mask functions reference `current_user()` — no hardcoded usernames or roles
- OBT uses `CREATE OR REPLACE TABLE` with a scheduled refresh, not a real-time stream
- Delta Sharing recipients receive views, not direct table access — governance stays in Unity Catalog
- Every derived metric has a `COMMENT` or YAML `description` explaining the business definition
