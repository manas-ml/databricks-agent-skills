---
name: databricks-aibi-dashboards
description: Build Databricks AI/BI dashboards (Lakeview) with datasets, visualisations, and metrics. Use when the user wants to create a Databricks dashboard, Lakeview dashboard, AI/BI dashboard, build charts or visualisations over Delta tables, publish a BI dashboard, define dashboard metrics, or use the Databricks Dashboards API.
---

# Databricks AI/BI Dashboards (Lakeview)

Create, publish, and manage Databricks AI/BI dashboards (Lakeview) backed by Delta Lake tables.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Source tables or views (catalog.schema.table) to visualise
- Key metrics to display (revenue, counts, ratios, trends)
- Visualisation types needed (time series, bar chart, pie chart, table, counter/KPI)
- Audience: internal analysts, executives, external stakeholders
- Whether the dashboard needs to be embedded in a Databricks App or shared externally

## Step 2 — Create a dashboard via API

```python
import requests, json

def get_auth(host_key="apiUrl"):
    ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    token = ctx.apiToken().get()
    host  = ctx.apiUrl().get()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, host

headers, host = get_auth()

# Create the dashboard
dashboard = requests.post(
    f"{host}/api/2.0/lakeview/dashboards",
    headers=headers,
    json={
        "display_name": "Sales Performance Dashboard",
        "warehouse_id": "<sql-warehouse-id>",
        "serialized_dashboard": json.dumps({
            "pages": [
                {
                    "name": "Sales Overview",
                    "layout": []   # populated in Step 3
                }
            ]
        })
    }
).json()

dashboard_id = dashboard["dashboard_id"]
print(f"Dashboard created: {dashboard_id}")
print(f"Edit URL: {host}/dashboardsv3/{dashboard_id}")
```

## Step 3 — Define datasets (SQL queries backing the visualisations)

```python
datasets = [
    {
        "name": "daily_revenue",
        "display_name": "Daily Revenue (last 90 days)",
        "query": """
            SELECT
                d.date_actual      AS order_date,
                SUM(f.net_revenue) AS revenue,
                SUM(f.gross_margin) AS gross_margin,
                COUNT(DISTINCT f.order_id) AS order_count
            FROM catalog.gold.fact_orders f
            JOIN catalog.gold.dim_date d ON f.date_sk = d.date_sk
            WHERE d.date_actual >= current_date() - INTERVAL 90 DAYS
            GROUP BY d.date_actual
            ORDER BY d.date_actual
        """
    },
    {
        "name": "revenue_by_segment",
        "display_name": "Revenue by Customer Segment",
        "query": """
            SELECT
                c.segment,
                SUM(f.net_revenue)  AS revenue,
                COUNT(DISTINCT f.order_id) AS orders,
                ROUND(SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0) * 100, 1) AS margin_pct
            FROM catalog.gold.fact_orders f
            JOIN catalog.gold.dim_customer c ON f.customer_sk = c.customer_sk AND c.is_current = true
            WHERE f.date_sk >= CAST(DATE_FORMAT(current_date() - INTERVAL 30 DAYS, 'yyyyMMdd') AS INT)
            GROUP BY c.segment
            ORDER BY revenue DESC
        """
    },
    {
        "name": "top_products",
        "display_name": "Top 10 Products by Revenue",
        "query": """
            SELECT
                p.product_name,
                p.category,
                SUM(f.net_revenue) AS revenue,
                SUM(f.quantity)    AS units_sold
            FROM catalog.gold.fact_orders f
            JOIN catalog.gold.dim_product p ON f.product_sk = p.product_sk AND p.is_current = true
            WHERE f.date_sk >= CAST(DATE_FORMAT(current_date() - INTERVAL 30 DAYS, 'yyyyMMdd') AS INT)
            GROUP BY p.product_name, p.category
            ORDER BY revenue DESC
            LIMIT 10
        """
    },
    {
        "name": "kpi_summary",
        "display_name": "KPI Summary",
        "query": """
            SELECT
                SUM(f.net_revenue)                                    AS total_revenue,
                SUM(f.net_revenue) - LAG(SUM(f.net_revenue)) OVER () AS revenue_delta,
                COUNT(DISTINCT f.order_id)                            AS total_orders,
                ROUND(AVG(f.net_revenue), 2)                          AS avg_order_value,
                ROUND(SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue),0) * 100, 1) AS gross_margin_pct
            FROM catalog.gold.fact_orders f
            JOIN catalog.gold.dim_date d ON f.date_sk = d.date_sk
            WHERE d.date_actual >= current_date() - INTERVAL 30 DAYS
        """
    }
]
```

## Step 4 — Dashboard layout definition (Lakeview JSON)

```python
dashboard_spec = {
    "pages": [
        {
            "name": "Sales Overview",
            "layout": [
                # Row 1: KPI counters
                {
                    "widget": {
                        "type": "counter",
                        "title": "Total Revenue (30d)",
                        "dataset": "kpi_summary",
                        "field": "total_revenue",
                        "value_format": "$,.0f"
                    },
                    "position": {"x": 0, "y": 0, "width": 3, "height": 2}
                },
                {
                    "widget": {
                        "type": "counter",
                        "title": "Total Orders (30d)",
                        "dataset": "kpi_summary",
                        "field": "total_orders",
                        "value_format": ",.0f"
                    },
                    "position": {"x": 3, "y": 0, "width": 3, "height": 2}
                },
                {
                    "widget": {
                        "type": "counter",
                        "title": "Avg Order Value",
                        "dataset": "kpi_summary",
                        "field": "avg_order_value",
                        "value_format": "$,.2f"
                    },
                    "position": {"x": 6, "y": 0, "width": 3, "height": 2}
                },
                {
                    "widget": {
                        "type": "counter",
                        "title": "Gross Margin %",
                        "dataset": "kpi_summary",
                        "field": "gross_margin_pct",
                        "value_format": ",.1f%"
                    },
                    "position": {"x": 9, "y": 0, "width": 3, "height": 2}
                },
                # Row 2: Revenue trend line chart
                {
                    "widget": {
                        "type": "line",
                        "title": "Daily Revenue — Last 90 Days",
                        "dataset": "daily_revenue",
                        "x_field": "order_date",
                        "y_fields": ["revenue", "gross_margin"],
                        "x_label": "Date",
                        "y_label": "Amount (USD)"
                    },
                    "position": {"x": 0, "y": 2, "width": 8, "height": 4}
                },
                # Row 2: Revenue by segment bar chart
                {
                    "widget": {
                        "type": "bar",
                        "title": "Revenue by Customer Segment",
                        "dataset": "revenue_by_segment",
                        "x_field": "segment",
                        "y_field": "revenue",
                        "orientation": "horizontal"
                    },
                    "position": {"x": 8, "y": 2, "width": 4, "height": 4}
                },
                # Row 3: Top products table
                {
                    "widget": {
                        "type": "table",
                        "title": "Top 10 Products by Revenue",
                        "dataset": "top_products",
                        "columns": ["product_name", "category", "revenue", "units_sold"]
                    },
                    "position": {"x": 0, "y": 6, "width": 12, "height": 4}
                }
            ]
        }
    ]
}
```

## Step 5 — Publish and share the dashboard

```python
# Update the dashboard with the full spec
requests.patch(
    f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}",
    headers=headers,
    json={"serialized_dashboard": json.dumps(dashboard_spec)}
)

# Publish the dashboard (makes it visible to shared users)
requests.post(
    f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
    headers=headers,
    json={"warehouse_id": "<sql-warehouse-id>", "embed_credentials": False}
)

# Get the published URL
pub = requests.get(
    f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
    headers=headers
).json()
print(f"Published dashboard: {pub.get('embed_url', host + '/dashboardsv3/' + dashboard_id)}")
```

## Step 6 — Scheduled refresh

```python
# Add a scheduled refresh job to keep the dashboard data current
requests.post(
    f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/schedules",
    headers=headers,
    json={
        "cron_schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC"},
        "pause_status": "UNPAUSED"
    }
)
```

## Step 7 — Validate SQL with dry-run before publishing

Always validate dataset SQL before publishing to avoid broken dashboards:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

w = WorkspaceClient()

def validate_sql(query: str, warehouse_id: str) -> bool:
    stmt = w.statement_execution.execute_statement(
        statement=f"EXPLAIN {query}",
        warehouse_id=warehouse_id,
        wait_timeout="30s"
    )
    if stmt.status.state == StatementState.SUCCEEDED:
        print("  ✓ SQL valid")
        return True
    else:
        print(f"  ✗ SQL error: {stmt.status.error.message}")
        return False

for ds in datasets:
    print(f"Validating: {ds['display_name']}")
    validate_sql(ds["query"], warehouse_id="<sql-warehouse-id>")
```

## Quality bars

- Always validate every dataset SQL query with `EXPLAIN` before publishing — broken SQL causes blank widgets with no visible error
- Use Unity Catalog semantic views (`catalog.semantic.*`) as data sources — column names and COMMENTS improve dashboard readability
- Set `embed_credentials: False` when publishing for internal users — they authenticate via their own Databricks identity
- Schedule dashboard refresh to match the ETL cadence — a dashboard that refreshes hourly backed by a daily ETL load misleads users
- KPI counter widgets must reference a single-row aggregate query — multi-row results render only the first row silently
