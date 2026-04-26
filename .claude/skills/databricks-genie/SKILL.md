---
name: databricks-genie
description: Create and curate Databricks Genie Spaces for natural language analytics. Use when the user wants to build a Genie Space, enable natural language querying over Delta tables, add curated questions, configure trusted assets, use the Genie Conversation API, or set up an AI/BI natural language interface on Databricks.
---

# Databricks Genie

Create and configure Databricks Genie Spaces that let business users query Delta Lake data in natural language.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Target tables or views to expose in the Genie Space
- Business domain and intended audience (analysts, executives, ops teams)
- Example questions users will ask (drives curated question setup)
- Whether Genie Conversation API integration (chatbot/app embedding) is needed
- Unity Catalog catalog and schema containing the data

## Step 2 — Create a Genie Space via API

```python
import requests

def get_auth_headers():
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host  = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, host

headers, host = get_auth_headers()

# Create the Genie Space
space = requests.post(f"{host}/api/2.0/genie/spaces", headers=headers, json={
    "title": "Sales Analytics Assistant",
    "description": "Ask questions about orders, customers, and revenue in plain English.",
    "warehouse_id": "<sql-warehouse-id>",
    "table_identifiers": [
        "catalog.gold.fact_orders",
        "catalog.gold.dim_customer",
        "catalog.gold.dim_product",
        "catalog.semantic.v_orders"
    ]
}).json()

space_id = space["space_id"]
print(f"Genie Space created: {space_id}")
```

## Step 3 — Add curated questions (trusted Q&A pairs)

Curated questions teach Genie the correct SQL for common business questions. Add the most frequent questions from your audience.

```python
curated_questions = [
    {
        "question": "What is the total revenue for the last 30 days?",
        "sql": """
            SELECT SUM(net_revenue) AS total_revenue_30d
            FROM catalog.gold.fact_orders f
            JOIN catalog.gold.dim_date d ON f.date_sk = d.date_sk
            WHERE d.date_actual >= current_date() - INTERVAL 30 DAYS
        """
    },
    {
        "question": "Who are the top 10 customers by revenue this quarter?",
        "sql": """
            SELECT c.customer_name, SUM(f.net_revenue) AS revenue
            FROM catalog.gold.fact_orders f
            JOIN catalog.gold.dim_customer c ON f.customer_sk = c.customer_sk AND c.is_current = true
            JOIN catalog.gold.dim_date d ON f.date_sk = d.date_sk
            WHERE d.quarter = QUARTER(current_date()) AND d.year = YEAR(current_date())
            GROUP BY c.customer_name
            ORDER BY revenue DESC
            LIMIT 10
        """
    },
    {
        "question": "What is the gross margin percentage by product category?",
        "sql": """
            SELECT
                p.category,
                SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0) * 100 AS gross_margin_pct
            FROM catalog.gold.fact_orders f
            JOIN catalog.gold.dim_product p ON f.product_sk = p.product_sk AND p.is_current = true
            GROUP BY p.category
            ORDER BY gross_margin_pct DESC
        """
    },
    {
        "question": "Show me daily revenue for the last 90 days",
        "sql": """
            SELECT d.date_actual, SUM(f.net_revenue) AS daily_revenue
            FROM catalog.gold.fact_orders f
            JOIN catalog.gold.dim_date d ON f.date_sk = d.date_sk
            WHERE d.date_actual >= current_date() - INTERVAL 90 DAYS
            GROUP BY d.date_actual
            ORDER BY d.date_actual
        """
    }
]

for q in curated_questions:
    resp = requests.post(
        f"{host}/api/2.0/genie/spaces/{space_id}/instructions",
        headers=headers,
        json={"content": f"When the user asks: '{q['question']}', use this SQL:\n```sql\n{q['sql']}\n```"}
    )
    print(f"Added: {q['question'][:60]}... → {resp.status_code}")
```

## Step 4 — Add business context instructions

Business context helps Genie understand domain terminology, metric definitions, and data relationships:

```python
business_context = """
## Data Definitions

- **Net revenue**: gross_margin + cost = net_revenue. Always use the `net_revenue` column from fact_orders.
- **Gross margin %**: gross_margin / net_revenue * 100. Never calculate from unit price alone.
- **Active customer**: a customer with at least one order in the last 12 months.
- **Quarter**: fiscal quarter aligned to calendar (Q1 = Jan-Mar, Q2 = Apr-Jun, Q3 = Jul-Sep, Q4 = Oct-Dec).

## Relationships
- fact_orders joins to dim_customer via customer_sk WHERE is_current = true
- fact_orders joins to dim_product via product_sk WHERE is_current = true
- fact_orders joins to dim_date via date_sk

## Important filters
- Always filter dim_customer and dim_product with is_current = true for current records
- For "this year" use YEAR(date_actual) = YEAR(current_date())
- For "last 30 days" use date_actual >= current_date() - INTERVAL 30 DAYS

## Restricted data
- Never query catalog.silver.customers.ssn or .credit_card columns — use catalog.gold views only
"""

requests.post(
    f"{host}/api/2.0/genie/spaces/{space_id}/instructions",
    headers=headers,
    json={"content": business_context}
)
```

## Step 5 — Genie Conversation API (embed in apps)

Use the Conversation API to embed Genie into a Databricks App, Slack bot, or internal chatbot:

```python
import time

def ask_genie(space_id: str, question: str) -> str:
    headers, host = get_auth_headers()

    # Start a conversation
    conv = requests.post(
        f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation",
        headers=headers,
        json={"content": question}
    ).json()

    conversation_id = conv["conversation_id"]
    message_id      = conv["message_id"]

    # Poll until the response is ready
    for _ in range(30):   # timeout after 60s
        msg = requests.get(
            f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
            headers=headers
        ).json()

        status = msg.get("status")
        if status == "COMPLETED":
            # Return the SQL result or text response
            attachments = msg.get("attachments", [])
            for a in attachments:
                if a.get("type") == "QUERY_RESULT":
                    return a["query_result"]["statement_response"]["result"]["data_array"]
                if a.get("type") == "TEXT":
                    return a["text"]["content"]
            return msg.get("content", "No response")
        elif status == "FAILED":
            return f"Error: {msg.get('error', 'Unknown error')}"
        time.sleep(2)

    return "Timeout waiting for Genie response"

# Usage
result = ask_genie(space_id, "What were the top 5 products by revenue last month?")
print(result)
```

## Step 6 — Databricks App with Genie chat UI

```python
# app.py — Gradio-based chat app deployed on Databricks Apps
import gradio as gr
import requests, time, os

SPACE_ID = os.environ["GENIE_SPACE_ID"]

def chat(message, history):
    return ask_genie(SPACE_ID, message)

demo = gr.ChatInterface(
    fn=chat,
    title="Sales Analytics Assistant",
    description="Ask questions about revenue, customers, and products in plain English.",
    examples=[
        "What is total revenue this quarter?",
        "Show me top 10 customers by revenue",
        "What is the gross margin by product category?"
    ]
)

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=8080)
```

## Quality bars

- Always add curated questions for the top 10 most common business questions before going live — reduces hallucinated SQL significantly
- Provide explicit metric definitions in the business context instructions — Genie uses these to resolve ambiguous column choices
- Use Unity Catalog semantic views (`catalog.semantic.*`) as Genie data sources, not raw Gold tables — column names and COMMENTS are visible to Genie
- Never expose Bronze or Silver tables directly to Genie — always use Gold or Semantic layer views
- Test Genie responses against a golden set of 20 questions before sharing with business users
