---
name: databricks-ai-functions
description: Use Databricks built-in AI SQL functions to add AI capabilities to data pipelines and queries. Use when the user wants to classify, summarize, extract, translate, generate, forecast, or parse documents using ai_classify, ai_summarize, ai_extract, ai_query, ai_forecast, ai_parse_document, or ai_generate in SQL or PySpark on Databricks.
---

# Databricks AI Functions

Generate SQL and PySpark code using Databricks built-in AI Functions to add AI capabilities directly inside data pipelines without managing model endpoints.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Which AI function(s) are needed (classify, summarize, extract, query, forecast, parse_document, generate)
- Input column(s) and their data types
- Target output column name and expected format
- Whether the task runs in a DLT pipeline, batch job, or interactive SQL
- Volume of data (affects cost and latency guidance)

## Step 2 — Function reference and patterns

### ai_classify — categorise text into predefined labels

```sql
-- SQL
SELECT
  review_id,
  review_text,
  ai_classify(review_text, ARRAY('positive', 'negative', 'neutral')) AS sentiment
FROM catalog.silver.product_reviews;

-- PySpark
df = df.withColumn(
    "sentiment",
    expr("ai_classify(review_text, array('positive', 'negative', 'neutral'))")
)
```

Use for: sentiment analysis, topic routing, content moderation, priority classification.

### ai_summarize — generate a concise summary of long text

```sql
SELECT
  ticket_id,
  ai_summarize(description, 100) AS summary_100_words  -- max word count is optional
FROM catalog.silver.support_tickets;
```

Use for: support ticket triage, document indexing, news digests, meeting notes.

### ai_extract — extract structured fields from unstructured text

```sql
SELECT
  doc_id,
  ai_extract(
    raw_text,
    ARRAY('invoice_number', 'vendor_name', 'total_amount', 'due_date')
  ) AS extracted        -- returns a STRUCT with one field per label
FROM catalog.bronze.invoices;

-- Access extracted fields
SELECT doc_id, extracted.invoice_number, extracted.total_amount
FROM above_query;
```

Use for: invoice parsing, resume parsing, contract field extraction, address normalisation.

### ai_query — call any Model Serving endpoint inline in SQL

```sql
-- Call a custom endpoint or foundation model
SELECT
  order_id,
  ai_query(
    'databricks-meta-llama-3-1-70b-instruct',   -- endpoint name
    CONCAT('Classify this order note as URGENT or NORMAL. Note: ', order_notes)
  ) AS priority
FROM catalog.silver.orders;

-- With a structured prompt template
SELECT
  customer_id,
  ai_query(
    'databricks-dbrx-instruct',
    named_struct(
      'messages', ARRAY(
        named_struct('role', 'system', 'content', 'You are a data quality analyst.'),
        named_struct('role', 'user',   'content', CONCAT('Is this address valid? ', address))
      )
    )
  ) AS address_validation
FROM catalog.silver.customers;
```

Use for: custom prompts, multi-turn logic, calling fine-tuned models, complex reasoning tasks.

### ai_forecast — time-series forecasting in SQL

```sql
SELECT * FROM ai_forecast(
  TABLE(SELECT date, store_id, sales FROM catalog.gold.daily_sales),
  horizon    => 30,          -- forecast 30 periods ahead
  time_col   => 'date',
  value_col  => 'sales',
  group_col  => 'store_id',
  frequency  => 'D'          -- D=daily, W=weekly, M=monthly
);
-- Returns: date, store_id, forecast, lower_bound, upper_bound
```

Use for: demand forecasting, capacity planning, revenue projection, inventory optimisation.

### ai_parse_document — extract content from PDFs and images

```sql
-- Files stored in a Unity Catalog Volume
SELECT
  file_path,
  ai_parse_document(
    content,                         -- BINARY column from read_files()
    'extract_text'                   -- mode: extract_text | extract_tables | extract_all
  ) AS parsed
FROM (
  SELECT path AS file_path, content
  FROM read_files('dbfs:/Volumes/catalog/schema/vol/invoices/', format='binaryFile')
);
```

Use for: PDF invoice processing, scanned document OCR, image-to-text extraction, table extraction from reports.

### ai_generate — open-ended text generation

```sql
SELECT
  product_id,
  product_name,
  ai_generate(
    CONCAT('Write a 50-word product description for: ', product_name,
           '. Category: ', category, '. Key features: ', features)
  ) AS generated_description
FROM catalog.gold.products
WHERE description IS NULL;
```

Use for: content generation, data augmentation, synthetic descriptions, email drafts.

## Step 3 — GenAI ETL pipeline pattern (batch)

For large-scale AI function usage in production, wrap in a DLT pipeline or batch job with checkpointing:

```python
# DLT Silver table — enrich with AI classification
import dlt
from pyspark.sql.functions import expr

@dlt.table(
    comment="Support tickets enriched with AI classification",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_classification", "sentiment IS NOT NULL")
def silver_tickets_enriched():
    return (
        dlt.read_stream("bronze_tickets")
        .withColumn("sentiment",    expr("ai_classify(body, array('billing','technical','general'))"))
        .withColumn("summary",      expr("ai_summarize(body, 50)"))
        .withColumn("urgency",      expr("ai_classify(body, array('urgent','normal','low'))"))
        .withColumn("_enriched_at", expr("current_timestamp()"))
    )
```

## Step 4 — Cost and performance guidance

| Function | Relative cost | Latency | Recommended batch size |
|---|---|---|---|
| `ai_classify` | Low | ~100ms/row | Up to 100K rows/run |
| `ai_summarize` | Medium | ~300ms/row | Up to 50K rows/run |
| `ai_extract` | Medium | ~300ms/row | Up to 50K rows/run |
| `ai_query` | High (depends on model) | ~500ms–2s/row | Up to 10K rows/run |
| `ai_forecast` | Medium | Batch | Full dataset at once |
| `ai_parse_document` | High | ~1–3s/doc | Up to 5K docs/run |

**Cost controls:**
- Process only new/unprocessed rows using incremental load patterns (watermark or Auto Loader)
- Cache `ai_query` results in a Delta table — never re-call for already-processed rows
- Use `ai_classify` over `ai_query` when labels are fixed — 5–10x cheaper
- Monitor spend via `system.billing.usage` filtering on `sku_name LIKE '%FOUNDATION_MODEL%'`

## Quality bars

- Always persist AI function results to a Delta table — never recalculate on every query
- Wrap DLT AI enrichment tables with `@dlt.expect_or_drop` to catch null/malformed model outputs
- Use `TRY()` wrapper in SQL for fault tolerance: `TRY(ai_classify(col, array(...)))` returns NULL instead of failing on error
- Process incrementally — AI functions are charged per token; full table scans are expensive
