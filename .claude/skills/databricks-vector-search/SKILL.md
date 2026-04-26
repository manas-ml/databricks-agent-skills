---
name: databricks-vector-search
description: Create and query Databricks Mosaic AI Vector Search indexes for RAG pipelines and semantic search. Use when the user wants to build a RAG application, semantic search, vector similarity search, hybrid search, or embedding-based retrieval on Databricks using Vector Search indexes synced with Delta tables.
---

# Databricks Vector Search

Create, sync, and query Mosaic AI Vector Search indexes for RAG and semantic search applications on Databricks.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Source Delta table (catalog.schema.table) and the text column to embed
- Embedding model: Databricks-hosted (e.g., `databricks-gte-large-en`) or external (OpenAI, Azure OpenAI)
- Index type: Delta Sync (auto-synced from Delta table) or Direct Vector Access (manual upsert)
- Search type needed: vector-only, hybrid (vector + keyword), or filtered vector search
- Downstream use: RAG chain, semantic dedup, recommendation engine

## Step 2 — Prepare the source Delta table

The source table must have:
1. A primary key column (integer or string, not null)
2. A text column to embed
3. Change Data Feed enabled (required for Delta Sync indexes)

```sql
CREATE TABLE IF NOT EXISTS catalog.rag.documents (
  doc_id      STRING  NOT NULL COMMENT 'Primary key',
  title       STRING,
  content     STRING  NOT NULL COMMENT 'Text to be embedded',
  source_url  STRING,
  doc_type    STRING,
  updated_at  TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT documents_pk PRIMARY KEY (doc_id)
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'   -- required for Delta Sync index
);
```

## Step 3 — Create a Vector Search endpoint

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

# Create endpoint (one endpoint can serve multiple indexes)
vsc.create_endpoint(
    name="rag_endpoint",
    endpoint_type="STANDARD"   # STANDARD | PERFORMANCE_OPTIMIZED
)

# Wait for endpoint to be ready
endpoint = vsc.get_endpoint("rag_endpoint")
print(endpoint)
```

## Step 4 — Create a Delta Sync index (auto-synced from Delta table)

```python
index = vsc.create_delta_sync_index(
    endpoint_name="rag_endpoint",
    index_name="catalog.rag.documents_index",
    source_table_name="catalog.rag.documents",
    pipeline_type="TRIGGERED",          # TRIGGERED (manual sync) | CONTINUOUS (real-time)
    primary_key="doc_id",
    embedding_source_column="content",  # column to embed
    embedding_model_endpoint_name="databricks-gte-large-en"  # hosted embedding model
)

# Trigger a sync to index all existing rows
index.sync()

# Check sync status
print(index.describe())
```

For **Direct Vector Access** (when you manage embeddings externally):

```python
index = vsc.create_direct_vector_access_index(
    endpoint_name="rag_endpoint",
    index_name="catalog.rag.custom_index",
    primary_key="doc_id",
    embedding_dimension=1024,
    embedding_vector_column="embedding"  # pre-computed ARRAY<FLOAT> column
)

# Upsert vectors manually
index.upsert([
    {"doc_id": "1", "content": "...", "embedding": [0.1, 0.2, ...]},
])
```

## Step 5 — Query the index

### Vector similarity search

```python
results = index.similarity_search(
    query_text="How do I configure auto-scaling on Databricks?",
    columns=["doc_id", "title", "content", "source_url"],
    num_results=5
)
for r in results["result"]["data_array"]:
    print(r)
```

### Hybrid search (vector + keyword BM25)

```python
results = index.similarity_search(
    query_text="auto-scaling clusters",
    columns=["doc_id", "title", "content"],
    num_results=5,
    query_type="HYBRID"   # combines semantic + keyword relevance
)
```

### Filtered vector search (metadata pre-filter)

```python
from databricks.vector_search.client import VectorSearchClient

results = index.similarity_search(
    query_text="data governance policy",
    columns=["doc_id", "title", "content", "doc_type"],
    filters={"doc_type": ("==", "policy")},   # pre-filter on metadata column
    num_results=10
)
```

## Step 6 — RAG pipeline (end-to-end)

```python
from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments

vsc    = VectorSearchClient(disable_notice=True)
index  = vsc.get_index("rag_endpoint", "catalog.rag.documents_index")
client = mlflow.deployments.get_deploy_client("databricks")

def rag_query(user_question: str, num_docs: int = 5) -> str:
    # 1. Retrieve relevant chunks
    results = index.similarity_search(
        query_text=user_question,
        columns=["title", "content", "source_url"],
        num_results=num_docs,
        query_type="HYBRID"
    )
    chunks = results["result"]["data_array"]

    # 2. Build context from retrieved documents
    context = "\n\n".join([
        f"Source: {r[2]}\n{r[1]}" for r in chunks
    ])

    # 3. Call LLM with retrieved context
    response = client.predict(
        endpoint="databricks-meta-llama-3-1-70b-instruct",
        inputs={
            "messages": [
                {"role": "system", "content": (
                    "You are a helpful assistant. Answer using only the provided context. "
                    "If the answer is not in the context, say you don't know."
                )},
                {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {user_question}"}
            ],
            "max_tokens": 512
        }
    )
    return response["choices"][0]["message"]["content"]

# Test
answer = rag_query("How do I set up Unity Catalog external locations?")
print(answer)
```

## Step 7 — Keep the index in sync (production)

For `TRIGGERED` pipeline type, add a sync step to the ETL Workflow:

```yaml
# databricks.yml — add after document ingestion task
- task_key: sync_vector_index
  depends_on:
    - task_key: ingest_documents
  notebook_task:
    notebook_path: ./notebooks/sync_vector_index
    base_parameters:
      endpoint_name: "rag_endpoint"
      index_name: "catalog.rag.documents_index"
  job_cluster_key: etl_cluster
```

Sync notebook:

```python
from databricks.vector_search.client import VectorSearchClient

index = VectorSearchClient().get_index(
    endpoint_name=dbutils.widgets.get("endpoint_name"),
    index_name=dbutils.widgets.get("index_name")
)
index.sync()
# Poll until sync completes
import time
while index.describe()["status"]["detailed_state"] != "ONLINE_NO_PENDING_UPDATE":
    time.sleep(10)
    index = VectorSearchClient().get_index(...)
print("Index sync complete")
```

## Quality bars

- Source table must have `delta.enableChangeDataFeed = true` before creating a Delta Sync index
- Use `HYBRID` query type by default — consistently outperforms pure vector search on domain-specific content
- Chunk long documents to 512–1024 tokens before storing in the source table — embedding quality degrades on very long texts
- Store chunk metadata (source URL, document type, section) as filterable columns for pre-filtering
- Use `TRIGGERED` sync for batch pipelines (daily/hourly); `CONTINUOUS` only when sub-minute freshness is required
- Monitor index staleness via `index.describe()["status"]["indexed_row_count"]` vs source table row count
