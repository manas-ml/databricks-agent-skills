---
name: databricks-model-serving
description: Deploy MLflow models and AI agents to Databricks Model Serving endpoints. Use when the user wants to deploy a model, create a serving endpoint, serve an LLM, set up a custom model wrapper, configure traffic splitting, log inference data, or serve a Pyfunc/LangChain/LlamaIndex agent on Databricks Model Serving.
---

# Databricks Model Serving

Deploy MLflow models and AI agents to scalable REST endpoints on Databricks Model Serving.

## Step 1 — Clarify before generating

If not already specified, ask once:
- What to serve: MLflow registered model, foundation LLM (DBRX, Llama, Mistral), external model (OpenAI/Azure), or custom Pyfunc agent
- Compute type: CPU or GPU; provisioned throughput or pay-per-token
- Traffic requirements: single endpoint or A/B split between model versions
- Inference table logging needed (yes/no)
- Autoscaling range (min/max concurrent requests)

## Step 2 — Log and register a model with MLflow

```python
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

mlflow.set_experiment("/experiments/order_classifier")

with mlflow.start_run() as run:
    # Train model
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)

    # Log metrics and params
    mlflow.log_params(model.get_params())
    mlflow.log_metric("accuracy", accuracy_score(y_test, predictions))

    # Infer and log signature (required for Model Serving)
    signature = infer_signature(X_train, predictions)
    input_example = X_train.iloc[:3]

    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="model",
        signature=signature,
        input_example=input_example,
        registered_model_name="catalog.ml.order_classifier"
    )

# Set model alias for serving
client = mlflow.tracking.MlflowClient()
client.set_registered_model_alias("catalog.ml.order_classifier", "champion", version=1)
```

## Step 3 — Create a serving endpoint

### Via Python SDK

```python
import requests

endpoint_config = {
    "name": "order-classifier-endpoint",
    "config": {
        "served_models": [
            {
                "model_name": "catalog.ml.order_classifier",
                "model_version": "1",
                "workload_size": "Small",   # Small | Medium | Large
                "scale_to_zero_enabled": True,
                "environment_vars": {
                    "MODEL_THRESHOLD": "0.7"
                }
            }
        ],
        "auto_capture_config": {
            "catalog_name": "catalog",
            "schema_name": "ml",
            "table_name_prefix": "order_classifier_inference",
            "enabled": True   # log all requests/responses to Delta table
        }
    }
}

# Create endpoint via REST API
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host  = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

resp = requests.post(
    f"{host}/api/2.0/serving-endpoints",
    headers={"Authorization": f"Bearer {token}"},
    json=endpoint_config
)
print(resp.json())
```

### Via Databricks SDK

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedModelInput, EndpointCoreConfigInput

w = WorkspaceClient()

w.serving_endpoints.create(
    name="order-classifier-endpoint",
    config=EndpointCoreConfigInput(
        served_models=[
            ServedModelInput(
                model_name="catalog.ml.order_classifier",
                model_version="1",
                workload_size="Small",
                scale_to_zero_enabled=True
            )
        ]
    )
)
```

## Step 4 — Serve a foundation LLM (pay-per-token)

```python
# Foundation model endpoints are pre-deployed by Databricks
# Reference them by name in ai_query() or the Deployments API

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

response = client.predict(
    endpoint="databricks-meta-llama-3-1-70b-instruct",
    inputs={
        "messages": [
            {"role": "system", "content": "You are a helpful data engineering assistant."},
            {"role": "user",   "content": "Explain Delta Lake ZORDER in one paragraph."}
        ],
        "max_tokens": 256,
        "temperature": 0.1
    }
)
print(response["choices"][0]["message"]["content"])
```

Available foundation models: `databricks-meta-llama-3-1-70b-instruct`, `databricks-meta-llama-3-1-8b-instruct`, `databricks-dbrx-instruct`, `databricks-mixtral-8x7b-instruct`, `databricks-gte-large-en` (embeddings).

## Step 5 — Custom Pyfunc model wrapper

Use when you need pre/post-processing logic around a model or to wrap a LangChain/LlamaIndex agent:

```python
import mlflow.pyfunc
import pandas as pd

class OrderClassifierWrapper(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        import joblib
        self.model     = joblib.load(context.artifacts["model_path"])
        self.threshold = float(context.model_config.get("threshold", 0.5))

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        proba  = self.model.predict_proba(model_input)[:, 1]
        labels = ["URGENT" if p >= self.threshold else "NORMAL" for p in proba]
        return pd.DataFrame({"label": labels, "confidence": proba})

# Log the wrapper
with mlflow.start_run():
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=OrderClassifierWrapper(),
        artifacts={"model_path": "models/classifier.pkl"},
        model_config={"threshold": 0.7},
        registered_model_name="catalog.ml.order_classifier_v2",
        pip_requirements=["scikit-learn==1.4.0", "joblib"]
    )
```

## Step 6 — A/B traffic splitting between model versions

```python
w.serving_endpoints.update_config(
    name="order-classifier-endpoint",
    served_models=[
        ServedModelInput(
            model_name="catalog.ml.order_classifier",
            model_version="1",
            workload_size="Small",
            scale_to_zero_enabled=True,
            traffic_percentage=80   # send 80% of traffic to v1
        ),
        ServedModelInput(
            model_name="catalog.ml.order_classifier",
            model_version="2",
            workload_size="Small",
            scale_to_zero_enabled=True,
            traffic_percentage=20   # send 20% to v2 (challenger)
        )
    ]
)
```

## Step 7 — Query the endpoint and monitor inference

### Call the endpoint

```python
import requests, json

def predict(endpoint_name: str, payload: dict) -> dict:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host  = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    resp  = requests.post(
        f"{host}/serving-endpoints/{endpoint_name}/invocations",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    resp.raise_for_status()
    return resp.json()

result = predict("order-classifier-endpoint", {"dataframe_records": [{"feature1": 1.2, "feature2": 0.5}]})
```

### Monitor inference table (auto-capture)

```sql
-- Inference table auto-created when auto_capture_config.enabled = true
SELECT
  request_id,
  request:dataframe_records[0] AS input,
  response:predictions[0]      AS prediction,
  databricks_request_id,
  timestamp
FROM catalog.ml.order_classifier_inference_payload
WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
ORDER BY timestamp DESC;

-- Latency p95
SELECT PERCENTILE(response_time_ms, 0.95) AS p95_latency_ms
FROM catalog.ml.order_classifier_inference_payload
WHERE timestamp >= current_date() - INTERVAL 7 DAYS;
```

## Quality bars

- Always log a `signature` and `input_example` when registering models — required for Model Serving validation
- Use `scale_to_zero_enabled=True` for non-production endpoints to eliminate idle costs
- Enable `auto_capture_config` on production endpoints — inference logs are the foundation for model monitoring and drift detection
- Set model alias (`champion` / `challenger`) on the registered model — reference aliases in endpoint config, not version numbers
- Never store API tokens in code — use `dbutils.secrets.get()` or the SDK's default credential chain
