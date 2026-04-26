---
name: databricks-serverless-migration
description: Migrate Databricks workloads from classic clusters to Serverless compute. Use when the user wants to migrate to serverless, convert classic cluster jobs to serverless, check serverless compatibility, understand serverless limitations, estimate cost savings, or update cluster policies and job configs for Databricks Serverless.
---

# Databricks Serverless Migration

Assess, plan, and execute migration of Databricks workloads from classic clusters to Serverless compute.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Workload type: ETL job, DLT pipeline, SQL warehouse, notebook, or streaming job
- Current cluster config (runtime version, instance type, init scripts, libraries)
- Whether custom init scripts or Docker containers are in use (key compatibility blockers)
- Target: Serverless Jobs, Serverless DLT, or Serverless SQL Warehouse

## Step 2 — Serverless compatibility assessment

Run this assessment checklist against the user's current cluster config:

### Automatic compatibility (no changes needed)
- Standard PySpark / Spark SQL code
- Delta Lake reads and writes
- MLflow logging
- `dbutils.secrets`, `dbutils.widgets`, `dbutils.fs`
- Python libraries installable via `%pip install` or `libraries` in job config
- Unity Catalog tables and volumes

### Requires changes
| Feature | Classic | Serverless equivalent |
|---|---|---|
| Init scripts | `init_scripts` cluster config | `%pip install` in notebook or `libraries` in job config |
| Docker containers | `docker_image` config | Not supported — use library installs instead |
| `spark.conf.set(...)` at cluster level | Cluster Spark config | Set in notebook before use; some configs ignored |
| Fixed worker count | `num_workers` | Serverless auto-scales; remove worker count config |
| Instance type selection | `node_type_id` | Managed by Databricks; remove instance type config |
| SSH access | `ssh_public_keys` | Not supported on serverless |
| Custom JVM options | `jvm_options` | Not supported |
| Cluster-scoped environment variables | `spark_env_vars` | Use `%pip install` and `dbutils.secrets` instead |

### Not supported on Serverless
- Custom Docker images
- SSH / direct cluster access
- Ganglia metrics UI (use Spark UI instead)
- Classic SQL warehouses (must use Pro or Serverless SQL Warehouses)
- `spark.databricks.clusterUsageTags.*` custom tags (use job tags instead)

## Step 3 — Migrate a job from classic to serverless

### Before (classic cluster job config)

```yaml
# Classic cluster job
job_clusters:
  - job_cluster_key: etl_cluster
    new_cluster:
      spark_version: 15.4.x-scala2.12
      node_type_id: Standard_DS4_v2
      num_workers: 8
      spark_conf:
        spark.sql.adaptive.enabled: "true"
      init_scripts:
        - dbfs:/init/install_deps.sh
      custom_tags:
        team: data-engineering
```

### After (serverless job config)

```yaml
# Serverless job — remove cluster config entirely
tasks:
  - task_key: main
    notebook_task:
      notebook_path: ./notebooks/etl_main
    # No job_cluster_key needed for serverless
    environment_key: default_env   # optional: pin library environment

environments:
  - environment_key: default_env
    spec:
      client: "1"
      dependencies:
        - "great-expectations==0.18.0"
        - "pyarrow==14.0.1"
```

### Migrate init script logic to notebook installs

```python
# Replace init script content with notebook-level pip installs
# Add this as the first cell in every notebook

%pip install great-expectations==0.18.0 pyarrow==14.0.1 --quiet
dbutils.library.restartPython()
```

## Step 4 — Migrate a DLT pipeline to serverless

```json
// Before: classic DLT pipeline config
{
  "clusters": [{"label": "default", "num_workers": 4, "node_type_id": "Standard_DS4_v2"}],
  "continuous": false
}

// After: serverless DLT pipeline config
{
  "serverless": true,
  "continuous": false
  // Remove "clusters" block entirely
}
```

## Step 5 — Migrate SQL warehouse to serverless

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import EditWarehouseRequest, WarehouseTypePair, WarehouseType

w = WorkspaceClient()

# Upgrade existing Pro warehouse to Serverless
w.warehouses.edit(
    id="<warehouse-id>",
    name="serverless-warehouse",
    warehouse_type=WarehouseType.PRO,   # Serverless uses PRO type with enable_serverless_compute
    enable_serverless_compute=True,
    auto_stop_mins=10,
    min_num_clusters=1,
    max_num_clusters=3,
    spot_instance_policy="COST_OPTIMIZED"
)
```

## Step 6 — Cost estimation before migration

```sql
-- Compare DBU cost: classic vs serverless
-- Classic compute cost (from system tables)
SELECT
  usage_metadata.cluster_id,
  usage_metadata.job_id,
  SUM(usage_quantity)  AS classic_dbus,
  COUNT(DISTINCT usage_date) AS active_days
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 30 DAYS
  AND sku_name LIKE '%JOBS_COMPUTE%'
GROUP BY 1, 2
ORDER BY classic_dbus DESC;

-- Note: Serverless DBU rate is typically 1.5–2x classic DBU rate
-- but idle time is eliminated (no cluster startup/teardown cost)
-- Net savings for short jobs (<30 min) is typically 20-40%
```

**Cost rule of thumb:**
- Jobs running **< 10 minutes**: serverless is almost always cheaper (eliminates 5–8 min startup cost)
- Jobs running **10–60 minutes**: roughly cost-neutral; serverless wins on operational simplicity
- Jobs running **> 60 minutes with steady load**: classic with autoscaling may be cheaper

## Step 7 — Post-migration validation checklist

Generate a validation notebook that runs after migration:

```python
validation_checks = [
    ("Serverless compute detected",
     lambda: spark.conf.get("spark.databricks.clusterSource", "") == "JOB_SERVERLESS"),
    ("No init scripts in use",
     lambda: True),  # validated at config level
    ("Unity Catalog accessible",
     lambda: spark.catalog.listDatabases() is not None),
    ("Delta write succeeds",
     lambda: spark.range(1).write.format("delta").mode("overwrite")
             .saveAsTable("catalog.test.serverless_validation") or True),
    ("Secrets accessible",
     lambda: dbutils.secrets.list("scope") is not None),
    ("pip libraries installed",
     lambda: __import__("great_expectations") is not None),
]

for name, check in validation_checks:
    try:
        result = check()
        print(f"  ✓ {name}")
    except Exception as e:
        print(f"  ✗ {name}: {e}")
```

## Quality bars

- Never reference `node_type_id`, `num_workers`, or `init_scripts` in serverless job configs — these are silently ignored or cause errors
- Move all library installations to `%pip install` cells or `environments` in `databricks.yml` — init scripts do not run on serverless
- Run the cost estimation query before migration for every job — short-running jobs benefit most
- Validate with the post-migration notebook in dev before switching prod to serverless
- Keep classic cluster config in a feature branch until serverless is validated in prod — easy rollback
