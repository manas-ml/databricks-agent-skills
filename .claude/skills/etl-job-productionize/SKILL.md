---
name: etl-job-productionize
description: Convert a Databricks notebook or PySpark script into a production-grade Databricks Job. Use when the user wants to productionize, schedule, deploy, or operationalize an ETL notebook or script on Databricks, or needs a Databricks Asset Bundle config, job YAML, CI/CD pipeline, or Workflow definition.
---

# ETL Job Productionize

Convert a Databricks notebook or PySpark script into a fully production-ready Databricks Job with scheduling, parameterization, cluster config, alerting, and CI/CD deployment.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Notebook path or script being productionized
- Runtime parameters the job needs (date ranges, environment, target tables)
- Schedule: cron expression or event-driven trigger
- Environment targets: dev / staging / prod
- Existing cluster policy or compute preference (job clusters vs. instance pools)
- Alerting: email, Slack webhook, or PagerDuty on failure

## Step 2 — Parameterize the notebook

Replace all hardcoded values with `dbutils.widgets`:

```python
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("target_catalog", "dev_catalog", "Target Catalog")
dbutils.widgets.text("load_date", "", "Load Date (YYYY-MM-DD, blank = yesterday)")
dbutils.widgets.dropdown("load_mode", "upsert", ["full", "append", "upsert"], "Load Mode")

env          = dbutils.widgets.get("env")
target_catalog = dbutils.widgets.get("target_catalog")
load_date    = dbutils.widgets.get("load_date") or (date.today() - timedelta(days=1)).isoformat()
load_mode    = dbutils.widgets.get("load_mode")
```

Rules:
- Every parameter must have a safe default for interactive notebook runs
- Secrets (passwords, tokens) go in `dbutils.secrets.get("scope", "key")` — never as widget defaults
- Add a `# PARAMETER:` comment above each widget explaining valid values

## Step 3 — Generate databricks.yml (Asset Bundle)

```yaml
bundle:
  name: <pipeline_name>

variables:
  env:
    default: dev
  target_catalog:
    default: dev_catalog

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: ${var.workspace_host}
  prod:
    mode: production
    workspace:
      host: ${var.workspace_host}

resources:
  jobs:
    <job_name>:
      name: "[${bundle.target}] <Job Display Name>"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: UTC
        pause_status: UNPAUSED

      tasks:
        - task_key: main
          notebook_task:
            notebook_path: ./notebooks/<notebook_name>
            base_parameters:
              env: ${var.env}
              target_catalog: ${var.target_catalog}
              load_date: ""
              load_mode: upsert
          job_cluster_key: etl_cluster
          libraries:
            - pypi:
                package: delta-spark==3.1.0

      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 4
            autoscale:
              min_workers: 2
              max_workers: 8
            spark_conf:
              spark.sql.adaptive.enabled: "true"
              spark.sql.adaptive.coalescePartitions.enabled: "true"
              spark.databricks.delta.optimizeWrite.enabled: "true"
            azure_attributes:
              availability: SPOT_WITH_FALLBACK_AZURE
              first_on_demand: 1

      email_notifications:
        on_failure:
          - <alert_email>
        on_success: []

      max_concurrent_runs: 1
      max_retries: 2
      min_retry_interval_millis: 300000
```

Adjust `node_type_id` and `availability` for AWS (`SPOT_WITH_FALLBACK`) or GCP equivalents.

## Step 4 — CI/CD pipeline (GitHub Actions)

```yaml
# .github/workflows/deploy-etl.yml
name: Deploy ETL Job

on:
  push:
    branches: [main]
  workflow_dispatch:
    inputs:
      environment:
        type: choice
        options: [dev, prod]
        default: dev

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: ${{ github.event.inputs.environment || 'dev' }}
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Validate bundle
        run: databricks bundle validate --target ${{ github.event.inputs.environment || 'dev' }}
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

      - name: Deploy bundle
        run: databricks bundle deploy --target ${{ github.event.inputs.environment || 'dev' }}
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

      - name: Run job (dev only)
        if: github.event.inputs.environment == 'dev' || github.ref == 'refs/heads/main'
        run: databricks bundle run <job_name> --target dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

## Step 5 — Multi-task job (if notebook has dependencies)

If the pipeline has multiple stages, generate a multi-task job:

```yaml
tasks:
  - task_key: extract
    notebook_task:
      notebook_path: ./notebooks/extract
    job_cluster_key: etl_cluster

  - task_key: transform
    depends_on:
      - task_key: extract
    notebook_task:
      notebook_path: ./notebooks/transform
    job_cluster_key: etl_cluster

  - task_key: load
    depends_on:
      - task_key: transform
    notebook_task:
      notebook_path: ./notebooks/load
    job_cluster_key: etl_cluster
```

## Step 6 — Output checklist

End with a deployment checklist:

- [ ] Widgets added for all hardcoded values
- [ ] Secrets moved to `dbutils.secrets` scope
- [ ] `databricks.yml` validated with `databricks bundle validate`
- [ ] Job deployed to dev and triggered successfully
- [ ] Email alert fires on a test failure
- [ ] GitHub Actions secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) configured
- [ ] `max_concurrent_runs: 1` set to prevent overlapping runs
- [ ] Schedule paused in dev (`pause_status: PAUSED`), active in prod

## Quality bars

- No hardcoded environment values, table names, or credentials in notebook code
- Job cluster (not all-purpose cluster) used for production runs to control cost
- Spot instances configured with fallback to on-demand for cost savings
- `max_concurrent_runs: 1` always set for ETL jobs to prevent duplicate writes
- Bundle targets (dev/prod) differ only in variable values, not in job structure
