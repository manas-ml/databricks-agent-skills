---
name: schema-evolution-manager
description: Manage schema changes, migrations, and drift detection for Delta Lake tables on Databricks. Use when the user needs to add columns, handle a breaking schema change, migrate a table schema safely, version-control DDL changes, detect schema drift from a source system, or evolve a Delta table without data loss.
---

# Schema Evolution Manager

Detect schema drift, classify changes by safety level, and generate versioned migration scripts for Delta Lake tables on Databricks.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Source of schema truth: JDBC `INFORMATION_SCHEMA`, Kafka Schema Registry (Avro/JSON), another Delta table, or a manually provided new schema
- Target Delta table to migrate (`catalog.schema.table`)
- Whether the table is used by downstream consumers (determines whether breaking changes need a versioned view)
- Whether Auto Loader is reading the source (determines `mergeSchema` guard requirements)

## Step 2 — Drift detection

Compare the live source schema to the current Delta table schema and classify every difference:

```python
from pyspark.sql.types import StructType, StructField
from pyspark.sql import functions as F

def detect_schema_drift(source_schema: StructType, target_table: str) -> dict:
    target_schema = spark.table(target_table).schema
    source_fields = {f.name: f for f in source_schema.fields}
    target_fields = {f.name: f for f in target_schema.fields}

    changes = {"additive_safe": [], "non_breaking": [], "breaking": [], "removed": []}

    for name, sf in source_fields.items():
        if name not in target_fields:
            changes["additive_safe"].append(f"ADD COLUMN {name} {sf.dataType.simpleString()}")
        else:
            tf = target_fields[name]
            if type(sf.dataType) != type(tf.dataType):
                if _is_type_widening(tf.dataType, sf.dataType):
                    changes["non_breaking"].append(f"WIDEN {name}: {tf.dataType} → {sf.dataType}")
                else:
                    changes["breaking"].append(f"TYPE CHANGE {name}: {tf.dataType} → {sf.dataType} (BREAKING)")
            if tf.nullable == False and sf.nullable == True:
                changes["non_breaking"].append(f"RELAX NULLABLE {name}: NOT NULL → nullable")

    for name in target_fields:
        if name not in source_fields and not name.startswith("_"):
            changes["removed"].append(f"DROPPED in source: {name}")

    return changes

def _is_type_widening(from_type, to_type) -> bool:
    widening_map = {
        "IntegerType": ["LongType", "FloatType", "DoubleType", "DecimalType"],
        "LongType":    ["DecimalType", "DoubleType"],
        "FloatType":   ["DoubleType"],
        "DateType":    ["TimestampType"],
    }
    return type(to_type).__name__ in widening_map.get(type(from_type).__name__, [])
```

Print a drift report before applying any migration:
```
[ADDITIVE-SAFE]  ADD COLUMN phone_number STRING          → safe to apply
[NON-BREAKING]   WIDEN order_id: INT → BIGINT            → requires ALTER TABLE
[BREAKING]       TYPE CHANGE status: STRING → INT        → requires table recreation
[REMOVED]        legacy_code dropped in source           → no action (tombstone recommended)
```

## Step 3 — Versioned migration scripts

Use numbered migration files committed to git. Generate a `schema_migrations` tracking table and an executor:

```sql
-- migrations/V001__add_phone_column.sql
ALTER TABLE catalog.silver.orders ADD COLUMN phone_number STRING COMMENT 'Added from CRM v2 schema';

-- migrations/V002__widen_order_id.sql
ALTER TABLE catalog.silver.orders CHANGE COLUMN order_id order_id BIGINT;

-- migrations/V003__rename_status_col.sql
ALTER TABLE catalog.silver.orders RENAME COLUMN legacy_status TO status;
```

```sql
-- Control table
CREATE TABLE IF NOT EXISTS catalog.observability.schema_migrations (
  version        INT       NOT NULL COMMENT 'Migration version number',
  script_name    STRING    NOT NULL COMMENT 'Filename of the migration script',
  target_table   STRING    NOT NULL,
  checksum       STRING    COMMENT 'MD5 of script content to detect tampering',
  applied_at     TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  applied_by     STRING    DEFAULT current_user(),
  CONSTRAINT schema_migrations_pk PRIMARY KEY (version, target_table)
)
USING DELTA;
```

Migration executor (run once per deploy):

```python
import hashlib, os

def run_migrations(migration_dir: str, target_table: str):
    applied = {r.version for r in
               spark.table("catalog.observability.schema_migrations")
               .filter(f"target_table = '{target_table}'").collect()}

    scripts = sorted(
        f for f in os.listdir(migration_dir) if f.endswith(".sql")
    )
    for script in scripts:
        version = int(script.split("__")[0].lstrip("V"))
        if version in applied:
            print(f"  SKIP V{version:03d} — already applied")
            continue
        path = os.path.join(migration_dir, script)
        sql  = open(path).read()
        chk  = hashlib.md5(sql.encode()).hexdigest()
        print(f"  APPLY V{version:03d} {script}")
        for statement in sql.split(";"):
            if statement.strip():
                spark.sql(statement)
        spark.sql(f"""
          INSERT INTO catalog.observability.schema_migrations
          VALUES ({version}, '{script}', '{target_table}', '{chk}', current_timestamp(), current_user())
        """)
```

## Step 4 — Safe migration patterns per change class

### Additive-safe: new nullable column

```sql
ALTER TABLE catalog.silver.orders ADD COLUMN phone_number STRING COMMENT 'Source: CRM v2';
-- No data backfill needed — existing rows get NULL automatically
-- Update Auto Loader schema hint if applicable:
-- .option("cloudFiles.schemaHints", "phone_number STRING")
```

### Non-breaking: column rename (Delta 2.0+, Column Mapping required)

```sql
ALTER TABLE catalog.silver.orders SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE catalog.silver.orders RENAME COLUMN legacy_status TO status;
-- WARNING: enable columnMapping on ALL downstream readers before running this
```

### Non-breaking: type widening (Delta 3.0+ type widening feature)

```sql
ALTER TABLE catalog.silver.orders SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
ALTER TABLE catalog.silver.orders CHANGE COLUMN order_id order_id BIGINT;
```

### Breaking: incompatible type change — full CLONE migration

```python
def breaking_migration(table: str, new_schema: StructType, transform_fn):
    backup = f"{table}_backup_{spark.sql('SELECT current_date()').first()[0].strftime('%Y%m%d')}"
    # 1. Shallow clone existing table as backup
    spark.sql(f"CREATE TABLE {backup} SHALLOW CLONE {table}")
    # 2. Read existing data, apply type cast transformation
    existing = spark.table(table)
    migrated = transform_fn(existing)  # caller provides the cast logic
    # 3. Overwrite with new schema
    migrated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"Migration complete. Backup at {backup}. Verify then drop backup.")
```

### Removed column: tombstone approach (Data Vault / audit tables)

```sql
-- Do NOT drop the column — mark it deprecated via a comment
ALTER TABLE catalog.silver.orders CHANGE COLUMN legacy_code legacy_code STRING COMMENT 'DEPRECATED: removed from source 2024-Q3. Kept for audit trail.';
```

## Step 5 — Auto Loader mergeSchema guard

Add to Auto Loader pipelines to prevent silent type coercion from a changed source:

```python
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")   # "rescue" puts unexpected cols into _rescued_data
    .option("cloudFiles.schemaHints", "order_id BIGINT, amount DECIMAL(18,4)")
    .load(source_path)
)
# Monitor rescued data — any non-null _rescued_data means schema drifted
```

## Step 6 — Schema contract YAML (commit to git, diff in CI)

Generate a `schema_contract.yml` per table:

```yaml
# schema_contracts/catalog.silver.orders.yml
table: catalog.silver.orders
version: 3
columns:
  - name: order_id
    type: BIGINT
    nullable: false
    description: "Business key from source OMS"
  - name: amount
    type: DECIMAL(18,4)
    nullable: true
  - name: status
    type: STRING
    nullable: false
    allowed_values: ["OPEN", "CLOSED", "CANCELLED"]
```

GitHub Actions CI step to detect drift before deploy:

```yaml
- name: Detect schema drift
  run: |
    python scripts/check_schema_contract.py \
      --contract schema_contracts/catalog.silver.orders.yml \
      --source-jdbc "${{ secrets.JDBC_URL }}" \
      --source-table dbo.orders
```

## Quality bars

- Migration scripts are idempotent — the `schema_migrations` table prevents double-application
- Breaking migrations always create a `SHALLOW CLONE` backup before overwriting
- Column mapping mode is enabled before any `RENAME COLUMN` — never rename without it on Delta 2.x+
- `cloudFiles.schemaEvolutionMode = "rescue"` used instead of `"addNewColumns"` when strict drift detection is needed
- Schema contract YAML is version-controlled and diffed in CI before every bundle deploy
