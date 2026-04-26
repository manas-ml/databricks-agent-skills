---
name: etl-pipeline-test
description: Generate unit tests, integration tests, and schema contract tests for a Databricks PySpark ETL pipeline. Use when the user asks to write tests, add pytest coverage, test a PySpark transformation, validate pipeline logic, or set up a testing framework for a Databricks data pipeline.
---

# ETL Pipeline Test

Generate a complete pytest test suite for a Databricks PySpark ETL pipeline that runs locally without a cluster.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Pipeline stages to test (extract, transform, load — or a specific transformation function)
- Target Delta table schema (paste `DESCRIBE TABLE` output or column list)
- Key business rules or transformations that need coverage
- Whether schema contract tests against live Delta tables are needed (requires Unity Catalog access)

## Step 2 — Generate conftest.py

```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("etl-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def empty_df(spark):
    from pyspark.sql.types import StructType
    return spark.createDataFrame([], StructType([]))
```

## Step 3 — Generate test files per pipeline stage

### Transform tests (golden-dataset pattern)

```python
# tests/test_transform.py
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.testing import assertDataFrameEqual
from src.transform import clean_orders  # import the function under test

SOURCE_SCHEMA = StructType([
    StructField("order_id",   StringType(),  nullable=False),
    StructField("customer_id",StringType(),  nullable=True),
    StructField("amount",     StringType(),  nullable=True),  # raw string from source
    StructField("status",     StringType(),  nullable=True),
])

EXPECTED_SCHEMA = StructType([
    StructField("order_id",   StringType(),     nullable=False),
    StructField("customer_id",StringType(),     nullable=False),
    StructField("amount",     DecimalType(18,4),nullable=True),
    StructField("status",     StringType(),     nullable=False),
])

class TestCleanOrders:

    def test_happy_path(self, spark):
        input_df = spark.createDataFrame([
            ("ORD-001", "CUST-1", "99.99", "OPEN"),
            ("ORD-002", "CUST-2", "150.00", "CLOSED"),
        ], schema=SOURCE_SCHEMA)

        expected = spark.createDataFrame([
            ("ORD-001", "CUST-1", Decimal("99.99"), "OPEN"),
            ("ORD-002", "CUST-2", Decimal("150.00"), "CLOSED"),
        ], schema=EXPECTED_SCHEMA)

        result = clean_orders(input_df)
        assertDataFrameEqual(result, expected, checkRowOrder=False)

    def test_null_customer_id_dropped(self, spark):
        input_df = spark.createDataFrame([
            ("ORD-003", None, "10.00", "OPEN"),
        ], schema=SOURCE_SCHEMA)
        result = clean_orders(input_df)
        assert result.count() == 0, "Rows with null customer_id must be dropped"

    def test_empty_dataframe(self, spark):
        input_df = spark.createDataFrame([], schema=SOURCE_SCHEMA)
        result = clean_orders(input_df)
        assert result.count() == 0
        assert result.schema == EXPECTED_SCHEMA

    def test_duplicate_order_ids_deduplicated(self, spark):
        input_df = spark.createDataFrame([
            ("ORD-001", "CUST-1", "10.00", "OPEN"),
            ("ORD-001", "CUST-1", "10.00", "OPEN"),  # exact duplicate
        ], schema=SOURCE_SCHEMA)
        result = clean_orders(input_df)
        assert result.count() == 1

    @pytest.mark.parametrize("bad_amount", ["not_a_number", "", "NaN", "-999999999999999999999"])
    def test_invalid_amount_handled(self, spark, bad_amount):
        input_df = spark.createDataFrame([
            ("ORD-004", "CUST-1", bad_amount, "OPEN"),
        ], schema=SOURCE_SCHEMA)
        result = clean_orders(input_df)
        # Should not raise; amount should be null or row dropped depending on business rule
        assert result.count() >= 0

    def test_output_schema_matches_contract(self, spark):
        input_df = spark.createDataFrame([("ORD-005", "CUST-1", "1.00", "OPEN")], schema=SOURCE_SCHEMA)
        result = clean_orders(input_df)
        assert result.schema == EXPECTED_SCHEMA, f"Schema mismatch:\n{result.schema}\nvs\n{EXPECTED_SCHEMA}"
```

### Load / upsert tests (mock Delta with temp views)

```python
# tests/test_load.py
from src.load import upsert_to_delta

def test_upsert_inserts_new_records(spark, tmp_path):
    target_path = str(tmp_path / "target")
    # Seed target with one existing record
    existing = spark.createDataFrame([("ORD-001", "CUST-1", 99.99)], ["order_id", "customer_id", "amount"])
    existing.write.format("delta").save(target_path)

    new_data = spark.createDataFrame([("ORD-002", "CUST-2", 50.00)], ["order_id", "customer_id", "amount"])
    upsert_to_delta(new_data, target_path, merge_key="order_id")

    result = spark.read.format("delta").load(target_path)
    assert result.count() == 2

def test_upsert_updates_existing_records(spark, tmp_path):
    target_path = str(tmp_path / "target")
    existing = spark.createDataFrame([("ORD-001", "CUST-1", 99.99)], ["order_id", "customer_id", "amount"])
    existing.write.format("delta").save(target_path)

    updated = spark.createDataFrame([("ORD-001", "CUST-1", 199.99)], ["order_id", "customer_id", "amount"])
    upsert_to_delta(updated, target_path, merge_key="order_id")

    result = spark.read.format("delta").load(target_path)
    assert result.count() == 1
    assert result.first()["amount"] == 199.99

def test_upsert_is_idempotent(spark, tmp_path):
    target_path = str(tmp_path / "target")
    data = spark.createDataFrame([("ORD-001", "CUST-1", 99.99)], ["order_id", "customer_id", "amount"])
    data.write.format("delta").save(target_path)

    upsert_to_delta(data, target_path, merge_key="order_id")
    upsert_to_delta(data, target_path, merge_key="order_id")

    result = spark.read.format("delta").load(target_path)
    assert result.count() == 1, "Idempotent re-run must not create duplicates"
```

### Schema contract tests (against live Unity Catalog tables)

```python
# tests/test_schema_contract.py — run in CI with Databricks Connect or skip locally
import pytest
from pyspark.sql.types import StructField, StringType, DecimalType, TimestampType

EXPECTED_SILVER_ORDERS_SCHEMA = {
    "order_id":    (StringType(),  False),  # (type, nullable)
    "customer_id": (StringType(),  False),
    "amount":      (DecimalType(18, 4), True),
    "status":      (StringType(),  False),
    "_ingested_at":(TimestampType(), False),
}

@pytest.mark.integration
def test_silver_orders_schema_contract(spark):
    actual = {f.name: (f.dataType, f.nullable)
              for f in spark.table("catalog.silver.orders").schema.fields}
    for col, (expected_type, expected_nullable) in EXPECTED_SILVER_ORDERS_SCHEMA.items():
        assert col in actual, f"Column '{col}' missing from silver.orders"
        assert type(actual[col][0]) == type(expected_type), \
            f"Column '{col}' type mismatch: expected {expected_type}, got {actual[col][0]}"
        assert actual[col][1] == expected_nullable, \
            f"Column '{col}' nullable mismatch: expected {expected_nullable}, got {actual[col][1]}"
```

## Step 4 — Project config files

### pyproject.toml

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "integration: marks tests that require a live Databricks connection (deselect with -m 'not integration')",
]
addopts = "--tb=short -q"

[tool.coverage.run]
source = ["src"]
omit = ["tests/*"]

[tool.coverage.report]
fail_under = 80
```

### GitHub Actions test step (insert before bundle deploy in etl-job-productionize workflow)

```yaml
- name: Run unit tests
  run: |
    pip install pyspark==3.5.0 delta-spark==3.1.0 pytest pytest-cov chispa
    pytest tests/ -m "not integration" --cov=src --cov-report=term-missing
```

## Step 5 — Output summary

End with:
- List of every test file generated and test count per file
- Instructions for running locally: `pytest tests/ -m "not integration" -v`
- Instructions for running integration tests against Databricks: `pytest tests/ -m integration --databricks-connect`
- Coverage command: `pytest --cov=src --cov-report=html` and where to find the HTML report

## Quality bars

- All unit tests run locally with no cluster — `SparkSession.builder.master("local[2]")`
- Every transformation function has at minimum: happy path, null input, empty DataFrame, and idempotency tests
- Schema contract tests are marked `@pytest.mark.integration` so they are excluded from unit CI runs
- No test mutates shared fixtures — use `tmp_path` for Delta table tests
