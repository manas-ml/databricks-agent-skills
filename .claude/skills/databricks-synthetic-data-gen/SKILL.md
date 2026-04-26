---
name: databricks-synthetic-data-gen
description: Generate realistic synthetic test data on Databricks for development, testing, and ML training. Use when the user needs to generate fake data, seed a Delta table with test records, create realistic sample datasets, generate data matching a schema, produce synthetic training data, or populate dev/test environments with data on Databricks.
---

# Databricks Synthetic Data Generation

Generate realistic, schema-consistent synthetic data for development, testing, ML training, and anonymisation on Databricks.

## Step 1 — Clarify before generating

If not already specified, ask once:
- Target table schema (paste `DESCRIBE TABLE` output or column list with types)
- Row count needed
- Realism requirements: random values, or statistically matching a source distribution
- Whether referential integrity between tables is needed (e.g., orders must reference valid customer_ids)
- Whether PII must be anonymised (generate fake names/emails vs. masking real data)

## Step 2 — Schema-driven generation with Faker and Spark

### Single table generation

```python
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from faker import Faker
import random, uuid
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
random.seed(42)

ORDER_SCHEMA = StructType([
    StructField("order_id",    StringType(),     False),
    StructField("customer_id", StringType(),     False),
    StructField("product_id",  StringType(),     False),
    StructField("amount",      DecimalType(18,4),True),
    StructField("status",      StringType(),     False),
    StructField("order_date",  TimestampType(),  False),
    StructField("region",      StringType(),     True),
])

STATUSES = ["OPEN", "CLOSED", "CANCELLED", "RETURNED"]
REGIONS  = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]

def generate_order(_):
    return Row(
        order_id    = str(uuid.uuid4()),
        customer_id = f"CUST-{random.randint(1, 10_000):06d}",
        product_id  = f"PROD-{random.randint(1, 500):04d}",
        amount      = round(random.uniform(10.0, 5000.0), 4),
        status      = random.choices(STATUSES, weights=[30, 50, 15, 5])[0],
        order_date  = fake.date_time_between(start_date="-2y", end_date="now"),
        region      = random.choice(REGIONS),
    )

NUM_ROWS = 1_000_000

orders_df = (
    spark.range(NUM_ROWS)
    .rdd.map(generate_order)
    .toDF(ORDER_SCHEMA)
)

orders_df.write.format("delta").mode("overwrite").saveAsTable("catalog.dev.orders")
print(f"Generated {NUM_ROWS:,} synthetic orders")
```

### Scalable generation using spark.range (no Python UDFs)

For very large datasets, use Spark SQL expressions to avoid serialisation overhead:

```python
from pyspark.sql import functions as F

NUM_ROWS = 10_000_000

orders_df = (
    spark.range(NUM_ROWS).toDF("id")
    .withColumn("order_id",    F.expr("uuid()"))
    .withColumn("customer_id", F.concat(F.lit("CUST-"), F.lpad((F.col("id") % 10000).cast("string"), 6, "0")))
    .withColumn("product_id",  F.concat(F.lit("PROD-"), F.lpad((F.col("id") % 500).cast("string"), 4, "0")))
    .withColumn("amount",      F.round(F.rand(seed=42) * 4990 + 10, 4))
    .withColumn("status",      F.element_at(F.array(F.lit("OPEN"), F.lit("CLOSED"), F.lit("CANCELLED")),
                                            (F.abs(F.hash("order_id")) % 3 + 1).cast("int")))
    .withColumn("order_date",  F.expr("timestamp_seconds(unix_timestamp('2022-01-01') + rand() * 7776000)"))
    .withColumn("region",      F.element_at(F.array(*[F.lit(r) for r in REGIONS]),
                                            (F.abs(F.hash("customer_id")) % 5 + 1).cast("int")))
    .drop("id")
)
```

## Step 3 — Referentially consistent multi-table generation

Generate related tables where foreign keys are valid:

```python
import random
from pyspark.sql import functions as F

# 1. Generate dimension tables first
customers_df = (
    spark.range(10_000).toDF("customer_sk")
    .withColumn("customer_id",   F.concat(F.lit("CUST-"), F.lpad(F.col("customer_sk").cast("string"), 6, "0")))
    .withColumn("full_name",     F.expr("concat(first_names[abs(hash(customer_id)) % size(first_names)], ' ', last_names[abs(hash(customer_id*7)) % size(last_names)])").alias("full_name"))
    # Simpler approach using arrays:
    .withColumn("full_name",     F.expr("concat(element_at(array('Alice','Bob','Carol','David','Emma'), abs(hash(customer_id)) % 5 + 1), ' ', element_at(array('Smith','Jones','Brown','Wilson','Taylor'), abs(hash(customer_id*7)) % 5 + 1))"))
    .withColumn("email",         F.lower(F.concat(F.col("customer_id"), F.lit("@example.com"))))
    .withColumn("segment",       F.element_at(F.array(F.lit("GOLD"), F.lit("SILVER"), F.lit("BRONZE")),
                                              (F.abs(F.hash("customer_id")) % 3 + 1).cast("int")))
    .withColumn("is_current",    F.lit(True))
)
customers_df.write.format("delta").mode("overwrite").saveAsTable("catalog.dev.dim_customer")

# Collect valid customer IDs to use as FK in fact table
customer_ids = [r.customer_id for r in customers_df.select("customer_id").collect()]
customer_ids_broadcast = spark.sparkContext.broadcast(customer_ids)

# 2. Generate fact table using valid FK values
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def random_customer():
    return random.choice(customer_ids_broadcast.value)

orders_df = (
    spark.range(1_000_000)
    .withColumn("order_id",    F.expr("uuid()"))
    .withColumn("customer_id", random_customer())   # guaranteed valid FK
    .withColumn("amount",      F.round(F.rand() * 4990 + 10, 4))
    .withColumn("order_date",  F.expr("timestamp_seconds(unix_timestamp('2022-01-01') + rand() * 63072000)"))
)
orders_df.write.format("delta").mode("overwrite").saveAsTable("catalog.dev.fact_orders")
```

## Step 4 — Distribution-matching generation (statistical realism)

Match the statistical distribution of a real source table without exposing real data:

```python
# Sample source statistics (run on real data, store summary only — no PII)
stats = spark.sql("""
    SELECT
        MIN(amount)    AS min_amt,
        MAX(amount)    AS max_amt,
        AVG(amount)    AS mean_amt,
        STDDEV(amount) AS std_amt,
        percentile_approx(amount, 0.5)  AS p50,
        percentile_approx(amount, 0.95) AS p95
    FROM catalog.prod.orders
""").first()

# Generate amounts matching the real distribution (log-normal approximation)
import numpy as np

mu    = np.log(float(stats.mean_amt))
sigma = 0.8  # tune to match std_amt

synth_amounts = spark.range(1_000_000).withColumn(
    "amount",
    F.round(F.expr(f"exp({mu} + {sigma} * randn())"), 4)
).withColumn(
    "amount",  # clip to observed min/max
    F.least(F.greatest(F.col("amount"), F.lit(float(stats.min_amt))), F.lit(float(stats.max_amt)))
)
```

## Step 5 — Anonymise real data (safer than pure synthetic for ML)

```python
from pyspark.sql import functions as F

def anonymise_customers(real_df):
    return (
        real_df
        # Replace PII with deterministic fake values (same input → same fake output)
        .withColumn("full_name",  F.concat(F.lit("User-"), F.sha2(F.col("customer_id"), 256).substr(1, 8)))
        .withColumn("email",      F.concat(F.sha2(F.col("email"), 256).substr(1, 12), F.lit("@anon.invalid")))
        .withColumn("phone",      F.concat(F.lit("555-"), F.lpad((F.abs(F.hash("customer_id")) % 9000 + 1000).cast("string"), 4, "0")))
        .withColumn("address",    F.lit("123 Fake Street"))
        .withColumn("ssn",        F.lit(None).cast("string"))    # always null — never anonymise SSN, drop it
        .drop("credit_card_number", "bank_account")              # drop columns that cannot be safely anonymised
    )

anon_df = anonymise_customers(spark.table("catalog.prod.customers"))
anon_df.write.format("delta").mode("overwrite").saveAsTable("catalog.dev.customers")
```

## Step 6 — ai_generate for content fields

For text columns (descriptions, notes, reviews), use Databricks AI Functions:

```sql
-- Generate realistic product descriptions using ai_generate
INSERT INTO catalog.dev.products
SELECT
  product_id, product_name, category,
  ai_generate(CONCAT('Write a 30-word product description for: ', product_name, ' in category: ', category)) AS description
FROM catalog.prod.products
WHERE description IS NULL
LIMIT 1000;  -- batch to control cost
```

## Quality bars

- Always set `Faker.seed(42)` and `random.seed(42)` for reproducible datasets across re-runs
- Prefer `spark.range + withColumn` expressions over Python UDFs for large row counts — 10–100x faster
- Never include real PII in dev/test synthetic datasets — use hash-based deterministic anonymisation
- Validate referential integrity after generation: `assert orders_df.join(customers_df, "customer_id", "left_anti").count() == 0`
- Write generated data to Unity Catalog dev catalog, never to prod schema
