# Module 07: Delta Lake Fundamentals & Modern Lakehouse

**Delta Lake** is an open-source storage framework that brings **ACID transactions**, **Time Travel**, and **Data Reliability** to Apache Spark on cloud object storage.

---

## 1. Why Traditional Data Lakes Fail

Without a transactional storage layer:
- **No ACID guarantees**: If a Spark write job fails midway, partial dirty files corrupt the table.
- **No Concurrent Writes**: Simultaneous readers and writers cause race conditions and phantom reads.
- **Slow Updates & Deletes**: Modifying a single row requires rewriting an entire partition or table.
- **Small Files Problem**: Frequent streaming writes create millions of tiny files.

---

## 2. The Delta Transaction Log (`_delta_log`)

Delta Lake stores records as standard Apache Parquet files, accompanied by an ordered JSON-based transaction log directory called `_delta_log/`:

```
my_delta_table/
├── _delta_log/
│   ├── 00000000000000000000.json   # Commit 0
│   ├── 00000000000000000001.json   # Commit 1
│   └── 00000000000000000010.checkpoint.parquet # State snapshot
├── part-0000-commit0.parquet
└── part-0001-commit1.parquet
```

Every atomic commit logs which parquet files were added and which were logically deleted.

---

## 3. Core Delta Lake Operations in PySpark

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip, DeltaTable

# 1. Initialize SparkSession with Delta Lake extensions
builder = SparkSession.builder \
    .appName("DeltaDemo") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 2. Write DataFrame as Delta Table
data = [(1, "Alice", 100), (2, "Bob", 200)]
df = spark.createDataFrame(data, ["id", "name", "credits"])
df.write.format("delta").mode("overwrite").save("delta/users")

# 3. Read Delta Table
delta_df = spark.read.format("delta").load("delta/users")
delta_df.show()
```

---

## 4. Upserts (CDC) with `MERGE INTO`

Change Data Capture (CDC) feeds require updating existing rows and inserting new ones:

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "delta/users")

# Incoming updates: Alice gets 150 credits, Charlie is a new user
updates_data = [(1, "Alice", 150), (3, "Charlie", 300)]
updates_df = spark.createDataFrame(updates_data, ["id", "name", "credits"])

# Atomic Merge
delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(set={
    "credits": "source.credits"
}).whenNotMatchedInsert(values={
    "id": "source.id",
    "name": "source.name",
    "credits": "source.credits"
}).execute()

delta_table.toDF().show()
```

---

## 5. Time Travel & Rollbacks

Query previous snapshots of the table by commit version or timestamp:

```python
# 1. Query by version
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("delta/users")

# 2. Query by timestamp
df_yesterday = spark.read.format("delta").option("timestampAsOf", "2026-09-16").load("delta/users")

# 3. View table transaction history
delta_table.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
```

---

## 6. Table Optimization (`OPTIMIZE` and `Z-ORDER`)

```python
# Compaction: Merges small files into 1GB files
# Z-Order: Colocates multidimensional data to maximize data skipping during queries
spark.sql("OPTIMIZE delta.`delta/users` ZORDER BY (id)")
```
