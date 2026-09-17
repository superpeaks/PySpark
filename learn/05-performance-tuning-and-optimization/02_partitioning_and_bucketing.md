# Module 05: Partitioning, Coalesce & Bucketing

Partitioning dictates how Spark parallelizes computations across CPU cores and how data is structured on disk.

---

## 1. `repartition()` vs `coalesce()`

This is one of the most common PySpark interview questions.

| Feature | `repartition(N)` | `coalesce(N)` |
| :--- | :--- | :--- |
| **Shuffle?** | **Full Shuffle** (data redistributed across cluster) | **No Full Shuffle** (merges adjacent partitions) |
| **Use Case** | Can **increase** or **decrease** partition count | Can **ONLY decrease** partition count |
| **Data Distribution**| Evenly balanced partitions (avoids skew) | May result in uneven partition sizes |
| **Performance** | Expensive (network I/O) | Very Fast |

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Partitions").getOrCreate()
df = spark.range(1, 1000000)

print("Default Partitions:", df.rdd.getNumPartitions())

# 1. repartition(): Full shuffle, increases parallelism
df_repart = df.repartition(16)
print("After repartition:", df_repart.rdd.getNumPartitions())

# 2. coalesce(): No full shuffle, reduces partitions (e.g. before saving to avoid small files)
df_coalesced = df_repart.coalesce(2)
print("After coalesce:", df_coalesced.rdd.getNumPartitions())
```

---

## 2. In-Memory Partitioning by Column

You can repartition by specific columns so rows with the same key reside in the same partition:

```python
# Repartition data so all rows with same 'region' land on same partition
df_by_col = df.repartition(4, "id")
```

---

## 3. Storage Partitioning (`partitionBy`)

When saving data to data lakes (S3, ADLS, GCS, HDFS), partition by low-cardinality query columns (like `year`, `month`, `country`):

```python
# Creates directory structure: output/year=2026/month=09/part-000...parquet
df.write \
  .mode("overwrite") \
  .partitionBy("year", "month") \
  .parquet("output/events_by_date")
```

### ⚠️ The Small Files Anti-Pattern:
- Never partition by high-cardinality columns (e.g., `user_id`, `timestamp`).
- Doing so creates millions of tiny 1 KB files, crippling file system metadata and degrading query performance.
- Target optimal file size: **128 MB to 512 MB per file**.

---

## 4. Bucketing (`bucketBy`)

While partitioning creates nested directories, **bucketing** distributes records into a fixed number of buckets based on a hash of a join column:

```python
# Saves into 8 pre-hashed buckets sorted by customer_id
df.write \
  .mode("overwrite") \
  .bucketBy(8, "customer_id") \
  .sortBy("order_date") \
  .saveAsTable("bucketed_orders")
```

### Why Bucketing Matters:
When two large tables bucketed on the same column and same number of buckets are joined, Spark performs a **Bucket Join with ZERO SHUFFLE**!
