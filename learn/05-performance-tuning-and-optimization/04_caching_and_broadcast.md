# Module 05: Caching Strategies, Broadcast Variables & Accumulators

Spark provides shared variables and in-memory caching mechanisms to optimize multi-pass computations and reduce redundant data transfers.

---

## 1. DataFrame Caching & Persistence

When a DataFrame is queried multiple times in the same pipeline, caching prevents Spark from re-executing all transformations from the raw files.

```python
from pyspark import StorageLevel
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CachingDemo").getOrCreate()

df = spark.read.parquet("data/large_transactions")

# Filter and clean
filtered_df = df.filter("amount > 100").filter("status = 'COMPLETED'")

# Persist in memory with disk spillover
filtered_df.persist(StorageLevel.MEMORY_AND_DISK)

# Action 1: Calculates and populates the cache
count_high_value = filtered_df.count()

# Action 2: Instantly reads from cache
sum_revenue = filtered_df.groupBy("region").sum("amount")

# Always unpersist when finished!
filtered_df.unpersist()
```

### When to Cache:
- Data is reused in multiple downstream actions (e.g., training a model with multiple iterations).
- The lineage graph is extremely complex and branching.

### When NOT to Cache:
- The DataFrame is evaluated only once (waste of memory!).
- The dataset is larger than aggregate cluster RAM and disk spill slows down execution more than reading raw files.

---

## 2. Broadcast Variables (`sc.broadcast`)

While `broadcast(df)` optimizes DataFrame joins, `sc.broadcast()` sends a read-only Python lookup dictionary or list to all worker nodes **once**, rather than shipping it with every single task.

```python
sc = spark.sparkContext

# Small reference lookup dictionary (e.g. 50k zip codes or config flags)
country_codes = {"US": "United States", "CA": "Canada", "MX": "Mexico", "UK": "United Kingdom"}

# Broadcast to all worker nodes
broadcast_codes = sc.broadcast(country_codes)

# Use inside transformation
rdd = sc.parallelize(["US", "UK", "CA", "FR"])
mapped = rdd.map(lambda code: broadcast_codes.value.get(code, "Unknown"))
print(mapped.collect())
```

---

## 3. Accumulators (`sc.accumulator`)

Accumulators are distributed variables that are only "added" to through an associative and commutative operation. They are typically used for implementing counters or debugging dirty data without triggering a full stage.

```python
# Create an accumulator to track corrupted records
corrupt_record_count = sc.accumulator(0)

raw_records = ["101,Valid", "BAD_RECORD", "102,Valid", "CORRUPT", "103,Valid"]
rdd = sc.parallelize(raw_records)

def validate_and_parse(line):
    global corrupt_record_count
    parts = line.split(",")
    if len(parts) != 2:
        corrupt_record_count.add(1)
        return None
    return (int(parts[0]), parts[1])

clean_rdd = rdd.map(validate_and_parse).filter(lambda x: x is not None)
print("Clean elements count:", clean_rdd.count())
print("Corrupted rows detected:", corrupt_record_count.value)
```

⚠️ **Warning with Accumulators in Transformations**: Because transformations are lazy and can be recomputed upon node failure, accumulators in transformations might be incremented multiple times! For guaranteed exactness, use accumulators inside `foreach()` actions.
