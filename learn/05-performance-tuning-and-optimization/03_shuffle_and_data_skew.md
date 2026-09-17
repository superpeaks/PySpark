# Module 05: Shuffle Mechanics & Solving Data Skew

In distributed computing, **Data Skew** is the #1 cause of pipeline slowdowns and `OutOfMemoryError` (OOM) crashes.

---

## 1. What is Shuffle and Why is it Slow?

A **Shuffle** occurs when data must be redistributed across executors (e.g., during `groupBy`, `join`, `distinct`, `repartition`).

### The Hidden Cost of Shuffle:
1. **Disk I/O**: Executors write shuffle files to local disk.
2. **Network Bandwidth**: Massive volumes of data are transferred across the network to destination executors.
3. **Garbage Collection (GC)**: Destination executors allocate large buffers in JVM heap to deserialize and sort incoming data blocks.

---

## 2. How to Identify Data Skew

### Symptoms in Spark Web UI:
- Look at the **Stage Details** tab:
  - **99 out of 100 tasks** finish in 2 seconds.
  - **1 task** runs for 45 minutes or fails with `java.lang.OutOfMemoryError: Java heap space`.
  - The **Max Task Time** is 100x higher than the **Median Task Time**.
  - **Shuffle Read Size** for one task is dramatically larger than others.

---

## 3. Techniques to Fix Data Skew

### Strategy 1: Broadcast Join (If one table is $< 100$ MB)
If the skew happens during a join with a dimension table, broadcast the smaller table:
```python
from pyspark.sql.functions import broadcast

# Eliminates the shuffle altogether!
skewed_df.join(broadcast(dim_df), on="join_key")
```

### Strategy 2: Key Salting (The Gold Standard for Large-Large Joins)
When both tables are too large to broadcast, **salting** artificially splits the hot key across multiple partitions by appending a random integer.

```python
from pyspark.sql.functions import col, concat, lit, rand, floor, explode, array

# Suppose 'join_key' = 'NULL' or a viral product ID causes 90% of data to land in 1 partition

# Step 1: Add a random salt (e.g., 0 to 4) to the skewed large table
SALT_FACTOR = 5
salted_large_df = large_df.withColumn(
    "salt", floor(rand() * SALT_FACTOR)
).withColumn(
    "salted_key", concat(col("join_key"), lit("_"), col("salt"))
)

# Step 2: Replicate keys in the lookup table with all possible salt values
replicated_lookup_df = lookup_df.withColumn(
    "salt_array", array([lit(i) for i in range(SALT_FACTOR)])
).withColumn(
    "salt", explode(col("salt_array"))
).withColumn(
    "salted_key", concat(col("join_key"), lit("_"), col("salt"))
)

# Step 3: Join on the salted key (distributes the hot key across 5 distinct partitions!)
salted_joined = salted_large_df.join(
    replicated_lookup_df, on="salted_key"
).drop("salt", "salted_key", "salt_array")
```

### Strategy 3: Enable Adaptive Query Execution (AQE) Skew Join
Spark 3.0+ includes native automated skew handling:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# A partition is considered skewed if size is 5x larger than median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# Minimum partition size threshold (e.g., 64MB)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "67108864")
```

---

## 4. Tuning `spark.sql.shuffle.partitions`

The default value is **200**, which is almost always sub-optimal:
- For small local jobs ($< 1$ GB), 200 causes hundreds of empty tasks: reduce to `4` or `8`.
- For large cluster jobs ($> 1$ TB), 200 causes massive partitions that spill to disk: increase to `2000` or `5000`.
- Target: Each shuffle partition should be approximately **100 MB to 200 MB** in size.
