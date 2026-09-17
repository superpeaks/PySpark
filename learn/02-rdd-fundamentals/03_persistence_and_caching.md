# Module 02: RDD Persistence & Caching

Because RDDs are lazily evaluated, whenever you run an action on an RDD, Spark **recomputes the entire lineage from source** unless that RDD has been cached or persisted in memory/disk.

---

## 1. `cache()` vs. `persist()`

- `rdd.cache()`: Shortcut for `rdd.persist(StorageLevel.MEMORY_ONLY)`.
- `rdd.persist(storageLevel)`: Allows specifying custom storage tiers (RAM, Disk, Serialized, Replicas).

```python
from pyspark import StorageLevel
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Persistence").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(range(1, 1000000))
transformed = rdd.map(lambda x: x * 2).filter(lambda x: x % 3 == 0)

# Persist in Memory and Disk
transformed.persist(StorageLevel.MEMORY_AND_DISK)

# Action 1: Lineage executes and partition data is saved in storage level
print("Count:", transformed.count())

# Action 2: Fetched directly from cache/persist (super fast!)
print("First item:", transformed.first())

# Always unpersist when finished to free memory
transformed.unpersist()
```

---

## 2. Storage Levels Comparison

| Storage Level | Space Used | CPU Time | In Memory? | On Disk? | Comments |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `MEMORY_ONLY` | High | Very Low | Yes | No | Default for RDD `.cache()`. Recomputes partitions if RAM full. |
| `MEMORY_ONLY_SER` | Moderate | Moderate | Yes (Serialized) | No | Stores as serialized Java objects (saves space, slight CPU cost). |
| `MEMORY_AND_DISK` | Moderate | Low | Yes | Yes (spill) | Spills partitions to disk if RAM is exhausted. |
| `MEMORY_AND_DISK_SER`| Low | Moderate | Yes (Serialized) | Yes (spill) | Best balance for large datasets exceeding cluster RAM. |
| `DISK_ONLY` | Low | High | No | Yes | Writes all partitions directly to disk. |
| `*_2` (e.g. `MEMORY_ONLY_2`)| 2x Space | Very Low | Yes | No | Replicates each partition across 2 worker nodes for high fault tolerance. |

---

## 3. Best Practices & Pitfalls

1. **Do NOT cache everything**: Caching consumes executor RAM that could otherwise be used for execution shuffle buffers and GC overhead.
2. **Cache when reused $\ge$ 2 times**: Only cache an RDD/DataFrame if it is being referenced in multiple subsequent actions or iterative algorithms (e.g., k-means, PageRank).
3. **Always `unpersist()`**: Release cached resources when no longer needed in the pipeline using `df.unpersist()` to avoid memory leaks.
