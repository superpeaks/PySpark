# Module 07: Parquet & Columnar File Formats

In big data analytics, choosing the right physical file format is one of the highest leverage performance decisions you will make.

---

## 1. Row-Oriented vs. Columnar Storage

```
Row-Oriented (CSV, JSON, Avro):
Row 1: [ID: 1, Name: Alice, Age: 30, Salary: 95000]
Row 2: [ID: 2, Name: Bob,   Age: 25, Salary: 72000]

Columnar (Apache Parquet, Apache ORC):
ID:     [1, 2]
Name:   ["Alice", "Bob"]
Age:    [30, 25]
Salary: [95000, 72000]
```

### Why Columnar Formats Dominate Analytics:
1. **Column Projection Pruning**: If your query only needs `SELECT AVG(salary)`, Spark only reads the `salary` bytes from disk, ignoring name, age, and ID completely!
2. **Extreme Compression**: Similar data types stored adjacently compress dramatically better (often **70% to 85% reduction** in storage costs compared to CSV).
3. **Embedded Schema & Metadata**: Parquet files are self-describing; schemas and data types are encoded directly in the file footer.

---

## 2. Parquet File Internals & Predicate Pushdown

A Parquet file is organized into **Row Groups**, **Column Chunks**, and **Pages**:

```
+-------------------------------------------------------------+
| PARQUET FILE                                                |
|  +-------------------------------------------------------+  |
|  | Row Group 1 (e.g. 128 MB)                             |  |
|  |   Column Chunk 'age': [Pages... Min: 20, Max: 45]     |  |
|  |   Column Chunk 'salary': [Pages... Min: 50k, Max: 120k]|  |
|  +-------------------------------------------------------+  |
|  +-------------------------------------------------------+  |
|  | Row Group 2                                           |  |
|  |   Column Chunk 'age': [Pages... Min: 46, Max: 80]     |  |
|  +-------------------------------------------------------+  |
|  +-------------------------------------------------------+  |
|  | File Footer Metadata (Schema, Row Group offsets & stats)|  |
+-------------------------------------------------------------+
```

### Predicate Pushdown in Action:
When executing `df.filter(col("age") < 30)`, Spark first reads the file footer metadata.
Because `Row Group 2` records `Min: 46`, Spark **skips reading Row Group 2 from disk entirely**!

---

## 3. Writing Optimized Parquet in PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("ParquetTuning").getOrCreate()
df = spark.range(1, 1000000).toDF("id")

# Write with ZSTD compression (higher compression ratio than snappy)
df.write \
  .mode("overwrite") \
  .option("compression", "zstd") \
  .parquet("output/optimized_parquet")

# Read Parquet
read_df = spark.read.parquet("output/optimized_parquet")
```

### Compression Codec Comparison:
- **Snappy**: Default. Balanced CPU speed and compression ratio.
- **Zstandard (`zstd`)**: Higher compression ratio with fast decompression speed (modern standard).
- **Gzip**: Highest compression, but slow and non-splittable in certain legacy engines.
