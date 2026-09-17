# Module 01: SparkSession & SparkContext

## 1. What is SparkSession?

Introduced in Spark 2.0, `SparkSession` is the unified entry point for reading data, executing SQL queries, creating DataFrames/Datasets, and configuring Spark runtime properties.

Prior to Spark 2.0, you had to manage distinct context objects:
- `SparkContext`: Low-level RDD operations.
- `SQLContext` / `HiveContext`: Structured tables and SQL queries.
- `StreamingContext`: Micro-batch streaming.

In modern PySpark, **`SparkSession` subsumes all of them**:
```python
spark = SparkSession.builder...getOrCreate()
sc = spark.sparkContext  # Access underlying SparkContext if needed
```

---

## 2. Initializing a SparkSession

```python
from pyspark.sql import SparkSession

# Standard production-ready initialization pattern
spark = SparkSession.builder \
    .appName("ProductionDataPipeline") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("Spark Session Created successfully!")
print(f"App Name: {spark.sparkContext.appName}")
print(f"Master URL: {spark.sparkContext.master}")
print(f"Spark Version: {spark.version}")
```

### Explaining Builder Configuration Parameters

- `.appName("...")`: Identifies your application in logs, cluster manager dashboards, and Spark Web UI.
- `.master("local[*]")`:
  - `local`: Run single thread locally.
  - `local[4]`: Run 4 worker threads locally.
  - `local[*]`: Use all available CPU cores on your local machine.
  - `yarn` / `k8s://...`: Cluster manager master URLs when running on distributed clusters.
- `.config(key, value)`: Override default Spark runtime parameters.
- `.getOrCreate()`: Reuses an existing session if active, or creates a new one (Singleton pattern).

---

## 3. Useful SparkSession Methods

```python
# 1. Check current runtime configs
shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Shuffle Partitions: {shuffle_partitions}")

# 2. Dynamically set a configuration
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") # 10MB

# 3. Access Catalog (list databases, tables, functions)
databases = spark.catalog.listDatabases()
print("Databases:", [db.name for db in databases])

# 4. Stop session (always release resources when done)
spark.stop()
```

---

## 4. Best Practices

1. **One Session per Application**: Avoid instantiating multiple `SparkSession` objects in the same process; use `getOrCreate()`.
2. **Always stop when done**: In scripts, invoke `spark.stop()` in a `try...finally` block to release JVM cluster resources and port allocations cleanly.
3. **Web UI inspection**: While the session is alive, open `http://localhost:4040` to view real-time DAG stages, memory consumption, and execution timelines.
