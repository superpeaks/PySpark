# Module 07: Apache Iceberg & Open Lakehouse Formats

Alongside Delta Lake, **Apache Iceberg** is a leading open table format for huge analytic datasets, created originally at Netflix and adopted across AWS, Snowflake, and Google Cloud.

---

## 1. The Three Modern Open Table Formats

| Feature | Delta Lake | Apache Iceberg | Apache Hudi |
| :--- | :--- | :--- | :--- |
| **Origin** | Databricks | Netflix / Apple | Uber |
| **Transaction Metadata** | Single JSON-log chain + Parquet checkpoints | Hierarchical Tree (Metadata $\rightarrow$ Manifest Lists $\rightarrow$ Manifests) | Timeline log + Avro/Parquet |
| **Partition Evolution** | Requires rewriting partition data or generated cols | **Native Hidden Partitioning** & seamless partition evolution | Partition transformations |
| **Multi-Engine Support**| Spark, Databricks, Trino, Flink | Spark, Trino, Snowflake, BigQuery, Athena, Flink | Spark, Presto, Trino, Flink |

---

## 2. Apache Iceberg Architecture: The Tree Structure

Unlike Hive tables where partitions depend on physical directory layouts, Iceberg tracks every individual data file using a snapshot tree:

```
           Iceberg Catalog (e.g. Hive, Glue, REST, Nessie)
                                │
                                ▼
                       Metadata File (v1.metadata.json)
                                │
                                ▼
                         Snapshot (s0)
                                │
                                ▼
                          Manifest List
                         ┌──────┴──────┐
                         ▼             ▼
                    Manifest A    Manifest B
                    (min/max stats per file)
                     ┌───┴───┐       ┌───┴───┐
                     ▼       ▼       ▼       ▼
                   File1   File2   File3   File4
```

### Advantages:
1. **O(1) Snapshot Reads**: Snapshot points directly to exact file lists without expensive directory listings.
2. **Hidden Partitioning**: Users don't need to specify partition columns like `WHERE year=2026 AND event_date = '2026-09-17'`; Iceberg automatically prunes partitions from `event_date` filters directly!
3. **Partition Evolution**: You can change partitioning from `days(ts)` to `hours(ts)` without rewriting old historical files.

---

## 3. Configuring Iceberg with PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergDemo") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg-warehouse") \
    .getOrCreate()

# Create Iceberg Table with hidden partitioning
spark.sql("""
    CREATE TABLE local.db.events (
        event_id BIGINT,
        user_id STRING,
        event_time TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
""")
```
