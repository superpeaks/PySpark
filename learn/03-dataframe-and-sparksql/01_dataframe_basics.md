# Module 03: DataFrame Basics & Schemas

## 1. What is a Spark DataFrame?

A **DataFrame** is a distributed collection of structured data organized into named columns. Conceptually equivalent to a table in a relational database or a Pandas DataFrame, but with:
- **Catalyst Query Optimizer** generating optimized physical plans.
- **Tungsten Execution Engine** leveraging off-heap memory and binary encoded formats.
- Massively parallel execution across a cluster.

---

## 2. Creating DataFrames

### Method A: From Python Lists/Tuples with Inferred Schema
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("DFBasics").getOrCreate()

data = [
    (101, "Alice", 29, 95000.0, "Engineering"),
    (102, "Bob", 34, 82000.0, "Marketing"),
    (103, "Charlie", 41, 115000.0, "Engineering"),
    (104, "Diana", 26, 71000.0, "Product")
]
columns = ["id", "name", "age", "salary", "department"]

df = spark.createDataFrame(data, schema=columns)
df.show(5, truncate=False)
df.printSchema()
```

### Method B: Defining Explicit Schema with `StructType` (Best Practice!)
⚠️ **Production Rule**: Never rely on schema inference for big data ingestion. Schema inference triggers an extra pass over the entire file to determine column types!

```python
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("department", StringType(), nullable=True)
])

df_explicit = spark.createDataFrame(data, schema=schema)
df_explicit.printSchema()
```

---

## 3. Reading and Writing Common Data Formats

### Reading CSV
```python
# Production pattern: Provide explicit schema and handle bad records
df_csv = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("mode", "DROPMALFORMED") \
    .schema(schema) \
    .load("data/employees.csv")
```

### Reading & Writing Parquet (Recommended format)
Parquet is columnar, compressed, and preserves schemas natively:
```python
# Write DataFrame as partitioned Parquet
df_explicit.write \
    .mode("overwrite") \
    .partitionBy("department") \
    .parquet("output/employees_parquet")

# Read Parquet
df_parquet = spark.read.parquet("output/employees_parquet")
```

### Reading JSON
```python
df_json = spark.read \
    .option("multiline", "false") \
    .json("data/events.json")
```

---

## 4. Inspection & Display Methods

```python
# Display rows
df.show(numRows=5, truncate=False)

# Show schema tree
df.printSchema()

# Return list of column names and types
print("Columns:", df.columns)
print("Data Types:", df.dtypes)

# Count total rows
print("Row count:", df.count())

# Summary statistics (count, mean, stddev, min, max)
df.describe().show()
```
