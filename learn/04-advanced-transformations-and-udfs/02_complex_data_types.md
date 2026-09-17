# Module 04: Complex Data Types (Arrays, Structs, Maps & JSON)

Modern data pipelines frequently process hierarchical or semi-structured data formats such as nested JSON payloads, arrays, and key-value maps.

---

## 1. Structs (Nested Objects)

A `Struct` represents an object with nested fields.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct

spark = SparkSession.builder.master("local[*]").appName("ComplexTypes").getOrCreate()

data = [(1, "Alice", "New York", "USA"), (2, "Bob", "London", "UK")]
df = spark.createDataFrame(data, ["id", "name", "city", "country"])

# Bundle city and country into an 'address' struct
df_struct = df.withColumn("address", struct(col("city"), col("country"))).drop("city", "country")
df_struct.printSchema()

# Access nested fields using dot notation
df_struct.select("id", "name", "address.city", "address.country").show()
```

---

## 2. Arrays: Flattening & Exploration

```python
from pyspark.sql.functions import (
    col, split, explode, posexplode, array_contains, size, slice as spark_slice
)

data = [
    (1, "Alice", ["Python", "Spark", "SQL"]),
    (2, "Bob", ["Java", "Scala"]),
    (3, "Charlie", ["Python", "Go"])
]
df_arrays = spark.createDataFrame(data, ["id", "name", "skills"])

# 1. Filter rows where array contains an element
df_arrays.filter(array_contains(col("skills"), "Spark")).show()

# 2. Get size of array
df_arrays.withColumn("skill_count", size(col("skills"))).show()

# 3. explode(): Unrolls array into multiple rows (1 row per item)
exploded_df = df_arrays.withColumn("skill", explode(col("skills")))
exploded_df.show()

# 4. posexplode(): Emits both the 0-indexed position and element
df_arrays.select(col("id"), col("name"), posexplode(col("skills")).alias("pos", "skill")).show()
```

---

## 3. Parsing JSON Strings with `from_json`

When reading streaming event logs or Kafka topics, payloads arrive as raw JSON strings:

```python
from pyspark.sql.functions import from_json, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

json_data = [
    (1, '{"user_id": 101, "event": "click", "device": "mobile"}'),
    (2, '{"user_id": 102, "event": "purchase", "device": "desktop"}')
]
raw_df = spark.createDataFrame(json_data, ["log_id", "raw_payload"])

# Define schema for the JSON content
json_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("event", StringType()),
    StructField("device", StringType())
])

# Parse string into struct
parsed_df = raw_df.withColumn("parsed", from_json(col("raw_payload"), json_schema))

# Flatten fields
parsed_df.select(
    col("log_id"),
    col("parsed.user_id"),
    col("parsed.event"),
    col("parsed.device")
).show()

# Convert struct back to JSON string
reconstructed_json = parsed_df.withColumn("json_str", to_json(col("parsed")))
```
