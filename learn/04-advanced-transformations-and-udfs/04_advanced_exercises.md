# Module 04: Advanced Transformations Practice Exercises

Test your skills in nested data extraction, array manipulation, and custom logic.

---

## Challenge 1: Flattening E-commerce Order JSON Payloads

### Problem Statement
You are given incoming API transaction payloads containing order metadata and a nested list of items purchased. Extract each product purchase with customer details as a flat DataFrame.

```python
raw_orders = [
    (101, "Alice", '[{"product": "Phone", "qty": 1, "price": 800.0}, {"product": "Case", "qty": 2, "price": 25.0}]'),
    (102, "Bob", '[{"product": "Monitor", "qty": 1, "price": 300.0}]')
]
```

### Solution
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

spark = SparkSession.builder.master("local[*]").appName("AdvEx1").getOrCreate()

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer", StringType()),
    StructField("items_json", StringType())
])
df = spark.createDataFrame(raw_orders, schema)

# 1. Define schema for the Array of Structs inside the JSON
item_schema = ArrayType(
    StructType([
        StructField("product", StringType()),
        StructField("qty", IntegerType()),
        StructField("price", DoubleType())
    ])
)

# 2. Parse JSON string into Array of Structs
# 3. Explode the array to get one row per purchased item
# 4. Extract nested struct fields
flattened_df = df \
    .withColumn("items_array", from_json(col("items_json"), item_schema)) \
    .withColumn("item", explode(col("items_array"))) \
    .select(
        col("order_id"),
        col("customer"),
        col("item.product").alias("product"),
        col("item.qty").alias("quantity"),
        col("item.price").alias("unit_price"),
        (col("item.qty") * col("item.price")).alias("total_item_cost")
    )

flattened_df.show()
```

---

## Challenge 2: Pandas Vectorized Haversine Distance

### Problem Statement
Calculate the geographic distance in kilometers between two GPS coordinates `(lat1, lon1)` and `(lat2, lon2)` using a high-speed Pandas Vectorized UDF and NumPy.

```python
locations = [
    (1, 40.7128, -74.0060, 34.0522, -118.2437),  # NYC to LA (~3935 km)
    (2, 51.5074, -0.1278, 48.8566, 2.3522)       # London to Paris (~343 km)
]
```

### Solution
```python
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

cols = ["trip_id", "lat1", "lon1", "lat2", "lon2"]
trips_df = spark.createDataFrame(locations, cols)

@pandas_udf(DoubleType())
def calculate_distance_km(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    # Convert degrees to radians
    r = 6371.0 # Earth radius in km
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    
    a = np.sin(delta_phi / 2.0)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return pd.Series(np.round(r * c, 2))

result_df = trips_df.withColumn(
    "distance_km",
    calculate_distance_km(col("lat1"), col("lon1"), col("lat2"), col("lon2"))
)

result_df.show()
```
