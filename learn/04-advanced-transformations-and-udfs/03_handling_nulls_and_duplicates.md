# Module 04: Handling Nulls, Missing Data & Deduplication

In distributed production datasets, null values and duplicate records are the leading causes of incorrect metric calculations, join drops, and downstream job failures.

---

## 1. Detecting & Filtering Nulls and NaNs

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull, when, count

spark = SparkSession.builder.master("local[*]").appName("NullHandling").getOrCreate()

data = [
    (1, "Alice", 50000.0),
    (2, "Bob", None),
    (3, None, 75000.0),
    (4, "Diana", float("nan")),
    (5, "Eve", None)
]
df = spark.createDataFrame(data, ["id", "name", "salary"])

# 1. Count nulls and NaNs per column
null_counts = df.select([
    count(when(isnull(c) | isnan(c), c)).alias(c) for c in df.columns
])
null_counts.show()

# 2. Filter out null names
df.filter(col("name").isNotNull()).show()
```

---

## 2. Imputing & Dropping Nulls

### Dropping rows: `dropna()`
```python
# Drop rows where ANY column is null
df.dropna(how="any").show()

# Drop rows where ALL columns are null
df.dropna(how="all").show()

# Drop rows only if specific subset columns are null
df.dropna(subset=["name", "salary"]).show()
```

### Imputing values: `fillna()`
```python
# Replace null strings with "Unknown" and null numbers with 0.0
df_cleaned = df.fillna({
    "name": "Unknown",
    "salary": 0.0
})
df_cleaned.show()
```

---

## 3. Deduplication

### A. Simple Deduplication: `dropDuplicates()`
```python
dups_data = [
    (1, "Alice", "2026-01-01"),
    (1, "Alice", "2026-01-05"), # Newer record for same ID
    (2, "Bob", "2026-01-02")
]
df_dups = spark.createDataFrame(dups_data, ["id", "name", "updated_at"])

# Deduplicate by subset of key columns (keeps an arbitrary record)
df_dups.dropDuplicates(subset=["id"]).show()
```

### B. Controlled Deduplication (Keep Most Recent Record)
⚠️ In production, simple `dropDuplicates()` is non-deterministic. Use a Window function to guarantee keeping the newest record!

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("id").orderBy(col("updated_at").desc())

latest_records = df_dups.withColumn("rn", row_number().over(window_spec)) \
                        .filter(col("rn") == 1) \
                        .drop("rn")

latest_records.show()
```

---

## 4. Null-Safe Equality (`eqNullSafe` / `<=>`)

In standard SQL, `NULL = NULL` evaluates to `NULL` (False). In PySpark, use `eqNullSafe` when joining or comparing nullable keys:

```python
# Standard equality will drop matching NULLs!
# eqNullSafe treats NULL == NULL as True:
df1 = spark.createDataFrame([(1, None)], ["id", "val"])
df2 = spark.createDataFrame([(1, None)], ["id", "val"])

# Join on val preserving null matches:
df1.join(df2, df1.val.eqNullSafe(df2.val)).show()
```
