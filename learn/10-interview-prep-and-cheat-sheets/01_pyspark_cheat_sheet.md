# Module 10: PySpark Quick Reference Cheat Sheet

A condensed, high-density syntax reference for daily PySpark development and interview coding tests.

---

## 1. Session Initialization & I/O

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Session
spark = SparkSession.builder \
    .appName("QuickRef") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Reading Files
df_csv     = spark.read.option("header", "true").schema(my_schema).csv("path/file.csv")
df_parquet = spark.read.parquet("path/*.parquet")
df_json    = spark.read.json("path/events.json")
df_delta   = spark.read.format("delta").load("path/delta_table")

# Writing Files
df.write.mode("overwrite").parquet("out/parquet_data")
df.write.mode("append").partitionBy("year", "month").parquet("out/partitioned")
```

---

## 2. DataFrame Inspection & Column Manipulation

```python
# Inspection
df.show(5, truncate=False)       # Display top 5 rows without truncation
df.printSchema()                 # Print schema tree
df.count()                       # Count total rows
df.columns                       # List of column names (strings)
df.dtypes                        # List of (col_name, data_type) tuples

# Column Transformations
df.select("col1", col("col2") + 10)
df.withColumn("new_col", col("existing") * 2)
df.withColumnRenamed("old_name", "new_name")
df.drop("col1", "col2")
df.dropDuplicates(["key1", "key2"])

# Conditional Logic
df.withColumn("tier", when(col("score") >= 90, "A").when(col("score") >= 80, "B").otherwise("C"))

# Casting Types
df.withColumn("age", col("age_str").cast(IntegerType()))
```

---

## 3. Filtering & String / Date Functions

```python
# Filtering (Use bitwise & and | with parenthesis)
df.filter((col("age") > 21) & (col("country") == "USA"))
df.filter(col("name").isin(["Alice", "Bob"]))
df.filter(col("name").like("A%"))
df.filter(col("dept").isNull())

# String Operations
concat_ws("-", col("first"), col("last")) # Join strings with delimiter
upper(col("str")), lower(col("str")), trim(col("str"))
split(col("full_name"), " ")              # Array of split tokens
regexp_replace(col("phone"), "[^0-9]", "") # Regex replace

# Date & Time Operations
to_date(col("date_str"), "yyyy-MM-dd")
year(col("date")), month(col("date")), dayofmonth(col("date"))
datediff(col("end_date"), col("start_date"))
date_add(col("date"), 7)                  # Add 7 days
current_timestamp(), current_date()
```

---

## 4. Aggregations & Window Functions

```python
# Aggregations
df.groupBy("dept").agg(
    count("*").alias("headcount"),
    avg("salary").alias("avg_sal"),
    sum("salary").alias("total_sal"),
    countDistinct("title").alias("unique_roles")
)

# Window Specifications
w_dept = Window.partitionBy("dept").orderBy(col("salary").desc())
w_running = Window.partitionBy("dept").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Window Operations
df.withColumn("rank", rank().over(w_dept))
df.withColumn("dense_rank", dense_rank().over(w_dept))
df.withColumn("row_num", row_number().over(w_dept))
df.withColumn("prev_val", lag("amount", 1).over(w_running))
df.withColumn("next_val", lead("amount", 1).over(w_running))
df.withColumn("running_total", sum("amount").over(w_running))
```

---

## 5. Joins & Unions

```python
# Join Types: inner, left, right, full, left_semi, left_anti, cross
df1.join(df2, on="id", how="inner")
df1.join(df2, df1["dept_id"] == df2["id"], how="left")

# Broadcast Join (Small Dimension Table)
df_large.join(broadcast(df_small), on="id")

# Union
df1.unionByName(df2)
df1.unionByName(df2, allowMissingColumns=True)
```
