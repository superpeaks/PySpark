# Module 03: Aggregations, Grouping & Pivot

Aggregations allow you to summarize distributed datasets across one or more dimensions.

---

## 1. Basic `groupBy` and `agg`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, countDistinct, min, max, round as spark_round

spark = SparkSession.builder.master("local[*]").appName("Aggregations").getOrCreate()

sales_data = [
    ("East", "Technology", "Laptops", 1200.0, 5),
    ("East", "Technology", "Phones", 800.0, 8),
    ("East", "Furniture", "Chairs", 150.0, 20),
    ("West", "Technology", "Laptops", 1250.0, 7),
    ("West", "Furniture", "Desks", 450.0, 4),
    ("West", "Furniture", "Chairs", 160.0, 15),
    ("South", "Technology", "Phones", 750.0, 12),
]
columns = ["region", "category", "product", "unit_price", "quantity"]
df = spark.createDataFrame(sales_data, columns)

# Multi-metric aggregation
summary_df = df.groupBy("region", "category").agg(
    spark_round(sum(col("unit_price") * col("quantity")), 2).alias("total_revenue"),
    spark_round(avg("unit_price"), 2).alias("avg_price"),
    sum("quantity").alias("total_units"),
    countDistinct("product").alias("unique_products")
).orderBy(col("total_revenue").desc())

summary_df.show()
```

---

## 2. Pivot Tables

Pivot rotates unique values in a specified column into multiple output columns.

```python
# Pivot regions across categories to see total quantity
pivot_df = df.groupBy("category") \
    .pivot("region") \
    .sum("quantity")

pivot_df.show()

# Performance Tip: Explicitly pass distinct values to pivot() to avoid extra scan!
distinct_regions = ["East", "West", "South"]
optimized_pivot = df.groupBy("category") \
    .pivot("region", distinct_regions) \
    .sum("quantity")
```

---

## 3. Hierarchical Aggregations: `rollup` and `cube`

### `rollup`: Multi-level hierarchical subtotals
Generates subtotals from left to right: (Region, Category) $\rightarrow$ (Region) $\rightarrow$ (Grand Total).

```python
df.rollup("region", "category") \
    .agg(sum(col("unit_price") * col("quantity")).alias("revenue")) \
    .orderBy("region", "category") \
    .show()
```

### `cube`: All possible dimensional combinations
Generates subtotals for all permutations: (Region, Category), (Region), (Category), and (Grand Total).

```python
df.cube("region", "category") \
    .agg(sum("quantity").alias("total_qty")) \
    .show()
```
