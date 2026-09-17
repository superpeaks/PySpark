# Module 03: Column Operations & Built-in Functions

PySpark provides over 300 optimized built-in SQL functions under `pyspark.sql.functions`. Always prefer built-in functions over custom Python UDFs for maximum performance.

---

## 1. Column Selection & Renaming

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws, upper, to_date, datediff, current_date

spark = SparkSession.builder.master("local[*]").appName("ColOps").getOrCreate()

data = [
    (1, "john", "doe", "1990-05-15", 75000),
    (2, "jane", "smith", "1985-11-20", 92000),
    (3, "alex", "jones", "2000-01-10", 58000)
]
df = spark.createDataFrame(data, ["id", "first_name", "last_name", "birth_date", "salary"])

# 1. Select specific columns
df.select(col("id"), col("first_name")).show()

# 2. Select with expressions
df.select(
    col("id"),
    concat_ws(" ", upper(col("first_name")), upper(col("last_name"))).alias("full_name")
).show()

# 3. Add or replace columns with withColumn()
df_with_cols = df \
    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
    .withColumn("annual_bonus", col("salary") * lit(0.15)) \
    .withColumnRenamed("salary", "base_salary") \
    .drop("first_name", "last_name")

df_with_cols.show()
```

---

## 2. Conditional Logic: `when()` and `otherwise()`

Equivalent to SQL `CASE WHEN ... THEN ... ELSE ... END`:

```python
from pyspark.sql.functions import when

# Classify salary tiers
df_tiered = df.withColumn(
    "salary_bracket",
    when(col("salary") >= 90000, "High")
    .when((col("salary") >= 60000) & (col("salary") < 90000), "Medium")
    .otherwise("Entry")
)

df_tiered.select("id", "salary", "salary_bracket").show()
```

---

## 3. Date and Time Functions

```python
from pyspark.sql.functions import year, month, date_add, to_date, round as spark_round

df_dates = df.withColumn("birth_dt", to_date(col("birth_date"), "yyyy-MM-dd")) \
    .withColumn("birth_year", year(col("birth_dt"))) \
    .withColumn("birth_month", month(col("birth_dt"))) \
    .withColumn("age_years", spark_round(datediff(current_date(), col("birth_dt")) / 365.25, 1))

df_dates.select("id", "birth_date", "birth_year", "age_years").show()
```

---

## 4. Casting Data Types & Null Handling

```python
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import coalesce

# Cast column types
df_cast = df.withColumn("salary_int", col("salary").cast(IntegerType()))

# Coalesce (first non-null value)
# Useful when falling back to secondary contact info or defaults
df_coalesce = df.withColumn("safe_salary", coalesce(col("salary"), lit(0)))
```

---

## 5. Filtering and Slicing Rows

```python
# Multiple filter conditions
# Always use bitwise operators & (AND), | (OR), ~ (NOT) with parentheses!
filtered_df = df.filter(
    (col("salary") > 60000) & (col("first_name").startswith("j"))
)

filtered_df.show()
```
