# Module 03: Spark SQL Queries & Views

Spark SQL seamlessly integrates standard ANSI SQL with the PySpark DataFrame API. Under the hood, both SQL queries and DataFrame operations compile into the exact same Catalyst Execution Plan!

---

## 1. Creating Temporary Views

To run raw SQL queries against a DataFrame, register it as a temporary view:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SparkSQLDemo").getOrCreate()

data = [
    (1, "Smartphone", "Electronics", 799.99),
    (2, "Coffee Maker", "Kitchen", 89.99),
    (3, "Laptop", "Electronics", 1299.99),
    (4, "Air Fryer", "Kitchen", 120.00),
    (5, "Headphones", "Electronics", 199.99)
]
df = spark.createDataFrame(data, ["item_id", "item_name", "category", "price"])

# 1. Local Temporary View: Scoped to the current SparkSession
df.createOrReplaceTempView("products")

# 2. Global Temporary View: Shared across multiple sessions in the application
df.createOrReplaceGlobalTempView("global_products")
```

---

## 2. Querying with `spark.sql()`

You can execute any standard ANSI SQL query using `spark.sql()`:

```python
# Execute SQL query returning a PySpark DataFrame
sql_df = spark.sql("""
    SELECT 
        category,
        COUNT(*) AS total_items,
        ROUND(AVG(price), 2) AS avg_price,
        MAX(price) AS max_price
    FROM products
    GROUP BY category
    HAVING AVG(price) > 100
    ORDER BY avg_price DESC
""")

sql_df.show()
```

### Accessing Global Temp Views
Global views are registered under the system `global_temp` database:
```python
spark.sql("SELECT * FROM global_temp.global_products WHERE price < 150").show()
```

---

## 3. Mixing SQL and DataFrame API

Because `spark.sql()` returns a DataFrame, you can freely chain DataFrame methods:

```python
from pyspark.sql.functions import col

spark.sql("SELECT * FROM products WHERE price > 100") \
     .filter(col("category") == "Electronics") \
     .withColumn("discounted_price", col("price") * 0.9) \
     .show()
```

---

## 4. Querying the Spark Catalog

```python
# List all registered tables/views
tables = spark.catalog.listTables()
for t in tables:
    print(f"Table: {t.name}, Database: {t.database}, isTemporary: {t.isTemporary}")

# Drop temporary view when finished
spark.catalog.dropTempView("products")
```
