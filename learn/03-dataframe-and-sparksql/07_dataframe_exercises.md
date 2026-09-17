# Module 03: DataFrame & SQL Practice Exercises

Real-world practical exercises covering filtering, windowing, joins, and aggregations.

---

## Challenge 1: Find the 2nd Highest Salary per Department

### Problem Statement
Given employee data with duplicate salaries, find the employee(s) earning the **2nd highest unique salary** in each department. If a department has fewer than 2 distinct salary levels, omit it.

```python
data = [
    (1, "Alice", "IT", 95000),
    (2, "Bob", "IT", 95000),      # Tied for 1st
    (3, "Charlie", "IT", 90000),  # 2nd highest
    (4, "David", "IT", 80000),
    (5, "Eve", "HR", 70000),
    (6, "Frank", "HR", 65000),    # 2nd highest
    (7, "Grace", "Finance", 80000) # Only 1 employee
]
```

### Solution (Using `dense_rank`)
```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank

spark = SparkSession.builder.master("local[*]").appName("ExDF1").getOrCreate()
df = spark.createDataFrame(data, ["emp_id", "name", "dept", "salary"])

# Use dense_rank so ties in 1st place don't skip rank 2
window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())

second_highest = df.withColumn("drank", dense_rank().over(window_spec)) \
                   .filter(col("drank") == 2) \
                   .drop("drank")

second_highest.show()
# Expected Output: Charlie (IT, 90000) and Frank (HR, 65000)
```

---

## Challenge 2: Identify Inactive Customers (Anti-Join)

### Problem Statement
Given two tables: `customers` and `orders`, identify all customers who have **never placed an order**.

```python
customers = [(101, "Alice"), (102, "Bob"), (103, "Charlie"), (104, "Diana")]
orders = [(1, 101, 250.0), (2, 101, 120.0), (3, 103, 450.0)]
```

### Solution
```python
cust_df = spark.createDataFrame(customers, ["cust_id", "cust_name"])
ord_df = spark.createDataFrame(orders, ["order_id", "cust_id", "amount"])

# Left Anti Join returns rows in cust_df where cust_id does NOT exist in ord_df
inactive_customers = cust_df.join(ord_df, on="cust_id", how="left_anti")
inactive_customers.show()
# Expected: Bob (102) and Diana (104)
```

---

## Challenge 3: Calculate Month-over-Month Revenue Growth %

### Problem Statement
Given monthly sales data, calculate each month's total revenue, previous month's revenue, and the percentage growth.

```python
monthly_sales = [
    ("2026-01", 10000.0),
    ("2026-02", 12500.0),
    ("2026-03", 11000.0),
    ("2026-04", 16500.0),
]
```

### Solution (Using `lag` and `round`)
```python
from pyspark.sql.functions import lag, round as spark_round

sales_df = spark.createDataFrame(monthly_sales, ["month", "revenue"])

win = Window.orderBy("month")

mom_df = sales_df \
    .withColumn("prev_revenue", lag("revenue", 1).over(win)) \
    .withColumn(
        "growth_pct",
        spark_round(((col("revenue") - col("prev_revenue")) / col("prev_revenue")) * 100, 2)
    )

mom_df.show()
```
