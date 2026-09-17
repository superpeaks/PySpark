# Module 03: Joins, Join Strategies & Unions

Joining distributed datasets is one of the most resource-intensive operations in Apache Spark. Mastering join types and physical strategies is critical for performance.

---

## 1. Join Types Overview

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder.master("local[*]").appName("Joins").getOrCreate()

employees = [
    (1, "Alice", 10),
    (2, "Bob", 20),
    (3, "Charlie", 30),
    (4, "David", 40)   # Dept 40 doesn't exist
]
departments = [
    (10, "HR"),
    (20, "Engineering"),
    (30, "Sales"),
    (50, "Marketing")  # No employees
]

emp_df = spark.createDataFrame(employees, ["emp_id", "name", "dept_id"])
dept_df = spark.createDataFrame(departments, ["dept_id", "dept_name"])
```

### Supported Join Types in PySpark:

```python
# 1. Inner Join (default)
emp_df.join(dept_df, on="dept_id", how="inner").show()

# 2. Left Outer Join
emp_df.join(dept_df, on="dept_id", how="left").show()

# 3. Right Outer Join
emp_df.join(dept_df, on="dept_id", how="right").show()

# 4. Full Outer Join
emp_df.join(dept_df, on="dept_id", how="full").show()

# 5. Left Semi Join (Returns only columns from left table where a match exists on right)
# Equivalent to: SELECT * FROM emp WHERE dept_id IN (SELECT dept_id FROM dept)
emp_df.join(dept_df, on="dept_id", how="left_semi").show()

# 6. Left Anti Join (Returns only rows from left table that have NO match on right)
# Equivalent to: SELECT * FROM emp WHERE dept_id NOT IN (SELECT dept_id FROM dept)
emp_df.join(dept_df, on="dept_id", how="left_anti").show()
```

---

## 2. Broadcast Join (Broadcast Hash Join - BHJ)

When one of the DataFrames is small (default threshold: $< 10$ MB), Spark can copy the entire small table to memory on every executor node.

### Why Broadcast Joins are Fast:
- **Zero Shuffle**: Avoids expensive data exchange across the network.
- High performance, eliminates disk spills.

```python
# Force a broadcast join using the broadcast() hint
joined_df = emp_df.join(broadcast(dept_df), on="dept_id", how="inner")
joined_df.show()

# Inspect query plan to verify BroadcastHashJoin
joined_df.explain()
```

---

## 3. Unions: `union` vs `unionByName`

```python
df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "val"])
df2 = spark.createDataFrame([(3, "C"), (4, "D")], ["id", "val"])
df3_diff_order = spark.createDataFrame([("E", 5)], ["val", "id"])

# 1. Standard union(): Combines by POSITION (prone to silent bugs if schemas differ!)
df_combined = df1.union(df2)

# 2. unionByName(): Combines strictly by COLUMN NAME (Safe!)
df_safe = df1.unionByName(df3_diff_order)
df_safe.show()

# 3. unionByName with allowMissingColumns=True (Spark 3.1+)
df4 = spark.createDataFrame([(6, "F", "Active")], ["id", "val", "status"])
df_flexible = df1.unionByName(df4, allowMissingColumns=True)
df_flexible.show()
```
