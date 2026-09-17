# Module 03: Window Functions

Window functions perform calculations across a set of table rows that are related to the current row, without collapsing the rows into a single summary output (unlike `groupBy`).

---

## 1. Syntax & Core Concepts

A Window specification defines:
1. **Partition Specification (`partitionBy`)**: Defines how data is sliced into subsets.
2. **Order Specification (`orderBy`)**: Defines ordering within each partition.
3. **Frame Specification (`rowsBetween` / `rangeBetween`)**: Defines the boundaries of sliding windows.

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, lead, lag, sum, avg
)

spark = SparkSession.builder.master("local[*]").appName("WindowDemo").getOrCreate()

data = [
    ("HR", "Alice", 60000),
    ("HR", "Bob", 60000),
    ("HR", "Charlie", 50000),
    ("IT", "David", 90000),
    ("IT", "Eve", 85000),
    ("IT", "Frank", 85000),
    ("IT", "Grace", 70000)
]
df = spark.createDataFrame(data, ["dept", "name", "salary"])
```

---

## 2. Ranking Functions: `row_number` vs `rank` vs `dense_rank`

### Key Differences:
- `row_number()`: Unique sequential integer for every row (1, 2, 3, 4...).
- `rank()`: Assigns identical ranks to ties, leaves gaps in subsequent ranks (1, 1, 3, 4...).
- `dense_rank()`: Assigns identical ranks to ties, NO gaps in subsequent ranks (1, 1, 2, 3...).

```python
window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())

ranked_df = df.withColumn("row_num", row_number().over(window_spec)) \
              .withColumn("rank", rank().over(window_spec)) \
              .withColumn("dense_rank", dense_rank().over(window_spec))

ranked_df.show()
```

### Classic Interview Problem: "Find Top N earners per Department"
```python
top2_df = ranked_df.filter(col("dense_rank") <= 2)
top2_df.show()
```

---

## 3. Lead and Lag (Time-Series / Row-to-Row Comparison)

- `lag(col, offset=1, default=None)`: Retrieves value from the previous row.
- `lead(col, offset=1, default=None)`: Retrieves value from the next row.

```python
lead_lag_window = Window.partitionBy("dept").orderBy("salary")

diff_df = df.withColumn("prev_salary", lag("salary", 1).over(lead_lag_window)) \
            .withColumn("next_salary", lead("salary", 1).over(lead_lag_window)) \
            .withColumn("diff_from_prev", col("salary") - col("prev_salary"))

diff_df.show()
```

---

## 4. Running Totals & Cumulative Aggregations (`rowsBetween`)

Frame boundaries control which rows are included in the calculation relative to the current row:
- `Window.unboundedPreceding`: From start of partition.
- `Window.currentRow`: Current row.
- `Window.unboundedFollowing`: To end of partition.

```python
cumulative_window = Window.partitionBy("dept") \
    .orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

running_df = df.withColumn("running_dept_total", sum("salary").over(cumulative_window)) \
               .withColumn("running_dept_avg", avg("salary").over(cumulative_window))

running_df.show()
```

### Moving 3-Row Average:
```python
moving_avg_window = Window.partitionBy("dept") \
    .orderBy("salary") \
    .rowsBetween(-1, 1)  # 1 row before, current row, 1 row after

df.withColumn("3row_moving_avg", avg("salary").over(moving_avg_window)).show()
```
