# Module 09: Unit Testing PySpark Applications

Unit testing distributed data pipelines ensures transformation logic remains bug-free and prevents costly regressions in production data lakes.

---

## 1. Best Practice: Write Pure Transformation Functions

Decouple reading and writing I/O from transformation logic:

```python
# pipeline/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, when

def categorize_customers(df: DataFrame) -> DataFrame:
    """Pure transformation function: takes a DataFrame and returns a transformed DataFrame"""
    return df.withColumn("tier", 
        when(col("spend") > 1000, "Platinum")
        .when(col("spend") > 500, "Gold")
        .otherwise("Standard")
    ).withColumn("name", upper(col("name")))
```

---

## 2. Setting Up Pytest Fixtures (`conftest.py`)

Creating a `SparkSession` is computationally heavy (~2-3 seconds). Use a **session-scoped fixture** so a single SparkSession is reused across all tests:

```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("PySparkUnitTests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()
```

---

## 3. Writing Unit Tests

### Method A: Using PySpark 3.5+ Built-in `assertDataFrameEqual`
```python
# tests/test_transformations.py
from pyspark.testing import assertDataFrameEqual
from pipeline.transformations import categorize_customers

def test_categorize_customers(spark):
    # Arrange: Input data
    input_data = [
        (1, "alice", 1500.0),
        (2, "bob", 600.0),
        (3, "charlie", 200.0)
    ]
    input_df = spark.createDataFrame(input_data, ["id", "name", "spend"])

    # Act: Run transformation
    actual_df = categorize_customers(input_df)

    # Expected output
    expected_data = [
        (1, "ALICE", 1500.0, "Platinum"),
        (2, "BOB", 600.0, "Gold"),
        (3, "CHARLIE", 200.0, "Standard")
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "name", "spend", "tier"])

    # Assert
    assertDataFrameEqual(actual_df, expected_df)
```

### Method B: Using `chispa` (Best for detailed diff reporting)
Install with `pip install chispa`:

```python
from chispa.dataframe_comparer import assert_df_equality

def test_categorize_customers_chispa(spark):
    # Compares schemas, values, with options to ignore row order or nullable differences
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
```

---

## 4. Running the Tests

```bash
pytest tests/ -v
```
