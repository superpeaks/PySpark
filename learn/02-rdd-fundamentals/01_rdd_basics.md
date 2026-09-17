# Module 02: Resilient Distributed Datasets (RDD) Basics

## 1. What is an RDD?

An **RDD (Resilient Distributed Dataset)** is the fundamental low-level abstraction in Apache Spark.
It is an immutable, distributed collection of elements partitioned across worker nodes in a cluster.

### Key Characteristics:
1. **Resilient**: Fault-tolerant through lineage graphs. If a partition fails or node crashes, Spark recomputes the lost partition automatically.
2. **Distributed**: Data is split into logical partitions distributed across cluster nodes for parallel processing.
3. **Dataset**: Represents collections of records (tuples, strings, Python objects).
4. **Immutable**: Once created, an RDD cannot be altered. Transformations produce brand-new RDDs.
5. **Lazy Evaluation**: Transformations are recorded, not executed until an action is called.

---

## 2. When to Use RDDs Today?

While DataFrames and Datasets are preferred for structured data due to Catalyst optimizer benefits, RDDs are still important for:
- Processing unstructured data (raw logs, arbitrary binary streams, non-relational text).
- Precise low-level control over partitioning and physical data placement.
- Custom serialization or integration with legacy Scala/Java libraries.
- Understanding how Spark internally executes higher-level DataFrame operations.

---

## 3. How to Create an RDD

### A. Parallelizing an existing collection
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("RDDBasics").getOrCreate()
sc = spark.sparkContext

# Create an RDD from a Python list
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(numbers, numSlices=4)

print("Partitions count:", rdd.getNumPartitions())
print("First 3 elements:", rdd.take(3))
```

### B. Reading from external storage
```python
# Read text files (splits by line)
text_rdd = sc.textFile("path/to/sample.txt")

# Read multiple files with filenames preserved
whole_rdd = sc.wholeTextFiles("path/to/folder/*.txt")
```

---

## 4. Partitions in RDDs

A **partition** is a chunk of the dataset that resides on a single physical node.
- Number of partitions determines the parallelism.
- Too few partitions $\rightarrow$ Underutilized CPU cores.
- Too many partitions $\rightarrow$ High task scheduling overhead.

```python
# Check partition distribution
def print_partition_info(split_index, iterator):
    yield f"Partition {split_index}: {list(iterator)}"

partition_data = rdd.mapPartitionsWithIndex(print_partition_info).collect()
for p in partition_data:
    print(p)
```

---

## 5. RDD Lineage Graph

Every transformation creates a child RDD with a pointer to its parent. This forms a **Lineage Graph (DAG)**.

```python
rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = rdd1.map(lambda x: x * 2)
rdd3 = rdd2.filter(lambda x: x > 4)

# Inspect the lineage string
print(rdd3.toDebugString().decode('utf-8'))
```
