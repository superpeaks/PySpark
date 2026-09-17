# Module 02: RDD Practice Exercises

Test your understanding of RDD transformations and actions with these hands-on challenges. Try writing the solution before checking the answers!

---

## Challenge 1: Log Parsing - Find Error Counts

### Problem Statement
Given a list of raw server log entries, count the occurrences of each log level (`ERROR`, `WARN`, `INFO`) and print only levels with a count $\ge 2$.

```python
logs = [
    "2026-09-01 10:00:01 INFO Server started",
    "2026-09-01 10:01:05 WARN High memory usage",
    "2026-09-01 10:02:10 ERROR Database connection failed",
    "2026-09-01 10:02:15 ERROR Retry timed out",
    "2026-09-01 10:03:00 INFO User logged in",
    "2026-09-01 10:04:00 WARN Disk usage 85%",
    "2026-09-01 10:05:00 ERROR Out of memory"
]
```

### Solution
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Exercise1").getOrCreate()
sc = spark.sparkContext

log_rdd = sc.parallelize(logs)

# 1. Extract level (3rd token, index 2)
# 2. Map to (level, 1)
# 3. Aggregate by key with reduceByKey
# 4. Filter count >= 2
result = (
    log_rdd
    .map(lambda line: (line.split(" ")[2], 1))
    .reduceByKey(lambda a, b: a + b)
    .filter(lambda pair: pair[1] >= 2)
    .collect()
)

print("Frequent log levels:", result)
# Expected Output: [('WARN', 2), ('ERROR', 3)]
```

---

## Challenge 2: Inverted Index for Search Engine

### Problem Statement
Given documents formatted as `(doc_id, text)`, create an inverted index mapping each unique word to the list of document IDs it appears in.

```python
docs = [
    (1, "spark is fast and scalable"),
    (2, "pyspark provides python api for spark"),
    (3, "fast and scalable data processing")
]
```

### Solution
```python
docs_rdd = sc.parallelize(docs)

# 1. flatMap to emit (word, doc_id) for each distinct word per document
# 2. distinct() to avoid duplicate doc_id per word
# 3. groupByKey or aggregateByKey to collect doc_ids
inverted_index = (
    docs_rdd
    .flatMap(lambda doc: [(word, doc[0]) for word in set(doc[1].split(" "))])
    .groupByKey()
    .mapValues(list)
    .collect()
)

for word, doc_ids in sorted(inverted_index):
    print(f"'{word}': {doc_ids}")
```

---

## Challenge 3: Average Grade per Student

### Problem Statement
Compute the average score per student from pairs of `(student_name, score)` without using `groupByKey` (to avoid shuffle memory overhead).

```python
scores = [
    ("Alice", 85),
    ("Bob", 72),
    ("Alice", 95),
    ("Bob", 88),
    ("Charlie", 90),
    ("Alice", 90)
]
```

### Solution (Using `aggregateByKey`)
```python
scores_rdd = sc.parallelize(scores)

# aggregateByKey arguments:
# zeroValue: (0, 0) -> (total_sum, count)
# seqOp: merge a score into local partition accumulator
# combOp: merge accumulators across partitions
sum_count = scores_rdd.aggregateByKey(
    (0, 0),
    lambda acc, score: (acc[0] + score, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
)

# Compute average
avg_scores = sum_count.mapValues(lambda x: round(x[0] / x[1], 2)).collect()
print("Averages:", dict(avg_scores))
# Output: {'Alice': 90.0, 'Bob': 80.0, 'Charlie': 90.0}
```
