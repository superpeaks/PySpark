# Module 06: Structured Streaming Practice Exercises

Hands-on streaming exercises that you can test locally using Spark's built-in synthetic streaming sources (`rate`).

---

## Challenge 1: Local Real-Time Throughput Monitor

### Problem Statement
Using Spark's built-in `rate` streaming source (which generates continuous integers with timestamps), compute the 5-second tumbling window sum of generated values and output the stream to the console.

### Solution
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, sum as spark_sum

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("StreamExercise1") \
    .getOrCreate()

# 1. Generate 10 rows per second synthetic stream
rate_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", "10") \
    .load()

# rate_stream schema: [timestamp: timestamp, value: bigint]

# 2. Windowed aggregation
windowed_sum = rate_stream \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "5 seconds")) \
    .agg(
        spark_sum("value").alias("total_sum")
    )

# 3. Stream to console with complete or update mode
query = windowed_sum.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

# Let it run for 15 seconds then stop
query.awaitTermination(timeout=15)
query.stop()
```

---

## Challenge 2: Streaming Deduplication

### Problem Statement
In streaming pipelines, network retries often deliver duplicate events. Deduplicate incoming events on `(user_id, event_id)` within a 1-hour watermark.

### Solution
```python
# Assuming incoming user_events stream with columns: [event_id, user_id, timestamp, action]
deduped_stream = user_events \
    .withWatermark("timestamp", "1 hour") \
    .dropDuplicates(["event_id", "user_id"])

# Write unique events to Parquet storage sink
query = deduped_stream.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "output/unique_events/") \
    .option("checkpointLocation", "/tmp/checkpoints/dedup") \
    .start()
```
