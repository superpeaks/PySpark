# Module 10: Scenario-Based System Design & Troubleshooting

In senior and staff data engineering interviews, technical interviewers present open-ended scenarios to test your diagnostic intuition and architectural experience.

---

## Scenario 1: The "99% Finished" Stalled Spark Job

### Interviewer Prompt:
> *"Your daily ETL pipeline processes 2 TB of data. When inspecting the Spark UI, you notice 199 out of 200 tasks completed in 4 minutes, but the last task has been running for 2 hours and eventually fails. What is happening and how do you fix it?"*

### Diagnosis:
This is the classic **Data Skew / Straggler Task** problem. A single key (e.g. `NULL`, an empty string `""`, or a power-user ID like Amazon/Walmart) has millions of times more rows than average, causing that entire partition to land on one single executor core.

### Step-by-Step Remediation Plan:
1. **Identify the Skewed Key**:
   ```python
   # Run exploratory query to find top key frequencies
   df.groupBy("join_key").count().orderBy(col("count").desc()).show(10)
   ```
2. **Apply Key Salting**:
   - If joining with another table, append a random salt integer `(0..9)` to the large table key and replicate the lookup table across salt values.
3. **Handle NULLs Separately**:
   - Filter out `NULL` join keys before the join, execute the join on non-null keys, and then union the null records back in:
   ```python
   valid_keys_df = df.filter(col("join_key").isNotNull())
   null_keys_df  = df.filter(col("join_key").isNull())
   
   joined_df = valid_keys_df.join(other_df, "join_key")
   final_df = joined_df.unionByName(null_keys_df, allowMissingColumns=True)
   ```
4. **Enable AQE**: Turn on `spark.sql.adaptive.skewJoin.enabled = true`.

---

## Scenario 2: Driver OOM vs. Executor OOM

### Interviewer Prompt:
> *"You get an alert: `java.lang.OutOfMemoryError: Java heap space`. How do you immediately know whether it happened on the Driver or an Executor, and how do you remediate each?"*

### Diagnosis:
- **Driver OOM**:
  - **Indicator**: Log shows Driver failure; the entire Spark application crashes abruptly; cluster manager reports Driver exited with status code 137.
  - **Root Causes**: Someone called `.collect()` on a multi-gigabyte DataFrame, ran `toPandas()` without filtering, or broadcasted a table exceeding Driver memory.
  - **Fix**: Replace `.collect()` with `.take(n)` or save results directly to cloud storage via `.write.parquet(...)`; increase `--driver-memory`.
- **Executor OOM**:
  - **Indicator**: Spark UI shows individual tasks failing with `ExecutorLostFailure (exit code 137 - OOM Killed)`; other executors retry the task.
  - **Root Causes**: Uneven partition sizes (data skew), huge window functions with unbounded partition sizes, or excessive concurrent tasks sharing executor heap (`executor-cores` set too high).
  - **Fix**: Reduce executor cores to 4 or 5; increase `--executor-memory` and `spark.executor.memoryOverhead`; use `repartition()` to break down large partitions.

---

## Scenario 3: The "Small Files Problem" in S3 / ADLS

### Interviewer Prompt:
> *"A streaming job writes Parquet files to S3 every 1 minute. After 3 months, queries that used to take 10 seconds now take 25 minutes. What happened and how do you resolve it?"*

### Diagnosis:
Writing every minute creates 60 files per hour $\times 24 \times 90 \approx \mathbf{130,000\text{ tiny files}}$ per partition!
Query engines spend 95% of their time performing HTTP `GET` requests and S3 metadata listings rather than reading actual data.

### Remediation Plan:
1. **Immediate Fix (Compaction Job)**:
   - Run a scheduled compaction job (e.g. daily) that reads the directory and writes back coalesced partitions:
   ```python
   # Delta Lake: Native automated compaction
   spark.sql("OPTIMIZE delta.`s3a://my-bucket/events`")
   
   # Standard Parquet: Coalesce rewrite
   df = spark.read.parquet("s3a://my-bucket/events/date=2026-09-01")
   df.coalesce(4).write.mode("overwrite").parquet("s3a://my-bucket/events/date=2026-09-01_temp")
   ```
2. **Streaming Sink Tuning**:
   - Increase streaming trigger interval from 1 minute to 10 or 15 minutes if real-time SLA allows.
   - For Delta Lake streaming, enable auto-compaction:
     `spark.databricks.delta.autoCompact.enabled = true`

---

## Scenario 4: Change Data Capture (CDC) Upserts at Scale

### Interviewer Prompt:
> *"You receive 50 million change events daily (inserts, updates, and deletes) from MySQL to be reflected in a 2-billion row Silver Data Lake table. How do you implement this efficiently?"*

### Solution Architecture:
1. Store target table in **Delta Lake** or **Apache Iceberg** format to enable ACID upserts.
2. Ingest the 50M daily CDC events into a staging DataFrame.
3. Deduplicate staging events on `primary_key` ordering by `cdc_timestamp desc` to keep only the latest state per row.
4. Execute atomic `MERGE INTO`:
   ```python
   target_table.alias("target").merge(
       latest_cdc_df.alias("source"),
       "target.customer_id = source.customer_id"
   ).whenMatchedUpdate(
       condition="source.op_type = 'UPDATE'",
       set={"name": "source.name", "email": "source.email", "updated_at": "source.timestamp"}
   ).whenMatchedDelete(
       condition="source.op_type = 'DELETE'"
   ).whenNotMatchedInsert(
       condition="source.op_type = 'INSERT'",
       values={"customer_id": "source.customer_id", "name": "source.name", "email": "source.email", "updated_at": "source.timestamp"}
   ).execute()
   ```
5. Periodically run `OPTIMIZE` and `VACUUM` to clean up tombstoned parquet files older than retention period.
