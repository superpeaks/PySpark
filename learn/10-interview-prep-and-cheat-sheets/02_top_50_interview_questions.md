# Module 10: Top 50 PySpark Technical Interview Questions & Answers

A curated collection of the most frequently asked PySpark and Apache Spark questions in Data Engineering interviews (FAANG, Fortune 500, and top tech companies).

---

## 🏛️ Category 1: Spark Architecture & Core Engine

### Q1: What happens under the hood when a Spark Application is submitted?
**Answer**:
1. `spark-submit` launches the **Driver Program** (either on client machine or cluster node).
2. Driver requests container resources from the **Cluster Manager** (YARN, K8s).
3. Cluster Manager launches **Executors** on worker nodes.
4. Driver establishes direct communication with Executors.
5. User code creates a logical execution plan; actions trigger physical planning (DAG creation).
6. DAG Scheduler breaks the job into **Stages** based on shuffle boundaries.
7. Task Scheduler distributes **Tasks** to executors for parallel execution.
8. Executors execute tasks and report results/metrics back to the Driver.

### Q2: What is Lazy Evaluation and what are its advantages?
**Answer**:
Lazy evaluation means Spark does not execute transformations immediately when called; instead, it registers them in an execution graph (Lineage/DAG).
**Advantages**:
- Allows Catalyst Optimizer to analyze the entire graph, enabling Whole-Stage CodeGen, filter pushdown, and column pruning.
- Prevents redundant intermediate writes to disk or network passes.
- Provides automatic fault tolerance by enabling partition recomputation on failure.

### Q3: What is the difference between an Application, Job, Stage, and Task?
**Answer**:
- **Application**: The entire program instance created by `SparkSession`.
- **Job**: A computation sequence triggered every time an **Action** (e.g. `count()`, `collect()`, `write`) is called.
- **Stage**: A subset of tasks in a job separated by a **Shuffle boundary** (wide dependency).
- **Task**: The smallest unit of execution; runs on a single CPU core for a single data partition.

### Q4: Explain the difference between Narrow and Wide transformations.
**Answer**:
- **Narrow**: Each input partition contributes to at most one output partition (no data exchange across network). Examples: `map`, `filter`, `flatMap`.
- **Wide**: Multiple input partitions contribute to multiple output partitions, requiring data to be shuffled over network and sorted on disk. Examples: `groupByKey`, `reduceByKey`, `distinct`, `join`.

### Q5: What is the Spark DAGScheduler vs TaskScheduler?
**Answer**:
- **DAGScheduler**: Operates at high level. Converts logical RDD graph into physical stages of tasks based on shuffle boundaries; handles stage-level fault recovery.
- **TaskScheduler**: Operates at low level. Receives task sets from DAGScheduler and submits them to executors across worker nodes; handles task retries and speculative execution.

---

## ⚡ Category 2: DataFrames, SQL & Transformations

### Q6: RDD vs DataFrame vs Dataset: What are the key differences?
**Answer**:
- **RDD**: Low-level distributed object collection, compile-time type safety (in Scala), but lacks Catalyst query optimization and Tungsten off-heap memory benefits.
- **DataFrame**: Distributed collection organized into named columns (untyped Dataset). Leverages Catalyst optimizer and Tungsten binary format.
- **Dataset**: Strongly typed JVM objects (`Dataset[Person]`), available only in Scala/Java, not in Python due to dynamic typing.

### Q7: Why is `reduceByKey` preferred over `groupByKey`?
**Answer**:
`reduceByKey` performs **map-side aggregation (combiner)** locally on each executor partition before shuffling across the network. `groupByKey` shuffles all raw key-value pairs across the network without pre-aggregation, leading to heavy network congestion and frequent Executor OOMs.

### Q8: What is the difference between `repartition()` and `coalesce()`?
**Answer**:
- `repartition(N)`: Performs a **full network shuffle**; can increase or decrease partition count; creates evenly sized partitions.
- `coalesce(N)`: Can **only decrease** partition count; avoids full shuffle by merging adjacent partitions on the same node; may result in uneven partition sizes.

### Q9: Explain `row_number()`, `rank()`, and `dense_rank()`.
**Answer**:
Given values `[100, 100, 80]`:
- `row_number()`: `1, 2, 3` (distinct sequential integers, no ties).
- `rank()`: `1, 1, 3` (ties share rank, skips subsequent rank).
- `dense_rank()`: `1, 1, 2` (ties share rank, does NOT skip rank).

### Q10: What is the difference between `union()` and `unionByName()`?
**Answer**:
- `union()` resolves columns strictly by **column position** (can accidentally map names to salaries if orders differ).
- `unionByName()` resolves columns strictly by **column name**. Setting `allowMissingColumns=True` fills absent columns with nulls.

---

## 🚀 Category 3: Performance Tuning & Optimization

### Q11: What is a Broadcast Hash Join (BHJ) and when is it used?
**Answer**:
Spark copies the smaller DataFrame ($< 10$ MB by default) into memory on all executor nodes. When joining, the large dataset is scanned locally without shuffling either table across the network.

### Q12: What is Data Skew and how do you resolve it?
**Answer**:
Data Skew occurs when one or few partition keys contain a disproportionate amount of records, causing a single task to run for hours while others finish in seconds.
**Resolutions**:
1. **Key Salting**: Append a random integer `(0..k)` to the skewed key to distribute it across multiple partitions.
2. **Broadcast Join**: If the other table is small, broadcast it to eliminate shuffle.
3. **Adaptive Query Execution (AQE)**: Enable `spark.sql.adaptive.skewJoin.enabled = true`.

### Q13: What are the main components of the Catalyst Optimizer?
**Answer**:
1. Analysis (Catalog schema resolution).
2. Logical Optimization (Predicate pushdown, projection pruning, constant folding).
3. Physical Planning (Selecting physical join and aggregation algorithms based on cost).
4. Whole-Stage Code Generation (Generating compact Java bytecode using Project Tungsten).

### Q14: What is Adaptive Query Execution (AQE) in Spark 3.x?
**Answer**:
AQE re-optimizes physical query plans at runtime based on actual stage metrics collected between shuffle boundaries:
1. Dynamically coalesces small shuffle partitions.
2. Dynamically switches Sort-Merge Joins to Broadcast Hash Joins.
3. Dynamically splits skewed join partitions into sub-partitions.

### Q15: What causes `OutOfMemoryError: Java heap space` and how do you fix it?
**Answer**:
**Causes**:
- Data skew overloading one executor.
- Calling `.collect()` on a huge dataset to Driver memory.
- Too high `spark.executor.cores` leading to too many concurrent tasks sharing executor heap.
- Inadequate executor memory overhead.
**Fixes**:
- Increase `spark.executor.memory` and `spark.executor.memoryOverhead`.
- Reduce `spark.executor.cores` (e.g. from 8 down to 4 or 5).
- Salt skewed keys.
- Avoid `.collect()`; use `.take(n)` or save directly to storage.

---

## 🌊 Category 4: Streaming, Lakehouse & Production

### Q16: What is the role of the Checkpoint directory in Structured Streaming?
**Answer**:
Checkpoints store write-ahead metadata logs and state snapshots on durable storage (S3/ADLS/GCS). If a streaming job fails or cluster restarts, Spark resumes exactly where it left off, guaranteeing **End-to-End Exactly-Once semantics**.

### Q17: What is Watermarking in Structured Streaming?
**Answer**:
A watermark defines how long Spark should wait for late-arriving event-time data before closing the aggregation window and evicting state from memory ($Threshold = Max(EventTime) - Delay$).

### Q18: What is Delta Lake and how does it achieve ACID transactions?
**Answer**:
Delta Lake stores data as Parquet files alongside an atomic `_delta_log/` directory containing chronological JSON commit files. Readers and writers achieve ACID transactions via single-file atomic commits and optimistic concurrency control.

### Q19: Why are Vectorized Pandas UDFs faster than Standard Python UDFs?
**Answer**:
Standard Python UDFs serialize row data row-by-row using Python pickle over socket IPC. Vectorized Pandas UDFs use **Apache Arrow** for zero-copy memory exchange and process entire batches using C-optimized Pandas/NumPy SIMD instructions.

### Q20: How do you calculate executor memory and core sizing for a 10-node cluster with 16 cores and 64 GB RAM per node?
**Answer**:
1. Reserve 1 core and 1 GB per node for OS daemons $\rightarrow$ 15 cores, 63 GB usable.
2. Optimal executor core count is 5 cores $\rightarrow$ $15 / 5 = 3$ executors per node (30 total).
3. Reserve 1 executor for Driver $\rightarrow$ 29 executors (`--num-executors 29`, `--executor-cores 5`).
4. RAM per executor: $63 / 3 = 21$ GB. Deduct 10% overhead ($\approx 2$ GB) $\rightarrow$ `--executor-memory 19G`, `--conf spark.executor.memoryOverhead=2G`.
