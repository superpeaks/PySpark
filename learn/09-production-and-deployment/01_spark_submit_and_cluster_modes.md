# Module 09: `spark-submit` & Cluster Sizing Calculations

Submitting Spark jobs and sizing cluster memory/cores is one of the most heavily tested topics in data engineering technical interviews.

---

## 1. The `spark-submit` Command

`spark-submit` is the unified CLI script used to package and launch Spark applications on any cluster manager (YARN, Kubernetes, Standalone).

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "DailyCustomerAggregation" \
  --num-executors 20 \
  --executor-cores 4 \
  --executor-memory 16G \
  --driver-memory 8G \
  --conf spark.sql.shuffle.partitions=400 \
  --conf spark.dynamicAllocation.enabled=true \
  --py-files dependencies.zip \
  main.py \
  --date 2026-09-17
```

---

## 2. Deploy Modes: Client vs. Cluster

| Feature | Client Mode (`--deploy-mode client`) | Cluster Mode (`--deploy-mode cluster`) |
| :--- | :--- | :--- |
| **Driver Location** | Runs locally on the client machine where `spark-submit` was launched | Runs inside a Worker Node / ApplicationMaster container |
| **Network Traffic** | High network latency between client machine and remote cluster executors | Low latency; Driver is colocated inside cluster network |
| **Terminal Crash Risk** | If client terminal disconnects, the **entire Spark job dies**! | Safe: If client disconnects, the job continues running |
| **Best For** | Interactive development, Jupyter notebooks, quick debugging | **Production Scheduled Jobs** (Airflow, Cron, Prefect) |

---

## 3. The Cluster Sizing Formula (The "Rule of Thumb")

Suppose your company has a cluster with:
- **10 Worker Nodes**
- **16 Cores per Node** (160 cores total)
- **64 GB RAM per Node** (640 GB total)

How do you size your `spark-submit` arguments?

### Step 1: Reserve OS / Daemon Resources
- Leave **1 core and 1 GB RAM per node** for OS daemons (YARN NodeManager, OS kernel).
- Usable per node: 15 cores, 63 GB RAM.

### Step 2: Determine Executor Cores (Sweet Spot: 4 to 5 cores)
- Why not 1 core? Underutilizes JVM memory sharing and multithreading.
- Why not 15 cores? Excessive JVM Garbage Collection (GC) pauses!
- **Optimal: 5 cores per executor**.
- Total executors per node = $15 / 5 = \mathbf{3\text{ executors per node}}$.
- Total executors across 10 nodes = $3 \times 10 = 30\text{ executors}$.

### Step 3: Reserve 1 Executor for Cluster Driver
- Number of worker executors = $30 - 1 = \mathbf{29\text{ executors}}$ (`--num-executors 29`).

### Step 4: Calculate Memory per Executor
- Usable RAM per node = $63\text{ GB} / 3\text{ executors} = 21\text{ GB per executor}$.
- Deduct **10% for Memory Overhead** (off-heap memory, Py4J buffers):
  $$\text{Memory Overhead} = 21\text{ GB} \times 0.10 \approx 2\text{ GB}$$
  $$\text{Executor Memory} = 21\text{ GB} - 2\text{ GB} = \mathbf{19\text{ GB}}$$

### Final Sizing Configuration:
```bash
--num-executors 29
--executor-cores 5
--executor-memory 19G
--conf spark.executor.memoryOverhead=2G
```
