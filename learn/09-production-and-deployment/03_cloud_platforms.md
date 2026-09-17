# Module 09: Running PySpark on Cloud Platforms

In modern enterprise data platforms, PySpark jobs run primarily on cloud-managed distributed clusters or serverless engines.

---

## 1. Cloud Object Storage URIs

PySpark seamlessly reads and writes from cloud storage using native Hadoop filesystem connectors:

| Cloud Provider | Storage Service | URI Scheme | Required Hadoop Connector |
| :--- | :--- | :--- | :--- |
| **AWS** | Amazon S3 | `s3a://bucket-name/path` | `hadoop-aws` (uses `S3AFileSystem`) |
| **Google Cloud** | Google Cloud Storage | `gs://bucket-name/path` | `gcs-connector` |
| **Azure** | Azure Data Lake Storage Gen2 | `abfss://container@account.dfs.core.windows.net/path` | `azure-data-lake-store-sdk` |

> ⚠️ **Security Rule**: NEVER hardcode API access keys or secret keys in PySpark code. Use IAM Instance Profiles (AWS), GCP Service Accounts with Workload Identity, or Azure Managed Identities.

---

## 2. Cloud Service Comparison

### A. Databricks (Unified Data Intelligence Platform)
- **Engine**: Proprietary C++ **Photon** engine vectorized on top of Apache Spark.
- **Data Governance**: Unity Catalog for fine-grained column/row level access controls.
- **Orchestration**: Databricks Workflows (multi-task job orchestration).
- **Local Development**: `databricks-connect` allows writing PySpark code in local IDEs while executing on remote Databricks clusters.

### B. Google Cloud Dataproc (GCP)
- **Dataproc on Compute Engine**: Fast-provisioning transient Hadoop/Spark clusters (boots in ~90 seconds).
- **Dataproc Serverless**: Submit PySpark scripts without managing clusters:
  ```bash
  gcloud dataproc batches submit pyspark main.py \
      --project=my-gcp-project \
      --region=us-central1 \
      --deps-bucket=gs://my-bucket/dependencies \
      --batch=daily-batch-001
  ```

### C. AWS EMR (Elastic MapReduce)
- **EMR on EC2**: Traditional autoscaling YARN clusters.
- **EMR Serverless**: Pay-per-second serverless execution for scheduled data pipelines.
- **EMR on EKS**: Run Spark applications inside Kubernetes pods.

---

## 3. Best Practices for Cloud PySpark Pipelines

1. **Transient Clusters**: Spin up clusters on-demand for scheduled batch jobs and terminate immediately upon completion to minimize compute billing.
2. **Spot / Preemptible Instances for Executors**: Use Spot/Preemptible instances for worker executors (saving 60-80% on compute cost). Keep the **Driver** on an On-Demand instance so jobs are not killed if a Spot node is reclaimed.
3. **Decouple Compute and Storage**: Store all raw, bronze, silver, and gold datasets in cloud object stores (S3/GCS/ADLS), keeping cluster nodes stateless.
