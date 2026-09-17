# Module 01: Environment Setup Guide

To run PySpark, you have several options ranging from zero-install cloud environments to local setups on Windows, macOS, Linux, or Docker.

---

## Option 1: Zero-Install Cloud Environments (Fastest)

### A. Google Colaboratory (Free, 2 minutes)
Open a new notebook on [Google Colab](https://colab.research.google.com/) and run:

```python
# 1. Install pyspark
!pip install -q pyspark

# 2. Verify installation
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ColabTest").getOrCreate()
df = spark.createDataFrame([(1, "Spark"), (2, "PySpark")], ["id", "tech"])
df.show()
```

### B. Databricks Community Edition (Recommended for Cloud Lakehouse)
1. Sign up for free at [community.cloud.databricks.com](https://community.cloud.databricks.com/).
2. Click **Compute** -> **Create Cluster** (Free Single Node, 15GB RAM).
3. Create a **Notebook**, select Python language, and start writing PySpark code immediately without any installation.

---

## Option 2: Local Windows Setup

Spark requires a Java Virtual Machine (JVM). Follow these 4 steps on Windows:

### 1. Install Java JDK (Java 8, 11, or 17)
- Download and install Eclipse Temurin OpenJDK (version 11 or 17 recommended) from [adoptium.net](https://adoptium.net/).
- Verify in PowerShell:
  ```powershell
  java -version
  ```
- Set system environment variable `JAVA_HOME`:
  - Name: `JAVA_HOME`
  - Value: `C:\Program Files\Eclipse Adoptium\jdk-17.x.x` (or your JDK path)
  - Add `%JAVA_HOME%\bin` to your `Path` variable.

### 2. Install Hadoop winutils (Required for Windows File I/O)
Spark relies on Hadoop binaries (`winutils.exe` and `hadoop.dll`) for Windows local file system operations:
1. Create a folder `C:\hadoop\bin`.
2. Download `winutils.exe` and `hadoop.dll` matching your Spark/Hadoop version from the trusted GitHub repository: [cdarlint/winutils](https://github.com/cdarlint/winutils).
3. Place `winutils.exe` and `hadoop.dll` inside `C:\hadoop\bin`.
4. Set system environment variables:
   - Name: `HADOOP_HOME`, Value: `C:\hadoop`
   - Add `%HADOOP_HOME%\bin` to your `Path` variable.

### 3. Create Python Virtual Environment
Using standard Python `venv`:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install pyspark pandas pyarrow
```

Or using `uv` (fast package manager):
```powershell
uv venv .venv
.\.venv\Scripts\Activate.ps1
uv pip install pyspark pandas pyarrow
```

### 4. Verify Local Installation
Run a simple Python script:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("WindowsSetupTest") \
    .getOrCreate()

print(f"PySpark Version: {spark.version}")
spark.stop()
```

---

## Option 3: Docker Setup (Clean & Isolated)

Run official Jupyter Docker image with PySpark pre-configured:

```bash
docker run -p 8888:8888 -p 4040:4040 \
  -v ${PWD}:/home/jovyan/work \
  jupyter/pyspark-notebook:latest
```

- Navigate to `http://localhost:8888` in your browser.
- Port `4040` exposes the Spark Web UI during job execution.

---

## Troubleshooting Common Setup Errors

| Error | Root Cause | Solution |
| :--- | :--- | :--- |
| `JAVA_HOME is not set` | PySpark cannot find JVM | Ensure `JAVA_HOME` points to JDK directory (not `bin`) and restart terminal. |
| `java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio` | Missing Windows Hadoop binaries | Set `HADOOP_HOME=C:\hadoop` and copy `hadoop.dll` to `C:\Windows\System32` or `C:\hadoop\bin`. |
| `Py4JJavaError` | Port conflict or firewall blocking local socket | Allow Java through Windows Defender Firewall. |
