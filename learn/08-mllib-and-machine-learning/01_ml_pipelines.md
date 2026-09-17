# Module 08: Spark MLlib Pipelines & Feature Engineering

Spark's **MLlib** is designed for distributed machine learning on datasets that exceed single-machine memory (unlike scikit-learn).

---

## 1. Core Concepts: Transformers, Estimators & Pipelines

- **Transformer**: An algorithm that converts one DataFrame into another by appending columns (implements `.transform()`). Example: `StringIndexer`, `VectorAssembler`.
- **Estimator**: An algorithm that fits on a DataFrame to produce a Transformer (implements `.fit()`). Example: `LogisticRegression`, `RandomForestRegressor`.
- **Pipeline**: Chains multiple Transformers and Estimators together into an end-to-end, reproducible workflow.

---

## 2. Feature Engineering Pipeline in PySpark

MLlib algorithms require all numerical feature columns to be packed into a single **Vector column** (usually named `features`).

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler

spark = SparkSession.builder.master("local[*]").appName("MLPipelines").getOrCreate()

data = [
    (1, "Finance", 25, 55000.0, 1),
    (2, "Tech", 38, 120000.0, 0),
    (3, "Healthcare", 45, 85000.0, 1),
    (4, "Tech", 29, 95000.0, 0),
    (5, "Finance", 52, 110000.0, 1)
]
columns = ["id", "industry", "age", "salary", "churned"]
df = spark.createDataFrame(data, columns)

# Step 1: Convert categorical string column into numeric index
indexer = StringIndexer(inputCol="industry", outputCol="industry_idx")

# Step 2: One-Hot Encode categorical indices
encoder = OneHotEncoder(inputCol="industry_idx", outputCol="industry_vec")

# Step 3: Assemble all features into a single Vector column
assembler = VectorAssembler(
    inputCols=["industry_vec", "age", "salary"],
    outputCol="raw_features"
)

# Step 4: Scale feature vectors to unit variance
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="scaled_features",
    withMean=True,
    withStd=True
)

# Step 5: Assemble Pipeline and Fit
ml_pipeline = Pipeline(stages=[indexer, encoder, assembler, scaler])
pipeline_model = ml_pipeline.fit(df)
prepared_data = pipeline_model.transform(df)

prepared_data.select("id", "industry", "scaled_features", "churned").show(truncate=False)
```

---

## 3. Saving and Loading Pipelines

Fitted pipeline models can be saved to persistent cloud storage and reloaded into batch or streaming inference services:

```python
# Save fitted pipeline model
pipeline_model.save("models/feature_pipeline_v1")

# Load model in downstream service
from pyspark.ml import PipelineModel
loaded_model = PipelineModel.load("models/feature_pipeline_v1")
```
