# Module 08: Distributed Classification, Regression & Hyperparameter Tuning

Training scalable predictive models using PySpark MLlib.

---

## 1. Train/Test Split & Logistic Regression

```python
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

spark = SparkSession.builder.master("local[*]").appName("ClassificationDemo").getOrCreate()

# Assume prepared_data has columns: 'scaled_features' and 'label'
train_df, test_df = prepared_data.randomSplit([0.8, 0.2], seed=42)

# Instantiate Estimator
lr = LogisticRegression(
    featuresCol="scaled_features",
    labelCol="churned",
    maxIter=20,
    regParam=0.01
)

# Train the model
lr_model = lr.fit(train_df)

# Inference on test set
predictions = lr_model.transform(test_df)
predictions.select("churned", "prediction", "probability").show(5, truncate=False)
```

---

## 2. Model Evaluation (ROC-AUC)

```python
evaluator = BinaryClassificationEvaluator(
    labelCol="churned",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

roc_auc = evaluator.evaluate(predictions)
print(f"Test Set ROC-AUC: {roc_auc:.4f}")
```

---

## 3. Hyperparameter Tuning with Cross-Validation

PySpark parallelizes hyperparameter grid searches across cluster workers:

```python
# Build parameter grid
param_grid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.001, 0.01, 0.1]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

# 3-Fold Cross-Validation
cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    parallelism=4 # evaluate 4 parameter combinations concurrently
)

cv_model = cv.fit(train_df)
best_model = cv_model.bestModel
print(f"Best RegParam: {best_model._java_obj.getRegParam()}")
```
