# Phase 4 — Machine Learning with Spark MLlib

> **Phase 4 Deliverable:** An end-to-end Machine Learning pipeline built on Apache Spark MLlib (`pyspark.ml`) to predict e-commerce purchase amounts (line-item revenue). Includes data preparation, feature engineering, pipeline assembly (`StringIndexer`, `OneHotEncoder`, `VectorAssembler`), train-test splitting, model training (`LinearRegression` vs `RandomForestRegressor`), evaluation metrics (`RMSE`, `MAE`, `R²`), prediction examples, model serialization (save & load), real-time streaming inference over Kafka / micro-batches, and a comprehensive test suite.
>
> **Project Status:** Phase 4 COMPLETE. Pipeline verified and tested. Stop after Phase 4.

---

## 1. Architecture Overview

```
                                 BATCH TRAINING PIPELINE
  ┌────────────────────────┐
  │ Cleaned Parquet Data   │ ──▶ Feature Engineering (Hour, DayOfWeek, StockPrefix)
  │ data/processed/cleaned │
  └────────────────────────┘
                                     │
                                     ▼
                        ┌─────────────────────────┐
                        │   Spark MLlib Pipeline  │
                        │ 1. StringIndexer        │ (Country, StockPrefix)
                        │ 2. OneHotEncoder        │ (Country_idx, StockPrefix_idx)
                        │ 3. VectorAssembler      │ (Numerics + One-Hot Vectors)
                        │ 4. Regressor Estimator  │ (LinearRegression / RandomForest)
                        └─────────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
             Train Set (80%)                    Test Set (20%)
                    │                                 │
              Model Fitting                     Model Evaluation
             (.fit(train_df))                 (RMSE, MAE, R²)
                    │                                 │
                    ▼                                 ▼
         Best PipelineModel ◀─────────────────── Compare Models
                    │
                    ▼
          Save to Disk (models/revenue_model/)
                    │
                    │ Load PipelineModel (.load())
                    ▼
─────────────────────────────────────────────────────────────────────────────
                             STREAMING INFERENCE
  ┌─────────────────────────┐
  │ Kafka / File Stream     │
  │ topic: ecommerce-       │
  │ transactions            │
  └─────────────────────────┘
              │
              ▼
    Clean & Parse Schema
              │
              ▼
   Trained PipelineModel (.transform(streaming_df))
              │
              ▼
   Streaming Predictions & Error Tracking (Console / Sink)
   [InvoiceNo | StockCode | Quantity | UnitPrice | Revenue | prediction | abs_error]
```

---

## 2. Requirements Compliance Checklist

| Requirement | Implementation Detail | Status |
|-------------|-----------------------|:------:|
| **Use Spark MLlib** | Implemented using `pyspark.ml` Estimators, Transformers, and Evaluators | ✅ Verified |
| **Train a regression model** | Predict line-item `Revenue` (`Quantity * UnitPrice`) | ✅ Verified |
| **Create pipeline** | `Pipeline(stages=[country_idx, prefix_idx, encoder, assembler, regressor])` | ✅ Verified |
| **StringIndexer** | Categorical indexing with `handleInvalid="keep"` for robustness | ✅ Verified |
| **OneHotEncoder** | Sparse binary encoding of indexed categorical features | ✅ Verified |
| **VectorAssembler** | Merges numeric columns and encoded vectors into `features` | ✅ Verified |
| **Train-test split** | 80/20 deterministic split with `randomSplit(seed=42)` | ✅ Verified |
| **Evaluation metrics** | `RegressionEvaluator` calculating `RMSE`, `MAE`, and `R²` | ✅ Verified |
| **RMSE** | Root Mean Squared Error computed and compared | ✅ Verified |
| **MAE** | Mean Absolute Error computed and compared | ✅ Verified |
| **Prediction examples** | 10 sample predictions with actual, predicted, and absolute error | ✅ Verified |
| **Save model** | Fitted `PipelineModel` saved to `models/revenue_model/` | ✅ Verified |
| **Load model** | `PipelineModel.load()` loads saved model for batch and streaming prediction | ✅ Verified |
| **Predict on streaming data** | `src/ml/predict_streaming.py` scores real-time Kafka or file micro-batches | ✅ Verified |
| **Explain every algorithm** | Detailed mathematical and architectural explanations included | ✅ Verified |
| **Test everything** | Automated test suite in `tests/test_ml.py` (5/5 tests passed) | ✅ Verified |
| **Stop after Phase 4** | All Phase 4 requirements satisfied; stopped before Phase 5 | ✅ Complete |

---

## 3. Machine Learning Algorithms & Pipeline Stages Explained

### 3.1 Feature Engineering (`engineer_features`)
Prior to feeding raw transactions into Spark MLlib, temporal and structural features are extracted:
- **`Hour`**: Extracted from `InvoiceTimestamp` (`0` to `23`). Captures intraday shopping patterns (e.g., peak afternoon wholesale orders vs evening retail browsing).
- **`DayOfWeek`**: Extracted from `InvoiceTimestamp` (`1` to `7`). Captures day-of-week purchasing behavior.
- **`StockPrefix`**: First 2 characters of `StockCode` (`substring("StockCode", 1, 2)`). Represents high-level product merchandise categories.
- **`Quantity` & `UnitPrice`**: Core transactional continuous features.

### 3.2 StringIndexer
- **Concept:** Spark MLlib algorithms require numerical representations. `StringIndexer` maps distinct string category values to numerical indices ordered by label frequency (`0.0` to `K-1`).
- **Columns Indexed:**
  - `Country` ➔ `Country_idx`
  - `StockPrefix` ➔ `StockPrefix_idx`
- **Invalid Handling (`handleInvalid="keep"`):** In production streaming systems, incoming transactions may contain unobserved countries or new product codes. Setting `handleInvalid="keep"` assigns unobserved categories to a dedicated index bin (`K`), preventing runtime crashes.

### 3.3 OneHotEncoder
- **Concept:** Passing raw index values (`0, 1, 2, ...`) directly into a regression algorithm imposes an artificial ordinal relationship (e.g., implying Country 2 is "twice" Country 1). `OneHotEncoder` converts each index into a sparse binary vector with a single non-zero entry at the category index.
- **Vector Sparsity:** Spark MLlib stores these as `SparseVector` instances (`size`, `indices`, `values`), requiring minimal memory footprint even for high-cardinality features.
- **Columns Encoded:**
  - `Country_idx` ➔ `Country_vec`
  - `StockPrefix_idx` ➔ `StockPrefix_vec`

### 3.4 VectorAssembler
- **Concept:** Spark MLlib regressors expect a single feature vector column (named `features`) per row. `VectorAssembler` concatenates continuous numeric scalar columns and sparse categorical vectors into a unified feature vector.
- **Inputs Assembled:**
  `["Quantity", "UnitPrice", "Hour", "DayOfWeek", "Country_vec", "StockPrefix_vec"]`
- **Output:** `features` vector column of type `VectorUDT`.

### 3.5 Train-Test Split
- **Method:** `df.randomSplit([0.8, 0.2], seed=42)`.
- **Why 80/20 with Seed:**
  - **80% Training (1,392 rows):** Gives the estimators sufficient data to learn coefficients and tree split points.
  - **20% Test (292 rows):** Held out completely from fitting to prevent data leakage and ensure an unbiased out-of-sample evaluation.
  - **Deterministic Seed (`42`):** Guarantees exact reproducibility across runs and testing environments.

### 3.6 Linear Regression (`LinearRegression`)
- **Mathematical Formulation:**
  $$\hat{y}_i = \mathbf{w}^T \mathbf{x}_i + b$$
- **Objective Function (with $L_2$ Ridge Regularization):**
  $$\min_{\mathbf{w}, b} \frac{1}{2n} \sum_{i=1}^n (y_i - (\mathbf{w}^T \mathbf{x}_i + b))^2 + \frac{\lambda}{2} \|\mathbf{w}\|_2^2$$
- **Hyperparameters:**
  - `regParam = 0.1` ($L_2$ penalty to control multicollinearity from one-hot features)
  - `elasticNetParam = 0.0` (pure Ridge regression)
  - `standardization = True` (standardizes feature variance before fitting)
  - `maxIter = 100`
- **Strengths:** High interpretability, analytical tractability, fast training.
- **Limitations:** Only captures linear additive relationships; struggles with non-linear interactions without explicit polynomial feature expansion.

### 3.7 Random Forest Regressor (`RandomForestRegressor`)
- **Mathematical Formulation:** An ensemble of $B$ decorrelated regression decision trees.
  $$\hat{y}_i = \frac{1}{B} \sum_{b=1}^B T_b(\mathbf{x}_i)$$
  where each tree $T_b$ recursively partitions the feature space into hyper-rectangles $R_m$ to minimize the variance / Mean Squared Error of the split:
  $$\text{MSE}(R) = \frac{1}{|R|} \sum_{i \in R} (y_i - \bar{y}_R)^2$$
- **Ensemble Techniques Employed:**
  - **Bootstrap Aggregating (Bagging):** Each tree is trained on a bootstrap sample of the training set drawn with replacement, reducing model variance without increasing bias.
  - **Feature Subsampling:** At each node split, a random subset of features is evaluated, decorrelating individual trees.
- **Hyperparameters:**
  - `numTrees = 50`
  - `maxDepth = 10`
  - `seed = 42`
- **Strengths:** Captures complex non-linear feature interactions (such as item price combined with quantity tiers), robust to outliers, invariant to monotonic feature scaling.

---

## 4. Evaluation Metrics Explained

1. **RMSE (Root Mean Squared Error):**
   $$\text{RMSE} = \sqrt{\frac{1}{n} \sum_{i=1}^n (y_i - \hat{y}_i)^2}$$
   - Measures the standard deviation of residuals.
   - Heavily penalizes large prediction mistakes because errors are squared before averaging.

2. **MAE (Mean Absolute Error):**
   $$\text{MAE} = \frac{1}{n} \sum_{i=1}^n |y_i - \hat{y}_i|$$
   - Measures average absolute deviation between predicted and actual amounts.
   - Robust to extreme outliers; intuitive real-world dollar/pound error.

3. **$R^2$ (Coefficient of Determination):**
   $$R^2 = 1 - \frac{\sum_{i=1}^n (y_i - \hat{y}_i)^2}{\sum_{i=1}^n (y_i - \bar{y})^2}$$
   - Quantifies the proportion of variance in purchase amount explained by the model features.
   - Values close to `1.0` indicate near-ideal predictive accuracy.

---

## 5. Model Evaluation Results & Comparison

| Model | RMSE | MAE | $R^2$ | Status |
|:------|:----:|:---:|:----:|:------:|
| **Linear Regression** | 20.9404 | 8.0330 | 0.8079 | Baseline |
| **Random Forest Regressor** | **7.6315** | **4.7555** | **0.9745** | **Winner (Selected)** |

**Decision:**
The `RandomForestRegressor` outperformed `LinearRegression` significantly across all metrics:
- **RMSE reduction:** $20.94 \rightarrow 7.63$ (63.5% error reduction)
- **MAE reduction:** $8.03 \rightarrow 4.76$ (40.8% error reduction)
- **$R^2$ boost:** $0.8079 \rightarrow 0.9745$ (captures 97.45% of total variance)

The best model (`RandomForestRegressor`) was serialized to `models/revenue_model/`.

---

## 6. Prediction Examples (Test Set — First 10 Rows)

```
+---------+---------+--------+---------+--------------+-------+------------------+---------+
|InvoiceNo|StockCode|Quantity|UnitPrice|Country       |Revenue|prediction        |abs_error|
+---------+---------+--------+---------+--------------+-------+------------------+---------+
|536365   |21730    |6       |4.25     |United Kingdom|25.50  |22.40             |3.10     |
|536365   |22752    |2       |7.65     |United Kingdom|15.30  |19.71             |4.41     |
|536365   |71053    |6       |3.39     |United Kingdom|20.34  |20.82             |0.48     |
|536365   |84029E   |6       |3.39     |United Kingdom|20.34  |21.89             |1.55     |
|536365   |84029G   |6       |3.39     |United Kingdom|20.34  |21.89             |1.55     |
|536365   |84406B   |8       |2.75     |United Kingdom|22.00  |22.14             |0.14     |
|536366   |22632    |6       |1.85     |United Kingdom|11.10  |16.06             |4.96     |
|536366   |22633    |6       |1.85     |United Kingdom|11.10  |16.06             |4.96     |
|536367   |22310    |6       |1.65     |United Kingdom|9.90   |15.63             |5.73     |
|536367   |22310    |6       |1.65     |United Kingdom|9.90   |15.63             |5.73     |
+---------+---------+--------+---------+--------------+-------+------------------+---------+
```

---

## 7. Model Persistence (Save & Load)

1. **Saving the Best Model:**
   ```python
   # train_model.py
   model_path = config.model_output_dir  # models/revenue_model
   best_model.save(model_path)
   ```
   Spark MLlib writes:
   - `models/revenue_model/metadata/`: Pipeline stages, UIDs, Spark version (`3.5.3`), and configuration parameters.
   - `models/revenue_model/stages/`: Serialized models and indexing dictionaries for each stage:
     - `0_StringIndexer_...`
     - `1_StringIndexer_...`
     - `2_OneHotEncoder_...`
     - `3_VectorAssembler_...`
     - `4_RandomForestRegressor_...`

2. **Loading the Model:**
   ```python
   from pyspark.ml import PipelineModel
   model = PipelineModel.load("models/revenue_model")
   predictions = model.transform(new_df)
   ```
   The loaded pipeline seamlessly executes all preprocessing stages and regressor scoring without needing manual dictionary reloading.

---

## 8. Real-Time Streaming Prediction

`src/ml/predict_streaming.py` loads the saved `PipelineModel` and applies it to incoming streaming DataFrames in real time.

### Streaming Execution Modes
1. **Kafka Stream (Production Mode):**
   ```powershell
   python src/ml/predict_streaming.py --source kafka --bootstrap-servers localhost:9092 --topic ecommerce-transactions
   ```
2. **File / Parquet Micro-Batch Stream (Verification Mode):**
   ```powershell
   python src/ml/predict_streaming.py --source file --data-path data/processed/cleaned --max-runtime-seconds 15 --trigger-seconds 3
   ```

### Real-Time Streaming Output (Batch 0 Sample)
```
-------------------------------------------
Batch: 0
-------------------------------------------
+---------+---------+--------+---------+--------------+----+---------+-------+------------------+---------+
|InvoiceNo|StockCode|Quantity|UnitPrice|Country       |Hour|DayOfWeek|Revenue|prediction        |abs_error|
+---------+---------+--------+---------+--------------+----+---------+-------+------------------+---------+
|536365   |85123A   |6       |2.55     |United Kingdom|8   |4        |15.30  |15.89             |0.59     |
|536365   |71053    |6       |3.39     |United Kingdom|8   |4        |20.34  |20.82             |0.48     |
|536365   |84406B   |8       |2.75     |United Kingdom|8   |4        |22.00  |22.14             |0.14     |
|536365   |84029G   |6       |3.39     |United Kingdom|8   |4        |20.34  |21.89             |1.55     |
|536365   |84029E   |6       |3.39     |United Kingdom|8   |4        |20.34  |21.89             |1.55     |
|536365   |22752    |2       |7.65     |United Kingdom|8   |4        |15.30  |19.71             |4.41     |
|536365   |21730    |6       |4.25     |United Kingdom|8   |4        |25.50  |22.40             |3.10     |
|536366   |22633    |6       |1.85     |United Kingdom|8   |4        |11.10  |16.06             |4.96     |
|536366   |22632    |6       |1.85     |United Kingdom|8   |4        |11.10  |16.06             |4.96     |
|536367   |84879    |32      |1.69     |United Kingdom|8   |4        |54.08  |57.33             |3.25     |
+---------+---------+--------+---------+--------------+----+---------+-------+------------------+---------+
only showing top 20 rows
```

---

## 9. Test Suite Verification (`tests/test_ml.py`)

A comprehensive test suite verifies every requirement:

| Test Case | Purpose | Result |
|-----------|---------|:------:|
| `test_feature_engineering` | Verifies temporal & structural columns (`Hour`, `DayOfWeek`, `StockPrefix`) are correctly derived | **PASSED** |
| `test_pipeline_construction` | Validates pipeline stages and `handleInvalid="keep"` parameter presence | **PASSED** |
| `test_model_training_and_evaluation` | Trains LinearRegression and RandomForest; asserts RMSE, MAE, R² computation | **PASSED** |
| `test_model_save_and_load` | Verifies model serialization to disk, deserialization, and deterministic output equality | **PASSED** |
| `test_streaming_prediction_microbatch` | Verifies streaming DataFrame inference across streaming micro-batches with memory sink | **PASSED** |

### Test Execution Output
```
============================= test session starts =============================
platform win32 -- Python 3.11.9, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\Users\sidha\Documents\trae_projects\BigData
collected 5 items

tests/test_ml.py::TestEcommerceML::test_feature_engineering PASSED       [ 20%]
tests/test_ml.py::TestEcommerceML::test_model_save_and_load PASSED       [ 40%]
tests/test_ml.py::TestEcommerceML::test_model_training_and_evaluation PASSED [ 60%]
tests/test_ml.py::TestEcommerceML::test_pipeline_construction PASSED     [ 80%]
tests/test_ml.py::TestEcommerceML::test_streaming_prediction_microbatch PASSED [100%]

======================== 5 passed in 142.96s (0:02:22) ========================
```

---

## 10. How to Run Phase 4

### 10.1 Train and Save the Model
```powershell
python src/ml/train_model.py --train
```

### 10.2 Load the Saved Model and Predict
```powershell
python src/ml/train_model.py --predict
```

### 10.3 Run Streaming Prediction
```powershell
# Using Kafka topic:
python src/ml/predict_streaming.py --source kafka --bootstrap-servers localhost:9092 --topic ecommerce-transactions

# Using file/parquet stream:
python src/ml/predict_streaming.py --source file --data-path data/processed/cleaned --max-runtime-seconds 30 --trigger-seconds 5
```

### 10.4 Run the Test Suite
```powershell
pytest tests/test_ml.py -v
```

---

## 11. Completion Notice

**Phase 4 is complete.** All requirements (Spark MLlib pipeline, StringIndexer, OneHotEncoder, VectorAssembler, train-test split, RMSE, MAE, prediction examples, save model, load model, predict on streaming data, algorithm explanations, test suite) are fully implemented and verified. Execution is halted after Phase 4 as instructed.
