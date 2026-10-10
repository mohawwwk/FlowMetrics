import argparse
import logging
import os
import sys
from pathlib import Path

# Ensure Hadoop native library on Windows is discoverable
hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
if os.path.exists(hadoop_bin) and hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.ml.config import MLConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecommerce-ml")


CATEGORICAL_COLS = ["Country", "StockPrefix"]
NUMERIC_COLS = ["Quantity", "UnitPrice", "Hour", "DayOfWeek"]


def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("EcommerceML-Train")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_cleaned(spark: SparkSession, config: MLConfig):
    df = spark.read.parquet(config.cleaned_parquet_path)
    logger.info("Loaded %d rows from %s", df.count(), config.cleaned_parquet_path)
    return df


def engineer_features(df):
    return (
        df
        .withColumn("Hour", F.hour("InvoiceTimestamp"))
        .withColumn("DayOfWeek", F.dayofweek("InvoiceTimestamp"))
        .withColumn("StockPrefix", F.substring("StockCode", 1, 2))
        .select(
            "InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice",
            "Country", "CustomerID", "InvoiceTimestamp", "Revenue",
            "Hour", "DayOfWeek", "StockPrefix",
        )
    )


def build_pipeline(config: MLConfig):
    country_indexer = StringIndexer(
        inputCol="Country", outputCol="Country_idx",
        handleInvalid="keep",
    )
    stock_prefix_indexer = StringIndexer(
        inputCol="StockPrefix", outputCol="StockPrefix_idx",
        handleInvalid="keep",
    )

    encoder = OneHotEncoder(
        inputCols=["Country_idx", "StockPrefix_idx"],
        outputCols=["Country_vec", "StockPrefix_vec"],
        handleInvalid="keep",
    )

    assembler = VectorAssembler(
        inputCols=NUMERIC_COLS + ["Country_vec", "StockPrefix_vec"],
        outputCol=config.features_col,
        handleInvalid="keep",
    )

    lr = LinearRegression(
        featuresCol=config.features_col,
        labelCol=config.label_col,
        predictionCol=config.prediction_col,
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.0,
        standardization=True,
    )

    rf = RandomForestRegressor(
        featuresCol=config.features_col,
        labelCol=config.label_col,
        predictionCol=config.prediction_col,
        numTrees=50,
        maxDepth=10,
        seed=config.random_seed,
    )

    preprocessing = [country_indexer, stock_prefix_indexer, encoder, assembler]

    lr_pipeline = Pipeline(stages=preprocessing + [lr])
    rf_pipeline = Pipeline(stages=preprocessing + [rf])

    return lr_pipeline, rf_pipeline, preprocessing


def evaluate(predictions, label_col, prediction_col):
    rmse_eval = RegressionEvaluator(
        labelCol=label_col, predictionCol=prediction_col, metricName="rmse"
    )
    mae_eval = RegressionEvaluator(
        labelCol=label_col, predictionCol=prediction_col, metricName="mae"
    )
    r2_eval = RegressionEvaluator(
        labelCol=label_col, predictionCol=prediction_col, metricName="r2"
    )
    return {
        "rmse": rmse_eval.evaluate(predictions),
        "mae": mae_eval.evaluate(predictions),
        "r2": r2_eval.evaluate(predictions),
    }


def train_and_evaluate(spark, config: MLConfig, save: bool = True):
    df = load_cleaned(spark, config)
    df = engineer_features(df)

    train_df, test_df = df.randomSplit(
        [1.0 - config.test_split_ratio, config.test_split_ratio],
        seed=config.random_seed,
    )
    logger.info("Train rows: %d | Test rows: %d", train_df.count(), test_df.count())

    lr_pipeline, rf_pipeline, preprocessing = build_pipeline(config)

    logger.info("Training Linear Regression...")
    lr_model = lr_pipeline.fit(train_df)
    lr_predictions = lr_model.transform(test_df)
    lr_metrics = evaluate(lr_predictions, config.label_col, config.prediction_col)
    logger.info("Linear Regression — RMSE: %.4f | MAE: %.4f | R2: %.4f",
                lr_metrics["rmse"], lr_metrics["mae"], lr_metrics["r2"])

    logger.info("Training Random Forest Regression...")
    rf_model = rf_pipeline.fit(train_df)
    rf_predictions = rf_model.transform(test_df)
    rf_metrics = evaluate(rf_predictions, config.label_col, config.prediction_col)
    logger.info("Random Forest     — RMSE: %.4f | MAE: %.4f | R2: %.4f",
                rf_metrics["rmse"], rf_metrics["mae"], rf_metrics["r2"])

    lr_stages = lr_model.stages
    lr_regressor = lr_stages[-1]
    logger.info("Linear Regression intercept: %.4f", lr_regressor.intercept)

    best_model_name = "LinearRegression" if lr_metrics["rmse"] <= rf_metrics["rmse"] else "RandomForest"
    best_model = lr_model if lr_metrics["rmse"] <= rf_metrics["rmse"] else rf_model
    logger.info("Best model by RMSE: %s", best_model_name)

    if save:
        import shutil
        model_path = config.model_output_dir
        if Path(model_path).exists():
            shutil.rmtree(model_path)
            logger.info("Removed existing model at %s", model_path)
        best_model.save(model_path)
        logger.info("Saved best model (%s) to %s", best_model_name, model_path)

    print(f"\n{'='*70}")
    print(f"MODEL EVALUATION RESULTS")
    print(f"{'='*70}")
    print(f"{'Model':<22} {'RMSE':>12} {'MAE':>12} {'R2':>12}")
    print(f"{'-'*70}")
    print(f"{'LinearRegression':<22} {lr_metrics['rmse']:>12.4f} {lr_metrics['mae']:>12.4f} {lr_metrics['r2']:>12.4f}")
    print(f"{'RandomForestRegressor':<22} {rf_metrics['rmse']:>12.4f} {rf_metrics['mae']:>12.4f} {rf_metrics['r2']:>12.4f}")
    print(f"{'-'*70}")
    print(f"Best model: {best_model_name} (lower RMSE)")
    print(f"{'='*70}\n")

    print("PREDICTION EXAMPLES (test set — first 10):")
    rf_predictions.select(
        "InvoiceNo", "StockCode", "Quantity", "UnitPrice", "Country",
        "Revenue", "prediction",
    ).withColumn(
        "abs_error", F.round(F.abs(F.col("Revenue") - F.col("prediction")), 2)
    ).show(10, truncate=False)

    return best_model, best_model_name, {"lr": lr_metrics, "rf": rf_metrics}


def load_and_predict(spark, config: MLConfig, sample_path: str = None):
    model_path = Path(config.model_output_dir)
    if not model_path.exists():
        logger.error("Model not found at %s. Train first with --train.", model_path)
        sys.exit(1)

    logger.info("Loading saved model from %s", model_path)
    model = PipelineModel.load(str(model_path))

    if sample_path:
        df = spark.read.parquet(sample_path)
    else:
        df = spark.read.parquet(config.cleaned_parquet_path)

    df = engineer_features(df)
    predictions = model.transform(df)

    print(f"\n{'='*70}")
    print(f"LOADED MODEL PREDICTIONS (first 15 rows)")
    print(f"{'='*70}")
    predictions.select(
        "InvoiceNo", "StockCode", "Quantity", "Country",
        "Revenue", "prediction",
    ).withColumn(
        "abs_error", F.round(F.abs(F.col("Revenue") - F.col("prediction")), 2)
    ).show(15, truncate=False)

    metrics = evaluate(predictions, config.label_col, config.prediction_col)
    logger.info("Loaded model on full cleaned set — RMSE: %.4f | MAE: %.4f | R2: %.4f",
                metrics["rmse"], metrics["mae"], metrics["r2"])
    return predictions


def parse_args():
    parser = argparse.ArgumentParser(description="Train / load revenue prediction model.")
    parser.add_argument("--train", action="store_true", help="Train and save the model.")
    parser.add_argument("--predict", action="store_true", help="Load saved model and predict.")
    parser.add_argument("--cleaned-path", default=None, help="Override cleaned parquet path.")
    return parser.parse_args()


def main():
    args = parse_args()
    config = MLConfig()
    if args.cleaned_path:
        config.cleaned_parquet_path = args.cleaned_path

    spark = create_spark()
    try:
        if args.train:
            train_and_evaluate(spark, config, save=True)
        elif args.predict:
            load_and_predict(spark, config)
        else:
            logger.info("No action specified. Use --train or --predict.")
            train_and_evaluate(spark, config, save=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
