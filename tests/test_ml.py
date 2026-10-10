import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# Ensure Hadoop native library on Windows is discoverable
hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
if os.path.exists(hadoop_bin) and hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.ml import PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Insert project root into sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ml.config import MLConfig
from src.ml.train_model import build_pipeline, engineer_features, evaluate


class TestEcommerceML(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("UnitTest-EcommerceML")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        # Create deterministic synthetic dataset
        schema = StructType([
            StructField("InvoiceNo", StringType(), False),
            StructField("StockCode", StringType(), False),
            StructField("Description", StringType(), True),
            StructField("Quantity", IntegerType(), False),
            StructField("UnitPrice", DoubleType(), False),
            StructField("Country", StringType(), False),
            StructField("CustomerID", StringType(), True),
            StructField("InvoiceTimestamp", TimestampType(), False),
            StructField("Revenue", DoubleType(), False),
        ])

        rows = [
            ("500001", "85123A", "HEART LIGHT", 6, 2.55, "United Kingdom", "17850", "2020-01-01 08:30:00", 15.30),
            ("500002", "71053", "LANTERN", 10, 3.39, "United Kingdom", "17850", "2020-01-01 09:15:00", 33.90),
            ("500003", "84406B", "CUPID HEART", 8, 2.75, "France", "12583", "2020-01-01 10:45:00", 22.00),
            ("500004", "84029G", "UNION FLAG", 12, 3.39, "Germany", "12600", "2020-01-02 11:00:00", 40.68),
            ("500005", "22752", "BOXWOOD CUSHION", 2, 7.65, "United Kingdom", "13047", "2020-01-02 14:20:00", 15.30),
            ("500006", "21730", "GLASS HANGER", 4, 4.25, "France", "12583", "2020-01-03 15:10:00", 17.00),
            ("500007", "22633", "PILLOW CASE", 20, 1.85, "United Kingdom", "17850", "2020-01-03 16:05:00", 37.00),
            ("500008", "84879", "BIRD ORNAMENT", 32, 1.69, "United Kingdom", "13047", "2020-01-04 11:30:00", 54.08),
            ("500009", "22310", "RETRO CAN", 15, 1.65, "Germany", "12600", "2020-01-04 12:45:00", 24.75),
            ("500010", "85099B", "JUMBO BAG", 50, 1.95, "United Kingdom", "17850", "2020-01-05 13:10:00", 97.50),
            ("500011", "21931", "JUMBO STORAGE", 25, 1.95, "EIRE", "14911", "2020-01-05 14:40:00", 48.75),
            ("500012", "84997D", "CHILDRENS CUTLERY", 16, 4.15, "United Kingdom", "13047", "2020-01-06 15:55:00", 66.40),
        ]

        # Convert strings to timestamp in spark dataframe
        raw_df = cls.spark.createDataFrame(
            [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]) for r in rows],
            schema=[
                "InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice",
                "Country", "CustomerID", "InvoiceDateStr", "Revenue",
            ],
        ).withColumn("InvoiceTimestamp", F.to_timestamp("InvoiceDateStr"))

        cls.sample_df = raw_df.drop("InvoiceDateStr")
        cls.config = MLConfig(random_seed=42)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_feature_engineering(self):
        """Test feature engineering creates Hour, DayOfWeek, StockPrefix correctly."""
        engineered = engineer_features(self.sample_df)
        cols = engineered.columns
        self.assertIn("Hour", cols)
        self.assertIn("DayOfWeek", cols)
        self.assertIn("StockPrefix", cols)

        first_row = engineered.first()
        self.assertEqual(first_row["Hour"], 8)
        self.assertEqual(first_row["StockPrefix"], "85")

    def test_pipeline_construction(self):
        """Test build_pipeline returns valid LinearRegression and RandomForest pipelines."""
        lr_pipe, rf_pipe, preprocessing = build_pipeline(self.config)

        self.assertEqual(len(preprocessing), 4)
        self.assertIsInstance(preprocessing[0], StringIndexer)
        self.assertIsInstance(preprocessing[1], StringIndexer)
        self.assertIsInstance(preprocessing[2], OneHotEncoder)
        self.assertIsInstance(preprocessing[3], VectorAssembler)

        self.assertEqual(preprocessing[0].getHandleInvalid(), "keep")
        self.assertEqual(preprocessing[1].getHandleInvalid(), "keep")
        self.assertEqual(preprocessing[2].getHandleInvalid(), "keep")
        self.assertEqual(preprocessing[3].getHandleInvalid(), "keep")

        self.assertEqual(len(lr_pipe.getStages()), 5)
        self.assertEqual(len(rf_pipe.getStages()), 5)

    def test_model_training_and_evaluation(self):
        """Test training both models on engineered data and evaluating metrics."""
        engineered = engineer_features(self.sample_df)
        lr_pipe, rf_pipe, _ = build_pipeline(self.config)

        # Fit models
        lr_model = lr_pipe.fit(engineered)
        rf_model = rf_pipe.fit(engineered)

        lr_preds = lr_model.transform(engineered)
        rf_preds = rf_model.transform(engineered)

        self.assertIn("prediction", lr_preds.columns)
        self.assertIn("prediction", rf_preds.columns)

        lr_metrics = evaluate(lr_preds, self.config.label_col, self.config.prediction_col)
        rf_metrics = evaluate(rf_preds, self.config.label_col, self.config.prediction_col)

        for key in ["rmse", "mae", "r2"]:
            self.assertIn(key, lr_metrics)
            self.assertIn(key, rf_metrics)
            self.assertGreaterEqual(lr_metrics[key], 0.0)
            self.assertGreaterEqual(rf_metrics[key], 0.0)

    def test_model_save_and_load(self):
        """Test that saved PipelineModel can be loaded and yields identical predictions."""
        engineered = engineer_features(self.sample_df)
        lr_pipe, _, _ = build_pipeline(self.config)
        fitted_model = lr_pipe.fit(engineered)

        orig_preds = fitted_model.transform(engineered).select("InvoiceNo", "prediction").collect()

        temp_dir = tempfile.mkdtemp()
        save_path = os.path.join(temp_dir, "test_model")
        try:
            fitted_model.save(save_path)
            self.assertTrue(os.path.exists(save_path))

            loaded_model = PipelineModel.load(save_path)
            loaded_preds = loaded_model.transform(engineered).select("InvoiceNo", "prediction").collect()

            self.assertEqual(len(orig_preds), len(loaded_preds))
            for orig, loaded in zip(orig_preds, loaded_preds):
                self.assertEqual(orig["InvoiceNo"], loaded["InvoiceNo"])
                self.assertAlmostEqual(orig["prediction"], loaded["prediction"], places=4)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_streaming_prediction_microbatch(self):
        """Test PipelineModel on a streaming DataFrame micro-batch."""
        engineered = engineer_features(self.sample_df)
        _, rf_pipe, _ = build_pipeline(self.config)
        fitted_model = rf_pipe.fit(engineered)

        # Create temporary parquet streaming source
        temp_dir = tempfile.mkdtemp()
        stream_input = os.path.join(temp_dir, "stream_in")
        checkpoint_dir = os.path.join(temp_dir, "checkpoint")
        try:
            engineered.write.parquet(stream_input)

            read_schema = self.spark.read.parquet(stream_input).schema
            streaming_df = self.spark.readStream.schema(read_schema).parquet(stream_input)

            # Apply PipelineModel as a streaming transformer
            scored_stream = fitted_model.transform(streaming_df)
            self.assertIn("prediction", scored_stream.columns)

            query = (
                scored_stream.writeStream
                .format("memory")
                .queryName("test_stream_sink")
                .outputMode("append")
                .start(checkpointLocation=checkpoint_dir)
            )

            query.processAllAvailable()
            query.stop()

            # Verify predictions in memory sink
            sink_df = self.spark.sql("SELECT * FROM test_stream_sink")
            self.assertGreater(sink_df.count(), 0)
            self.assertIn("prediction", sink_df.columns)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
