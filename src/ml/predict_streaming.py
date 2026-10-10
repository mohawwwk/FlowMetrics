import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Ensure Hadoop native library on Windows is discoverable
hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
if os.path.exists(hadoop_bin) and hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.consumer.schema import ecommerce_schema
from src.ml.config import MLConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecommerce-ml-streaming")


def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("EcommerceML-StreamPredict")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str, starting_offsets: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 2000)
        .load()
    )


def read_file_stream(spark: SparkSession, path: str):
    return (
        spark.readStream
        .schema(
            spark.read.parquet(path).schema
        )
        .parquet(path)
    )


def clean_and_engineer(raw_df):
    schema = ecommerce_schema()

    parsed = raw_df.select(
        F.from_json(
            F.col("value").cast("string"),
            schema,
            options={"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ).alias("data"),
    )

    cleaned = (
        parsed
        .filter(F.col("data._corrupt_record").isNull())
        .select(
            "data.InvoiceNo", "data.StockCode", "data.Description",
            "data.Quantity", "data.InvoiceDate", "data.UnitPrice",
            "data.CustomerID", "data.Country",
        )
        .filter(
            F.col("InvoiceNo").isNotNull()
            & F.col("StockCode").isNotNull()
            & F.col("Quantity").isNotNull()
            & F.col("InvoiceDate").isNotNull()
            & F.col("UnitPrice").isNotNull()
            & F.col("CustomerID").isNotNull()
            & F.col("Country").isNotNull()
        )
        .filter(~F.col("InvoiceNo").startswith("C"))
        .filter(F.col("Quantity") > 0)
        .filter(F.col("UnitPrice") > 0)
        .withColumn("Quantity", F.col("Quantity").cast(IntegerType()))
        .withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
        .withColumn("InvoiceTimestamp", F.to_timestamp(F.col("InvoiceDate"), "yyyy/M/d H:mm"))
        .withColumn("Revenue", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
        .withColumn("Hour", F.hour("InvoiceTimestamp"))
        .withColumn("DayOfWeek", F.dayofweek("InvoiceTimestamp"))
        .withColumn("StockPrefix", F.substring("StockCode", 1, 2))
    )
    return cleaned


def parse_args():
    parser = argparse.ArgumentParser(description="Predict revenue on streaming Kafka or file data.")
    parser.add_argument("--source", choices=["kafka", "file"], default="kafka", help="Streaming input source.")
    parser.add_argument("--data-path", default="data/processed/cleaned", help="Path for file-based streaming source.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="ecommerce-transactions")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--max-runtime-seconds", type=int, default=60)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--sqlite-sink", action="store_true", help="Persist streaming predictions to SQLite database.")
    parser.add_argument("--db-path", default="data/ecommerce.db", help="Path to SQLite database.")
    return parser.parse_args()


def main():
    args = parse_args()
    config = MLConfig()

    model_path = Path(config.model_output_dir)
    if not model_path.exists():
        logger.error("Trained model not found at %s. Run --train first.", model_path)
        sys.exit(1)

    spark = create_spark()
    db_pool = None
    try:
        logger.info("Loading saved PipelineModel from %s", model_path)
        model = PipelineModel.load(str(model_path))
        logger.info("Model loaded. Stages: %d", len(model.stages))

        if args.sqlite_sink:
            from src.database.config import DatabaseConfig
            from src.database.connection_pool import SQLiteConnectionPool
            from src.database.operations import DatabaseOperations
            from src.database.schema import create_schema

            db_cfg = DatabaseConfig(db_path=args.db_path)
            db_pool = SQLiteConnectionPool(db_cfg)
            with db_pool.get_connection() as c:
                create_schema(c)
            db_ops = DatabaseOperations(db_pool)
            logger.info("SQLite sink initialized at %s", args.db_path)

        if args.source == "kafka":
            raw_df = read_kafka_stream(spark, args.bootstrap_servers, args.topic, args.starting_offsets)
            logger.info("Kafka stream connected: topic=%s starting=%s", args.topic, args.starting_offsets)
            stream_df = clean_and_engineer(raw_df)
        else:
            logger.info("Connecting file stream to %s", args.data_path)
            raw_file_stream = read_file_stream(spark, args.data_path)
            stream_df = (
                raw_file_stream
                .withColumn("Hour", F.hour("InvoiceTimestamp"))
                .withColumn("DayOfWeek", F.dayofweek("InvoiceTimestamp"))
                .withColumn("StockPrefix", F.substring("StockCode", 1, 2))
            )

        predictions_df = model.transform(stream_df)

        output_df = predictions_df.select(
            "InvoiceNo", "StockCode", "Quantity", "UnitPrice", "Country",
            "Hour", "DayOfWeek", "Revenue", "prediction",
        ).withColumn(
            "abs_error", F.round(F.abs(F.col("Revenue") - F.col("prediction")), 2)
        )

        queries = []

        console_query = (
            output_df
            .writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .queryName("streaming_predictions_console")
            .start()
        )
        queries.append(console_query)

        if args.sqlite_sink:
            from src.database.spark_sink import write_predictions_batch

            def _batch_to_sqlite(b_df, b_id):
                write_predictions_batch(b_df, b_id, db_ops, model_name="RandomForestRegressor")

            sqlite_query = (
                predictions_df
                .writeStream
                .outputMode("append")
                .foreachBatch(_batch_to_sqlite)
                .trigger(processingTime=f"{args.trigger_seconds} seconds")
                .queryName("streaming_predictions_sqlite")
                .start()
            )
            queries.append(sqlite_query)

        logger.info("Streaming prediction queries started. Running for %d seconds.", args.max_runtime_seconds)
        end_time = time.time() + args.max_runtime_seconds
        while time.time() < end_time and any(q.isActive for q in queries):
            time.sleep(2)

        for q in queries:
            if q.isActive:
                q.stop()
        logger.info("Max runtime reached. Streaming queries stopped.")

    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as exc:
        logger.error("Fatal error: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
