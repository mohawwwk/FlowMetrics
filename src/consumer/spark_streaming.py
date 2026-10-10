import argparse
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.consumer.config import StreamingConfig
from src.consumer.schema import ecommerce_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecommerce-streaming")


def create_spark_session(config: StreamingConfig) -> SparkSession:
    spark = (
        SparkSession.builder
        .master(config.master)
        .appName(config.app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.streaming.checkpointLocation", config.checkpoint_dir)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created (app=%s, master=%s)", config.app_name, config.master)
    return spark


def read_kafka_stream(spark: SparkSession, config: StreamingConfig):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )


def parse_and_clean(raw_df, config: StreamingConfig):
    schema = ecommerce_schema()

    parsed = raw_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(
            F.col("value").cast("string"),
            schema,
            options={"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ).alias("data"),
    )

    exploded = parsed.select(
        "kafka_key", "kafka_offset", "kafka_timestamp",
        "data.InvoiceNo", "data.StockCode", "data.Description",
        "data.Quantity", "data.InvoiceDate", "data.UnitPrice",
        "data.CustomerID", "data.Country",
        F.col("data._corrupt_record").alias("_corrupt_record"),
    )

    malformed = exploded.filter(F.col("_corrupt_record").isNotNull())

    cleaned = (
        exploded
        .filter(F.col("_corrupt_record").isNull())
        .drop("_corrupt_record")
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
        .withColumn(
            "InvoiceTimestamp",
            F.to_timestamp(F.col("InvoiceDate"), "yyyy/M/d H:mm"),
        )
        .withColumn(
            "Revenue",
            F.round(F.col("Quantity") * F.col("UnitPrice"), 2),
        )
    )

    return cleaned, malformed


def build_queries(cleaned_df, malformed_df, config: StreamingConfig):
    cleaned_df = cleaned_df.withWatermark("InvoiceTimestamp", f"{config.watermark_seconds} seconds")

    cleaned_df.createOrReplaceTempView("cleaned")

    trigger = F.expr(f"{config.trigger_interval_seconds} seconds")

    cleaned_query = (
        cleaned_df
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .queryName("cleaned_records")
        .start(checkpointLocation=f"{config.checkpoint_dir}/cleaned")
    )

    cleaned_sink_query = (
        cleaned_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{config.processed_dir}/cleaned")
        .option("checkpointLocation", f"{config.checkpoint_dir}/cleaned_sink")
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .queryName("cleaned_sink")
        .start()
    )

    order_metrics = (
        cleaned_df
        .groupBy(F.window("InvoiceTimestamp", f"{config.tumbling_window_seconds} seconds"))
        .agg(
            F.approx_count_distinct("InvoiceNo").alias("orders_count"),
            F.count("*").alias("line_items_count"),
            F.sum("Revenue").alias("total_revenue"),
            F.round(F.sum("Revenue") / F.approx_count_distinct("InvoiceNo"), 2).alias("avg_order_value"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "orders_count",
            "line_items_count",
            F.round("total_revenue", 2).alias("total_revenue"),
            "avg_order_value",
        )
    )

    order_metrics_query = (
        order_metrics
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .queryName("order_metrics")
        .start(checkpointLocation=f"{config.checkpoint_dir}/order_metrics")
    )

    rolling_metrics = (
        cleaned_df
        .groupBy(
            F.window(
                "InvoiceTimestamp",
                f"{config.sliding_window_seconds} seconds",
                f"{config.sliding_slide_seconds} seconds",
            )
        )
        .agg(
            F.approx_count_distinct("InvoiceNo").alias("rolling_orders"),
            F.sum("Revenue").alias("rolling_revenue"),
            F.round(F.avg("Revenue"), 2).alias("rolling_avg_line_revenue"),
            F.round(F.sum("Revenue") / F.approx_count_distinct("InvoiceNo"), 2).alias("rolling_avg_order_value"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "rolling_orders",
            F.round("rolling_revenue", 2).alias("rolling_revenue"),
            "rolling_avg_line_revenue",
            "rolling_avg_order_value",
        )
    )

    rolling_query = (
        rolling_metrics
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .queryName("rolling_metrics")
        .start(checkpointLocation=f"{config.checkpoint_dir}/rolling_metrics")
    )

    popular_products = (
        cleaned_df
        .groupBy(
            F.window("InvoiceTimestamp", f"{config.tumbling_window_seconds} seconds"),
            "StockCode",
            "Description",
        )
        .agg(
            F.count("*").alias("items_sold"),
            F.sum("Quantity").alias("total_quantity"),
            F.sum("Revenue").alias("product_revenue"),
        )
        .orderBy(F.desc("items_sold"))
        .limit(10)
    )

    popular_products_query = (
        popular_products
        .writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 15)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .queryName("popular_products")
        .start(checkpointLocation=f"{config.checkpoint_dir}/popular_products")
    )

    top_customers = (
        cleaned_df
        .groupBy(
            F.window("InvoiceTimestamp", f"{config.tumbling_window_seconds} seconds"),
            "CustomerID",
        )
        .agg(
            F.approx_count_distinct("InvoiceNo").alias("customer_orders"),
            F.sum("Revenue").alias("customer_revenue"),
        )
        .orderBy(F.desc("customer_revenue"))
        .limit(10)
    )

    top_customers_query = (
        top_customers
        .writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 15)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .queryName("top_customers")
        .start(checkpointLocation=f"{config.checkpoint_dir}/top_customers")
    )

    malformed_query = (
        malformed_df
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 10)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .queryName("malformed_records")
        .start(checkpointLocation=f"{config.checkpoint_dir}/malformed")
    )

    return [
        cleaned_query,
        cleaned_sink_query,
        order_metrics_query,
        rolling_query,
        popular_products_query,
        top_customers_query,
        malformed_query,
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming consumer for e-commerce transactions."
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--checkpoint-dir", default=None)
    parser.add_argument("--processed-dir", default=None)
    parser.add_argument("--starting-offsets", default=None, choices=["earliest", "latest"])
    parser.add_argument("--trigger-seconds", type=int, default=None)
    parser.add_argument("--watermark-seconds", type=int, default=None)
    parser.add_argument("--max-runtime-seconds", type=int, default=60,
                        help="Max runtime in seconds before stopping (0 = run forever).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = StreamingConfig.from_overrides(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        checkpoint_dir=args.checkpoint_dir,
        processed_dir=args.processed_dir,
        starting_offsets=args.starting_offsets,
        trigger_interval_seconds=args.trigger_seconds,
        watermark_seconds=args.watermark_seconds,
    )

    logger.info("Starting Spark Structured Streaming Consumer")
    logger.info("Config: bootstrap_servers=%s topic=%s starting_offsets=%s",
                config.bootstrap_servers, config.topic, config.starting_offsets)

    spark = create_spark_session(config)

    try:
        raw_df = read_kafka_stream(spark, config)
        logger.info("Kafka source loaded: topic=%s", config.topic)

        cleaned_df, malformed_df = parse_and_clean(raw_df, config)
        logger.info("Schema parsed and cleaning pipeline built.")

        queries = build_queries(cleaned_df, malformed_df, config)
        logger.info("Started %d streaming queries.", len(queries))

        max_runtime = args.max_runtime_seconds
        if max_runtime and max_runtime > 0:
            logger.info("Running for %d seconds (max_runtime_seconds)...", max_runtime)
            end_time = time.time() + max_runtime
            while time.time() < end_time:
                time.sleep(2)
                if not any(q.isActive for q in queries):
                    logger.warning("One or more queries stopped.")
                    break
            logger.info("Max runtime reached. Stopping all queries.")
            for q in queries:
                if q.isActive:
                    q.stop()
        else:
            logger.info("Running indefinitely. Press Ctrl+C to stop.")
            spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as exc:
        logger.error("Fatal streaming error: %s", exc, exc_info=True)
        raise
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
