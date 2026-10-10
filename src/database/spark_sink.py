import logging
import os
from typing import Optional

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.database.config import DatabaseConfig
from src.database.connection_pool import SQLiteConnectionPool
from src.database.operations import DatabaseOperations
from src.database.schema import create_schema

logger = logging.getLogger("ecommerce-spark-sink")


def write_transactions_batch(batch_df: DataFrame, batch_id: int, ops: DatabaseOperations) -> int:
    """
    ForeachBatch sink handler for streaming cleaned transactions into SQLite.
    Converts micro-batch rows into dictionary payloads and inserts via connection pool.
    """
    if batch_df.isEmpty():
        logger.debug("Microbatch %d is empty, skipping.", batch_id)
        return 0

    rows = batch_df.select(
        "InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice",
        "CustomerID", "Country", "InvoiceTimestamp", "Revenue",
    ).collect()

    records = [
        {
            "invoice_no": r["InvoiceNo"],
            "stock_code": r["StockCode"],
            "description": r["Description"],
            "quantity": r["Quantity"],
            "unit_price": r["UnitPrice"],
            "customer_id": r["CustomerID"],
            "country": r["Country"],
            "invoice_timestamp": str(r["InvoiceTimestamp"]),
            "revenue": r["Revenue"],
        }
        for r in rows
    ]

    inserted = ops.insert_transactions(records)
    logger.info("Batch %d: Inserted %d transactions into SQLite", batch_id, inserted)
    return inserted


def write_predictions_batch(
    batch_df: DataFrame,
    batch_id: int,
    ops: DatabaseOperations,
    model_name: str = "RandomForestRegressor",
) -> int:
    """
    ForeachBatch sink handler for streaming ML predictions into SQLite.
    """
    if batch_df.isEmpty():
        logger.debug("Prediction microbatch %d is empty, skipping.", batch_id)
        return 0

    rows = batch_df.select(
        "InvoiceNo", "StockCode", "Quantity", "UnitPrice", "Country",
        "Revenue", "prediction",
    ).withColumn(
        "abs_error", F.round(F.abs(F.col("Revenue") - F.col("prediction")), 2)
    ).collect()

    records = [
        {
            "invoice_no": r["InvoiceNo"],
            "stock_code": r["StockCode"],
            "quantity": r["Quantity"],
            "unit_price": r["UnitPrice"],
            "country": r["Country"],
            "actual_revenue": r["Revenue"],
            "predicted_revenue": round(float(r["prediction"]), 2),
            "abs_error": r["abs_error"],
            "model_name": model_name,
        }
        for r in rows
    ]

    inserted = ops.insert_predictions(records)
    logger.info("Batch %d: Inserted %d ML predictions into SQLite", batch_id, inserted)
    return inserted


def populate_database_from_processed_data(
    spark: SparkSession,
    config: Optional[DatabaseConfig] = None,
) -> dict[str, int]:
    """
    Bootstraps the SQLite database from Phase 3 cleaned Parquet data and Phase 4 trained model.
    Inserts processed transactions, calculates and stores predictions, computes customer analytics,
    and returns record statistics.
    """
    cfg = config or DatabaseConfig()
    pool = SQLiteConnectionPool(cfg)
    ops = DatabaseOperations(pool)

    # Initialize tables and indexes
    with pool.get_connection() as conn:
        create_schema(conn)

    results = {"transactions": 0, "predictions": 0, "customers": 0, "order_metrics": 0}

    # 1. Load Cleaned Parquet from Phase 3
    parquet_path = cfg.cleaned_parquet_path
    if not os.path.exists(parquet_path):
        logger.warning("Cleaned parquet path does not exist: %s", parquet_path)
        return results

    df = spark.read.parquet(parquet_path)
    logger.info("Loaded %d cleaned transactions from %s", df.count(), parquet_path)

    # Add required feature engineering columns if absent
    if "Hour" not in df.columns:
        df = df.withColumn("Hour", F.hour("InvoiceTimestamp"))
    if "DayOfWeek" not in df.columns:
        df = df.withColumn("DayOfWeek", F.dayofweek("InvoiceTimestamp"))
    if "StockPrefix" not in df.columns:
        df = df.withColumn("StockPrefix", F.substring("StockCode", 1, 2))

    # Insert transactions into SQLite
    tx_count = write_transactions_batch(df, batch_id=0, ops=ops)
    results["transactions"] = tx_count

    # 2. Compute customer analytics
    cust_count = ops.compute_and_update_customer_analytics()
    results["customers"] = cust_count
    logger.info("Updated %d customer records in customer_analytics", cust_count)

    # 3. Compute Tumbling Window Order Metrics (60 seconds tumbling)
    windowed = (
        df
        .groupBy(F.window("InvoiceTimestamp", "60 seconds"))
        .agg(
            F.approx_count_distinct("InvoiceNo").alias("orders_count"),
            F.count("*").alias("line_items_count"),
            F.round(F.sum("Revenue"), 2).alias("total_revenue"),
            F.round(F.sum("Revenue") / F.approx_count_distinct("InvoiceNo"), 2).alias("avg_order_value"),
        )
        .select(
            F.col("window.start").cast("string").alias("window_start"),
            F.col("window.end").cast("string").alias("window_end"),
            "orders_count",
            "line_items_count",
            "total_revenue",
            "avg_order_value",
        )
    )
    metric_rows = windowed.collect()
    metric_records = [
        {
            "window_start": r["window_start"],
            "window_end": r["window_end"],
            "orders_count": r["orders_count"],
            "line_items_count": r["line_items_count"],
            "total_revenue": r["total_revenue"],
            "avg_order_value": r["avg_order_value"],
        }
        for r in metric_rows
    ]
    w_count = ops.insert_order_metrics(metric_records)
    results["order_metrics"] = w_count
    logger.info("Inserted %d windowed order metric summaries into SQLite", w_count)

    # 4. Score with Phase 4 ML Model and store predictions
    model_path = cfg.model_output_dir
    if os.path.exists(model_path):
        logger.info("Loading ML PipelineModel from %s to generate predictions", model_path)
        model = PipelineModel.load(model_path)
        pred_df = model.transform(df)
        pred_count = write_predictions_batch(pred_df, batch_id=0, ops=ops, model_name="RandomForestRegressor")
        results["predictions"] = pred_count
        logger.info("Stored %d predictions in ml_predictions table", pred_count)
    else:
        logger.warning("ML model path not found at %s. Skipping prediction insertion.", model_path)

    pool.close()
    return results
