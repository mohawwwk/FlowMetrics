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

# Insert project root into sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession

from src.database.config import DatabaseConfig
from src.database.connection_pool import SQLiteConnectionPool
from src.database.operations import DatabaseOperations
from src.database.schema import create_schema, get_tables_info
from src.database.spark_sink import populate_database_from_processed_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecommerce-db-cli")


def format_table(headers: list[str], rows: list[list], align: list[str] = None) -> str:
    """Formats rows into an ASCII table string."""
    if not rows:
        return "(No rows returned)\n"

    col_widths = [len(h) for h in headers]
    for row in rows:
        for idx, val in enumerate(row):
            col_widths[idx] = max(col_widths[idx], len(str(val)))

    if not align:
        align = ["<"] * len(headers)

    header_line = " | ".join(
        f"{h:{align[i]}{col_widths[i]}}" for i, h in enumerate(headers)
    )
    separator = "-+-".join("-" * w for w in col_widths)

    lines = [header_line, separator]
    for row in rows:
        row_line = " | ".join(
            f"{str(v):{align[i]}{col_widths[i]}}" for i, v in enumerate(row)
        )
        lines.append(row_line)
    return "\n".join(lines) + "\n"


def run_inspect(pool: SQLiteConnectionPool) -> None:
    print(f"\n{'='*75}\nDATABASE INSPECTION & TABLE STATUS\n{'='*75}")
    with pool.get_connection() as conn:
        stats = get_tables_info(conn)
        headers = ["Table Name", "Rows", "Cols", "Indexes Active"]
        rows = []
        for s in stats:
            rows.append([
                s["table_name"],
                f"{s['row_count']:,}",
                s["column_count"],
                ", ".join(s["indexes"]) if s["indexes"] else "none",
            ])
        print(format_table(headers, rows, ["<", ">", ">", "<"]))


def run_queries(ops: DatabaseOperations) -> None:
    print(f"\n{'='*75}\n1. EXECUTIVE SUMMARY & KPIS\n{'='*75}")
    summary = ops.query_summary_stats()
    for k, v in summary.items():
        print(f"  • {k.replace('_', ' ').title():<30} : {v}")

    print(f"\n{'='*75}\n2. REVENUE BY COUNTRY (TOP 10)\n{'='*75}")
    countries = ops.query_revenue_by_country(10)
    if countries:
        headers = ["Country", "Transactions", "Orders", "Revenue (£)", "Avg/Item (£)", "Share %"]
        rows = [
            [c["country"], f"{c['transactions_count']:,}", f"{c['orders_count']:,}",
             f"{c['country_revenue']:,.2f}", f"{c['avg_item_revenue']:,.2f}", f"{c['revenue_share_pct']}%"]
            for c in countries
        ]
        print(format_table(headers, rows, ["<", ">", ">", ">", ">", ">"]))

    print(f"\n{'='*75}\n3. TOP SELLING PRODUCTS BY REVENUE (TOP 10)\n{'='*75}")
    products = ops.query_top_selling_products(10)
    if products:
        headers = ["StockCode", "Description", "Units Sold", "Avg Unit Price (£)", "Total Revenue (£)"]
        rows = [
            [p["stock_code"], (p["description"][:28] + "...") if len(p["description"]) > 28 else p["description"],
             f"{p['total_units_sold']:,}", f"{p['avg_unit_price']:,.2f}", f"{p['total_product_revenue']:,.2f}"]
            for p in products
        ]
        print(format_table(headers, rows, ["<", "<", ">", ">", ">"]))

    print(f"\n{'='*75}\n4. IN-DATABASE ML PREDICTION PERFORMANCE & METRICS\n{'='*75}")
    ml_stats = ops.query_prediction_accuracy()
    for k, v in ml_stats.items():
        print(f"  • {k.replace('_', ' ').title():<30} : {v}")

    print(f"\n{'='*75}\n5. TOP MODEL PREDICTION DISCREPANCIES (FOR DRIFT MONITORING)\n{'='*75}")
    discrepancies = ops.query_top_prediction_discrepancies(5)
    if discrepancies:
        headers = ["Invoice", "Stock", "Qty", "Price", "Country", "Actual (£)", "Predicted (£)", "Abs Error (£)"]
        rows = [
            [d["invoice_no"], d["stock_code"], d["quantity"], f"{d['unit_price']:.2f}",
             d["country"][:14], f"{d['actual_revenue']:.2f}", f"{d['predicted_revenue']:.2f}", f"{d['abs_error']:.2f}"]
            for d in discrepancies
        ]
        print(format_table(headers, rows, ["<", "<", ">", ">", "<", ">", ">", ">"]))

    print(f"\n{'='*75}\n6. CUSTOMER LEADERBOARD (TOP 10 SPENDERS)\n{'='*75}")
    customers = ops.query_customer_leaderboard(10)
    if customers:
        headers = ["CustomerID", "Country", "Orders", "Total Spend (£)", "Avg/Order (£)", "Last Order"]
        rows = [
            [c["customer_id"], c["country"][:14], c["total_orders"],
             f"{c['total_spend']:,.2f}", f"{c['avg_spend_per_order']:,.2f}", c["last_order_timestamp"]]
            for c in customers
        ]
        print(format_table(headers, rows, ["<", "<", ">", ">", ">", "<"]))


def parse_args():
    parser = argparse.ArgumentParser(description="SQLite database operations and analytics CLI.")
    parser.add_argument("--init", action="store_true", help="Initialize database schema and tables.")
    parser.add_argument("--populate", action="store_true", help="Populate DB from Phase 3 & 4 data.")
    parser.add_argument("--inspect", action="store_true", help="Inspect database tables and row counts.")
    parser.add_argument("--query", action="store_true", help="Execute and print analytical queries.")
    return parser.parse_args()


def main():
    args = parse_args()
    config = DatabaseConfig()
    pool = SQLiteConnectionPool(config)
    ops = DatabaseOperations(pool)

    try:
        if args.init:
            logger.info("Initializing SQLite database schema at %s", config.db_path)
            with pool.get_connection() as conn:
                create_schema(conn)
            run_inspect(pool)

        elif args.populate:
            logger.info("Populating database from Phase 3 & 4 data artifacts...")
            spark = (
                SparkSession.builder
                .master("local[*]")
                .appName("EcommerceDB-Populate")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("WARN")
            try:
                results = populate_database_from_processed_data(spark, config)
                logger.info("Population complete: %s", results)
            finally:
                spark.stop()
            run_inspect(pool)
            run_queries(ops)

        elif args.inspect:
            run_inspect(pool)

        elif args.query:
            run_queries(ops)

        else:
            # Default: init and inspect
            with pool.get_connection() as conn:
                create_schema(conn)
            run_inspect(pool)
            run_queries(ops)

    finally:
        pool.close()


if __name__ == "__main__":
    main()
