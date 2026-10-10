import argparse
import csv
import json
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.database.config import DatabaseConfig
from src.database.connection_pool import SQLiteConnectionPool
from src.database.schema import create_schema
from src.testing.metrics_logger import (
    MetricSummary,
    PerformanceCollector,
    configure_structured_logging,
)

logger = configure_structured_logging("large-dataset-benchmark")


class LargeDatasetBenchmark:
    """
    Comprehensive performance benchmark evaluating the ingestion, sanitization,
    feature engineering, and bulk transactional persistence of the UCI Online Retail dataset.
    """

    def __init__(
        self,
        csv_path: Optional[str] = None,
        db_path: Optional[str] = None,
        batch_size: int = 5000,
    ):
        self.csv_path = Path(csv_path or "data/raw/online_retail.csv")
        self.batch_size = max(100, batch_size)
        self.temp_dir: Optional[str] = None
        if db_path:
            self.db_path = Path(db_path)
        else:
            self.temp_dir = tempfile.mkdtemp(prefix="benchmark_db_")
            self.db_path = Path(self.temp_dir) / "benchmark.db"

        self.parsing_collector = PerformanceCollector("CSV_Parse_Clean")
        self.aggregation_collector = PerformanceCollector("Analytical_Aggregation")
        self.db_insert_collector = PerformanceCollector("SQLite_Bulk_Ingest")

    def cleanup(self) -> None:
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir, ignore_errors=True)
            except Exception:
                pass

    def benchmark_parse_and_clean(self, max_rows: Optional[int] = 50000) -> Tuple[MetricSummary, List[Dict[str, Any]]]:
        """
        Benchmarks streaming CSV disk I/O, regex/field parsing, type casting,
        data validation, and filtering.
        """
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Dataset not found at {self.csv_path}")

        logger.info("Starting CSV Parsing & Cleaning benchmark (limit=%s rows)...", max_rows or "ALL")
        cleaned_records: List[Dict[str, Any]] = []

        self.parsing_collector.start()
        with open(self.csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch: List[Dict[str, Any]] = []
            batch_bytes = 0
            t_batch_start = time.perf_counter()

            for idx, row in enumerate(reader):
                if max_rows and idx >= max_rows:
                    break

                raw_bytes = sum(len(k) + len(v) for k, v in row.items())
                batch_bytes += raw_bytes

                # Clean & validate
                try:
                    qty = int(row.get("Quantity") or 0)
                    unit_price = float(row.get("UnitPrice") or 0.0)
                    inv_no = str(row.get("InvoiceNo") or "").strip()
                    stock_code = str(row.get("StockCode") or "").strip()
                    desc = str(row.get("Description") or "").strip()
                    inv_date = str(row.get("InvoiceDate") or "").strip()
                    cust_id = row.get("CustomerID")
                    cust_str = str(cust_id).strip() if cust_id not in (None, "") else None
                    country = str(row.get("Country") or "United Kingdom").strip()

                    # Filter cancellations / invalid transactions
                    is_valid = qty > 0 and unit_price >= 0 and not inv_no.startswith("C")
                    if is_valid:
                        record = {
                            "invoice_no": inv_no,
                            "stock_code": stock_code,
                            "description": desc,
                            "quantity": qty,
                            "unit_price": unit_price,
                            "total_amount": round(qty * unit_price, 2),
                            "invoice_date": inv_date,
                            "customer_id": cust_str,
                            "country": country,
                        }
                        batch.append(record)
                except Exception:
                    pass

                if len(batch) >= self.batch_size:
                    t_batch_end = time.perf_counter()
                    batch_lat_ms = (t_batch_end - t_batch_start) * 1000.0
                    for _ in batch:
                        self.parsing_collector.record_operation(
                            batch_lat_ms / len(batch),
                            byte_count=batch_bytes // len(batch),
                        )
                    cleaned_records.extend(batch)
                    batch = []
                    batch_bytes = 0
                    t_batch_start = time.perf_counter()

            # Drain remainder
            if batch:
                t_batch_end = time.perf_counter()
                batch_lat_ms = (t_batch_end - t_batch_start) * 1000.0
                for _ in batch:
                    self.parsing_collector.record_operation(
                        batch_lat_ms / len(batch),
                        byte_count=batch_bytes // len(batch),
                    )
                cleaned_records.extend(batch)

        summary = self.parsing_collector.stop()
        summary.extra_metadata["valid_records_extracted"] = len(cleaned_records)
        logger.info(
            "Parsing Benchmark: %.1f rows/sec | %.2f MB/sec | Total Valid=%d rows",
            summary.throughput_ops_sec,
            summary.throughput_mb_sec,
            len(cleaned_records),
        )
        return summary, cleaned_records

    def benchmark_analytical_aggregations(self, records: List[Dict[str, Any]]) -> MetricSummary:
        """
        Benchmarks in-memory feature engineering and analytical rollups:
        - Revenue by country
        - Top selling products
        - Customer spending profiles
        """
        logger.info("Starting Analytical Aggregations benchmark on %d records...", len(records))
        self.aggregation_collector.start()

        country_rev: Dict[str, float] = {}
        product_sales: Dict[str, Tuple[int, float]] = {}
        customer_profiles: Dict[str, Dict[str, Any]] = {}

        t0 = time.perf_counter()
        for rec in records:
            # Country rollup
            c = rec["country"]
            country_rev[c] = country_rev.get(c, 0.0) + rec["total_amount"]

            # Product rollup
            sc = rec["stock_code"]
            curr_q, curr_r = product_sales.get(sc, (0, 0.0))
            product_sales[sc] = (curr_q + rec["quantity"], curr_r + rec["total_amount"])

            # Customer rollup
            cid = rec["customer_id"]
            if cid:
                if cid not in customer_profiles:
                    customer_profiles[cid] = {"orders": 0, "spend": 0.0, "items": 0}
                customer_profiles[cid]["orders"] += 1
                customer_profiles[cid]["spend"] += rec["total_amount"]
                customer_profiles[cid]["items"] += rec["quantity"]

        t1 = time.perf_counter()
        total_time_ms = (t1 - t0) * 1000.0
        avg_op_ms = total_time_ms / max(len(records), 1)

        for _ in records:
            self.aggregation_collector.record_operation(avg_op_ms)

        summary = self.aggregation_collector.stop()
        summary.extra_metadata["unique_countries"] = len(country_rev)
        summary.extra_metadata["unique_products"] = len(product_sales)
        summary.extra_metadata["unique_customers"] = len(customer_profiles)

        logger.info(
            "Aggregations Benchmark: %.1f records/sec | Unique Countries=%d | Unique Products=%d",
            summary.throughput_ops_sec,
            len(country_rev),
            len(product_sales),
        )
        return summary

    def benchmark_sqlite_bulk_ingestion(self, records: List[Dict[str, Any]]) -> MetricSummary:
        """
        Benchmarks WAL-mode SQLite transactional persistence with prepared batch statements.
        Measures write IOPS, B-Tree indexing overhead, and connection pool execution.
        """
        logger.info("Initializing SQLite WAL database at %s...", self.db_path)
        db_config = DatabaseConfig(db_path=str(self.db_path), pool_size=4, timeout_seconds=30.0)
        pool = SQLiteConnectionPool(config=db_config)
        with pool.get_connection() as conn:
            create_schema(conn)

        logger.info("Starting SQLite Bulk Ingestion benchmark on %d records (batch_size=%d)...", len(records), self.batch_size)
        self.db_insert_collector.start()

        insert_sql = """
            INSERT OR IGNORE INTO processed_transactions (
                invoice_no, stock_code, description, quantity, unit_price,
                customer_id, country, invoice_timestamp, revenue
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        batches = [records[i:i + self.batch_size] for i in range(0, len(records), self.batch_size)]

        with pool.get_connection() as conn:
            for b in batches:
                param_tuples = [
                    (
                        r["invoice_no"],
                        r["stock_code"],
                        r["description"],
                        r["quantity"],
                        r["unit_price"],
                        r["customer_id"],
                        r["country"],
                        r["invoice_date"],
                        r["total_amount"],
                    )
                    for r in b
                ]
                t0 = time.perf_counter()
                conn.executemany(insert_sql, param_tuples)
                t1 = time.perf_counter()
                batch_ms = (t1 - t0) * 1000.0

                for r in b:
                    self.db_insert_collector.record_operation(batch_ms / len(b))

        pool.close()
        summary = self.db_insert_collector.stop()
        summary.extra_metadata["batches_executed"] = len(batches)
        summary.extra_metadata["db_file_size_mb"] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)

        logger.info(
            "SQLite Ingestion: %.1f rows/sec | DB Size=%.2f MB | p50=%.3fms | p99=%.3fms",
            summary.throughput_ops_sec,
            summary.extra_metadata["db_file_size_mb"],
            summary.p50_latency_ms,
            summary.p99_latency_ms,
        )
        return summary

    def run_all(self, max_rows: Optional[int] = 50000) -> Dict[str, MetricSummary]:
        """Runs the entire pipeline benchmark suite on the dataset."""
        try:
            parse_sum, records = self.benchmark_parse_and_clean(max_rows=max_rows)
            agg_sum = self.benchmark_analytical_aggregations(records)
            db_sum = self.benchmark_sqlite_bulk_ingestion(records)
            return {
                "csv_parsing": parse_sum,
                "aggregations": agg_sum,
                "sqlite_ingestion": db_sum,
            }
        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(description="Large Dataset Performance Benchmark")
    parser.add_argument("--limit", type=int, default=50000, help="Row limit for benchmark (e.g. 50000 or 0 for all)")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for SQLite ingestion")
    args = parser.parse_args()

    limit = None if args.limit <= 0 else args.limit
    benchmark = LargeDatasetBenchmark(batch_size=args.batch_size)
    results = benchmark.run_all(max_rows=limit)

    print("\n================== LARGE DATASET BENCHMARK RESULTS ==================")
    for stage, summ in results.items():
        print(f"\n--- {stage.upper()} ---")
        print(json.dumps(summ.to_dict(), indent=2))


if __name__ == "__main__":
    main()
