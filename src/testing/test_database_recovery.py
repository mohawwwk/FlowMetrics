import argparse
import json
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

logger = configure_structured_logging("database-recovery")


class DatabaseRecoveryTester:
    """
    Stress tests and validates SQLite Write-Ahead Logging (WAL) recovery mechanisms:
    1. Uncommitted transaction rollback and process crash recovery
    2. WAL checkpoint truncation (PRAGMA wal_checkpoint(TRUNCATE))
    3. Structural integrity verification (PRAGMA integrity_check)
    4. Multi-threaded write lock contention and busy_timeout resilience
    """

    def __init__(self, db_path: Optional[str] = None):
        self.temp_dir: Optional[str] = None
        if db_path:
            self.db_path = Path(db_path)
        else:
            self.temp_dir = tempfile.mkdtemp(prefix="db_recovery_")
            self.db_path = Path(self.temp_dir) / "recovery_test.db"

        self.wal_path = Path(str(self.db_path) + "-wal")
        self.config = DatabaseConfig(db_path=str(self.db_path), pool_size=5, timeout_seconds=15.0)
        self.pool = SQLiteConnectionPool(config=self.config)
        with self.pool.get_connection() as conn:
            create_schema(conn)

    def cleanup(self) -> None:
        self.pool.close()
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir, ignore_errors=True)
            except Exception:
                pass

    def test_uncommitted_rollback_on_crash(self) -> Dict[str, Any]:
        """
        Simulates an abrupt application crash mid-transaction.
        Verifies that partial / uncommitted dirty writes are completely rolled back and never leak.
        """
        logger.info("--- Testing Scenario 1: Uncommitted Crash Rollback ---")

        # Insert 10 committed baseline records
        with self.pool.get_connection() as conn:
            for i in range(10):
                conn.execute(
                    "INSERT INTO processed_transactions (invoice_no, stock_code, description, quantity, unit_price, country, invoice_timestamp, revenue) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                    (f"BASE-{i}", "SC1", "Desc", 1, 10.0, "UK", "2026-10-11 00:00:00", 10.0),
                )

        # Count baseline
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM processed_transactions;")
            baseline_count = cursor.fetchone()[0]
        assert baseline_count == 10, f"Expected 10 baseline records, got {baseline_count}"

        # Simulate crash mid-transaction: open a standalone connection, write dirty rows, but close without commit
        raw_conn = sqlite3.connect(str(self.db_path))
        raw_conn.execute("PRAGMA journal_mode = WAL;")
        raw_conn.execute("BEGIN TRANSACTION;")
        raw_conn.execute(
            "INSERT INTO processed_transactions (invoice_no, stock_code, description, quantity, unit_price, country, invoice_timestamp, revenue) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            ("CRASH-999", "DIRTY", "Uncommitted Row", 99, 99.0, "UK", "2026-10-11 00:00:00", 9801.0),
        )
        # Abruptly close / simulate crash without calling COMMIT
        raw_conn.close()
        logger.warning("Simulated sudden connection abort before COMMIT.")

        # Reconnect via pool and verify state
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM processed_transactions;")
            post_crash_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM processed_transactions WHERE invoice_no = 'CRASH-999';")
            dirty_found = cursor.fetchone()[0]

        assert post_crash_count == 10, f"Dirty data leaked! Expected 10 rows, got {post_crash_count}"
        assert dirty_found == 0, "Dirty record 'CRASH-999' was found in database!"

        result = {
            "scenario": "uncommitted_crash_rollback",
            "baseline_count": baseline_count,
            "post_crash_count": post_crash_count,
            "dirty_rows_detected": dirty_found,
            "rollback_verified": True,
        }
        logger.info("Scenario 1 Result: Rollback verified. Zero dirty rows leaked.")
        return result

    def test_wal_checkpoint_and_truncation(self, insert_count: int = 5000) -> Dict[str, Any]:
        """
        Inserts high volume of transactions generating significant WAL pages,
        then executes PRAGMA wal_checkpoint(TRUNCATE) and confirms file truncation.
        """
        logger.info("--- Testing Scenario 2: WAL Checkpoint & Truncation ---")

        # Insert batch into WAL
        insert_sql = """
            INSERT OR IGNORE INTO processed_transactions (
                invoice_no, stock_code, description, quantity, unit_price,
                customer_id, country, invoice_timestamp, revenue
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        records = [
            (f"WAL-{i}", f"SC-{i%100}", "WAL Product", 2, 15.0, "12345", "UK", "2026-10-11 01:00:00", 30.0)
            for i in range(insert_count)
        ]

        with self.pool.get_connection() as conn:
            conn.executemany(insert_sql, records)

        # Check WAL size before checkpoint
        wal_size_before = self.wal_path.stat().st_size if self.wal_path.exists() else 0
        logger.info("WAL file size before checkpoint: %d bytes (%.2f KB)", wal_size_before, wal_size_before / 1024)

        # Execute WAL checkpoint TRUNCATE
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            checkpoint_status = cursor.fetchone()
            # Returns (busy, log_frames, checkpointed_frames)

        wal_size_after = self.wal_path.stat().st_size if self.wal_path.exists() else 0
        logger.info("WAL file size after checkpoint(TRUNCATE): %d bytes", wal_size_after)

        assert wal_size_after == 0 or wal_size_after < wal_size_before, "WAL file should be truncated!"

        # Confirm data is intact
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM processed_transactions WHERE invoice_no LIKE 'WAL-%';")
            stored_count = cursor.fetchone()[0]

        assert stored_count == insert_count, f"Expected {insert_count} records, found {stored_count}"

        result = {
            "scenario": "wal_checkpoint_truncate",
            "inserted_records": insert_count,
            "wal_size_before_bytes": wal_size_before,
            "wal_size_after_bytes": wal_size_after,
            "checkpoint_status": {
                "busy": checkpoint_status[0],
                "log_frames": checkpoint_status[1],
                "checkpointed_frames": checkpoint_status[2],
            },
            "records_intact": stored_count == insert_count,
        }
        logger.info(
            "Scenario 2 Result: WAL Checkpoint TRUNCATE succeeded. Before=%d B -> After=%d B",
            wal_size_before, wal_size_after
        )
        return result

    def test_integrity_check(self) -> Dict[str, Any]:
        """Runs PRAGMA integrity_check and PRAGMA quick_check to verify page checksums."""
        logger.info("--- Testing Scenario 3: Database Page & Index Integrity Check ---")
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check;")
            integrity_result = cursor.fetchall()
            cursor.execute("PRAGMA quick_check;")
            quick_result = cursor.fetchall()

        is_ok = len(integrity_result) == 1 and integrity_result[0][0] == "ok"
        is_quick_ok = len(quick_result) == 1 and quick_result[0][0] == "ok"

        assert is_ok, f"Integrity check failed: {integrity_result}"
        assert is_quick_ok, f"Quick check failed: {quick_result}"

        result = {
            "scenario": "integrity_check",
            "integrity_check": integrity_result[0][0],
            "quick_check": quick_result[0][0],
            "status": "HEALTHY",
        }
        logger.info("Scenario 3 Result: Database Integrity is 100% HEALTHY ('ok').")
        return result

    def test_concurrent_write_contention(self, num_threads: int = 10, writes_per_thread: int = 100) -> Dict[str, Any]:
        """
        Fires 10 concurrent threads performing rapid transactions into the database pool.
        Validates SQLite WAL concurrency and busy_timeout backoff under severe lock contention.
        """
        logger.info(
            "--- Testing Scenario 4: Concurrent Write Contention (%d threads, %d writes each) ---",
            num_threads, writes_per_thread
        )

        collector = PerformanceCollector(f"SQLite_Contention_{num_threads}threads").start()
        errors: List[str] = []

        def worker(thread_id: int):
            for i in range(writes_per_thread):
                t0 = time.perf_counter()
                try:
                    with self.pool.get_connection() as conn:
                        conn.execute(
                            "INSERT OR IGNORE INTO processed_transactions (invoice_no, stock_code, description, quantity, unit_price, country, invoice_timestamp, revenue) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                            (f"THR-{thread_id}-{i}", "CONC", "Thread item", 1, 5.0, "UK", "2026-10-11 02:00:00", 5.0),
                        )
                    lat_ms = (time.perf_counter() - t0) * 1000.0
                    collector.record_operation(lat_ms)
                except Exception as exc:
                    lat_ms = (time.perf_counter() - t0) * 1000.0
                    collector.record_operation(lat_ms, is_error=True)
                    errors.append(f"Thread {thread_id} op {i}: {exc}")

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, t) for t in range(num_threads)]
            for f in as_completed(futures):
                f.result()

        summary = collector.stop()
        total_attempted = num_threads * writes_per_thread

        assert summary.error_count == 0, f"Encountered {summary.error_count} lock errors: {errors[:3]}"

        result = {
            "scenario": "concurrent_write_contention",
            "threads": num_threads,
            "total_transactions": total_attempted,
            "successful_transactions": total_attempted - summary.error_count,
            "lock_errors": summary.error_count,
            "throughput_tx_sec": summary.throughput_ops_sec,
            "p50_latency_ms": summary.p50_latency_ms,
            "p95_latency_ms": summary.p95_latency_ms,
            "p99_latency_ms": summary.p99_latency_ms,
        }
        logger.info(
            "Scenario 4 Result: 100%% Success (%d/%d tx) | %.1f tx/sec | p50=%.2fms | p99=%.2fms | Lock Errors=%d",
            result["successful_transactions"], total_attempted, summary.throughput_ops_sec,
            summary.p50_latency_ms, summary.p99_latency_ms, summary.error_count
        )
        return result

    def run_all(self) -> Dict[str, Any]:
        """Runs all 4 recovery and resilience test scenarios."""
        try:
            r1 = self.test_uncommitted_rollback_on_crash()
            r2 = self.test_wal_checkpoint_and_truncation(insert_count=3000)
            r3 = self.test_integrity_check()
            r4 = self.test_concurrent_write_contention(num_threads=8, writes_per_thread=50)
            return {
                "rollback_test": r1,
                "wal_checkpoint_test": r2,
                "integrity_check_test": r3,
                "concurrency_contention_test": r4,
            }
        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(description="Database Recovery & WAL Resilience Test Harness")
    args = parser.parse_args()

    tester = DatabaseRecoveryTester()
    results = tester.run_all()
    print("\n================== DATABASE RECOVERY RESULTS ==================")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
