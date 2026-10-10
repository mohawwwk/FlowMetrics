import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

# Insert project root into sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.database.config import DatabaseConfig
from src.database.connection_pool import SQLiteConnectionPool
from src.database.operations import DatabaseOperations
from src.database.schema import create_schema, get_tables_info


class TestDatabaseIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_ecommerce.db")
        self.config = DatabaseConfig(
            db_path=self.db_path,
            pool_size=3,
            timeout_seconds=5.0,
        )
        self.pool = SQLiteConnectionPool(self.config)
        self.ops = DatabaseOperations(self.pool)

    def tearDown(self):
        self.pool.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_auto_create_database_and_directory(self):
        """Verifies that the database file and directory are created automatically."""
        self.assertTrue(os.path.exists(self.db_path))

    def test_schema_creation_and_introspection(self):
        """Verifies that create_schema creates all 5 tables and 13 indexes."""
        with self.pool.get_connection() as conn:
            create_schema(conn)
            tables = get_tables_info(conn)

        table_names = [t["table_name"] for t in tables]
        self.assertIn("processed_transactions", table_names)
        self.assertIn("ml_predictions", table_names)
        self.assertIn("windowed_order_metrics", table_names)
        self.assertIn("windowed_product_metrics", table_names)
        self.assertIn("customer_analytics", table_names)

        # Verify index count across tables
        all_indexes = [idx for t in tables for idx in t["indexes"]]
        self.assertIn("idx_tx_timestamp", all_indexes)
        self.assertIn("idx_pred_abs_error", all_indexes)
        self.assertIn("idx_cust_spend", all_indexes)

    def test_insert_processed_transactions_idempotence(self):
        """Verifies insert_transactions inserts records and handles duplicate transactions."""
        with self.pool.get_connection() as conn:
            create_schema(conn)

        records = [
            {
                "invoice_no": "5001",
                "stock_code": "85123A",
                "description": "WHITE HEART LIGHT",
                "quantity": 6,
                "unit_price": 2.55,
                "customer_id": "17850",
                "country": "United Kingdom",
                "invoice_timestamp": "2020-01-01 08:30:00",
                "revenue": 15.30,
            },
            {
                "invoice_no": "5002",
                "stock_code": "71053",
                "description": "WHITE METAL LANTERN",
                "quantity": 4,
                "unit_price": 3.39,
                "customer_id": "17850",
                "country": "United Kingdom",
                "invoice_timestamp": "2020-01-01 09:00:00",
                "revenue": 13.56,
            },
        ]

        inserted = self.ops.insert_transactions(records)
        self.assertEqual(inserted, 2)

        # Duplicate insert should be ignored due to UNIQUE constraint
        inserted_dup = self.ops.insert_transactions(records)
        self.assertEqual(inserted_dup, 0)

        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM processed_transactions;")
            self.assertEqual(cursor.fetchone()[0], 2)
            cursor.close()

    def test_insert_predictions(self):
        """Verifies storing ML predictions with error calculations."""
        with self.pool.get_connection() as conn:
            create_schema(conn)

        preds = [
            {
                "invoice_no": "5001",
                "stock_code": "85123A",
                "quantity": 6,
                "unit_price": 2.55,
                "country": "United Kingdom",
                "actual_revenue": 15.30,
                "predicted_revenue": 16.00,
                "abs_error": 0.70,
                "model_name": "RandomForestRegressor",
            },
            {
                "invoice_no": "5002",
                "stock_code": "71053",
                "quantity": 4,
                "unit_price": 3.39,
                "country": "United Kingdom",
                "actual_revenue": 13.56,
                "predicted_revenue": 13.00,
                "abs_error": 0.56,
                "model_name": "RandomForestRegressor",
            },
        ]

        inserted = self.ops.insert_predictions(preds)
        self.assertEqual(inserted, 2)

        stats = self.ops.query_prediction_accuracy()
        self.assertEqual(stats["sample_count"], 2)
        self.assertAlmostEqual(stats["calculated_mae"], 0.63, places=2)

    def test_customer_analytics_aggregation(self):
        """Verifies automatic customer analytics aggregation from processed_transactions."""
        with self.pool.get_connection() as conn:
            create_schema(conn)

        records = [
            {"invoice_no": "5001", "stock_code": "A1", "description": "D1", "quantity": 2, "unit_price": 10.0, "customer_id": "C101", "country": "United Kingdom", "invoice_timestamp": "2020-01-01 10:00:00", "revenue": 20.0},
            {"invoice_no": "5001", "stock_code": "A2", "description": "D2", "quantity": 1, "unit_price": 30.0, "customer_id": "C101", "country": "United Kingdom", "invoice_timestamp": "2020-01-01 10:00:00", "revenue": 30.0},
            {"invoice_no": "5002", "stock_code": "A3", "description": "D3", "quantity": 5, "unit_price": 10.0, "customer_id": "C101", "country": "United Kingdom", "invoice_timestamp": "2020-01-02 11:00:00", "revenue": 50.0},
            {"invoice_no": "5003", "stock_code": "B1", "description": "D4", "quantity": 1, "unit_price": 100.0, "customer_id": "C102", "country": "France", "invoice_timestamp": "2020-01-03 12:00:00", "revenue": 100.0},
        ]
        self.ops.insert_transactions(records)
        cust_count = self.ops.compute_and_update_customer_analytics()
        self.assertEqual(cust_count, 2)

        leaderboard = self.ops.query_customer_leaderboard(5)
        self.assertEqual(len(leaderboard), 2)
        # C101 has 2 distinct orders (5001, 5002) and 100 total spend
        # C102 has 1 order and 100 total spend
        c101 = next(c for c in leaderboard if c["customer_id"] == "C101")
        self.assertEqual(c101["total_orders"], 2)
        self.assertEqual(c101["total_spend"], 100.0)
        self.assertEqual(c101["avg_spend_per_order"], 50.0)

    def test_analytical_queries(self):
        """Verifies summary KPIs, revenue by country, top products, and hourly trends."""
        with self.pool.get_connection() as conn:
            create_schema(conn)

        records = [
            {"invoice_no": "5001", "stock_code": "A1", "description": "Item A", "quantity": 5, "unit_price": 10.0, "customer_id": "C1", "country": "United Kingdom", "invoice_timestamp": "2020-01-01 08:30:00", "revenue": 50.0},
            {"invoice_no": "5002", "stock_code": "A2", "description": "Item B", "quantity": 2, "unit_price": 25.0, "customer_id": "C2", "country": "Germany", "invoice_timestamp": "2020-01-01 14:15:00", "revenue": 50.0},
        ]
        self.ops.insert_transactions(records)

        summary = self.ops.query_summary_stats()
        self.assertEqual(summary["total_transactions"], 2)
        self.assertEqual(summary["total_revenue"], 100.0)
        self.assertEqual(summary["unique_countries"], 2)

        by_country = self.ops.query_revenue_by_country(5)
        self.assertEqual(len(by_country), 2)

        top_products = self.ops.query_top_selling_products(5)
        self.assertEqual(len(top_products), 2)

        hourly = self.ops.query_hourly_trends()
        hours = [h["hour_of_day"] for h in hourly]
        self.assertIn(8, hours)
        self.assertIn(14, hours)

    def test_transaction_rollback_on_error(self):
        """Verifies that an error within get_connection rolls back all changes."""
        with self.pool.get_connection() as conn:
            create_schema(conn)

        # Attempt to insert, then raise exception
        try:
            with self.pool.get_connection() as conn:
                conn.execute(
                    "INSERT INTO processed_transactions (invoice_no, stock_code, quantity, unit_price, country, invoice_timestamp, revenue) "
                    "VALUES ('9999', 'TEST', 1, 10.0, 'UK', '2020-01-01 00:00:00', 10.0);"
                )
                raise ValueError("Simulated unexpected failure")
        except ValueError:
            pass

        # Verify record was rolled back
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM processed_transactions WHERE invoice_no='9999';")
            count = cursor.fetchone()[0]
            cursor.close()
            self.assertEqual(count, 0)

    def test_connection_pool_concurrency(self):
        """Verifies that multiple concurrent threads safely use the pool without deadlocks."""
        with self.pool.get_connection() as conn:
            create_schema(conn)

        def worker(thread_id: int):
            for i in range(5):
                rec = {
                    "invoice_no": f"THR_{thread_id}_{i}",
                    "stock_code": f"SKU_{i}",
                    "description": "CONCURRENT_TEST",
                    "quantity": 1,
                    "unit_price": 5.0,
                    "customer_id": f"C_{thread_id}",
                    "country": "United Kingdom",
                    "invoice_timestamp": "2020-01-01 12:00:00",
                    "revenue": 5.0,
                }
                self.ops.insert_transactions([rec])

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM processed_transactions;")
            count = cursor.fetchone()[0]
            cursor.close()
            self.assertEqual(count, 20)


if __name__ == "__main__":
    unittest.main()
