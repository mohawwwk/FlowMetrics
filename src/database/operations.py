import logging
import sqlite3
from typing import Any, Iterable, Optional, Union

from src.database.connection_pool import SQLiteConnectionPool

logger = logging.getLogger("ecommerce-db-ops")


class DatabaseOperations:
    """
    High-level database operations: inserting processed streaming data,
    storing ML predictions, aggregating metrics, and running analytical queries.
    """

    def __init__(self, pool: SQLiteConnectionPool):
        self.pool = pool

    # =========================================================================
    # Data Ingestion Methods (DML)
    # =========================================================================

    def insert_transactions(
        self,
        records: Iterable[Union[dict, tuple]],
        conn: Optional[sqlite3.Connection] = None,
    ) -> int:
        """
        Inserts processed streaming transactions into processed_transactions.
        Uses INSERT OR IGNORE to handle duplicate stream messages idempotently.
        """
        sql = """
        INSERT OR IGNORE INTO processed_transactions (
            invoice_no, stock_code, description, quantity, unit_price,
            customer_id, country, invoice_timestamp, revenue
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        def _execute(c: sqlite3.Connection) -> int:
            params = []
            for r in records:
                if isinstance(r, dict):
                    params.append((
                        str(r["invoice_no"]),
                        str(r["stock_code"]),
                        r.get("description"),
                        int(r["quantity"]),
                        float(r["unit_price"]),
                        str(r["customer_id"]) if r.get("customer_id") else None,
                        str(r["country"]),
                        str(r["invoice_timestamp"]),
                        float(r["revenue"]),
                    ))
                else:
                    params.append(r)
            if not params:
                return 0
            cursor = c.cursor()
            cursor.executemany(sql, params)
            affected = cursor.rowcount
            cursor.close()
            return affected

        if conn is not None:
            return _execute(conn)
        return self.pool.execute_with_retry(_execute)

    def insert_predictions(
        self,
        records: Iterable[Union[dict, tuple]],
        conn: Optional[sqlite3.Connection] = None,
    ) -> int:
        """
        Inserts model predictions into ml_predictions table.
        """
        sql = """
        INSERT INTO ml_predictions (
            invoice_no, stock_code, quantity, unit_price, country,
            actual_revenue, predicted_revenue, abs_error, model_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        def _execute(c: sqlite3.Connection) -> int:
            params = []
            for r in records:
                if isinstance(r, dict):
                    params.append((
                        str(r["invoice_no"]),
                        str(r["stock_code"]),
                        int(r["quantity"]),
                        float(r["unit_price"]),
                        str(r["country"]),
                        float(r["actual_revenue"]),
                        float(r["predicted_revenue"]),
                        float(r["abs_error"]),
                        str(r.get("model_name", "RandomForestRegressor")),
                    ))
                else:
                    params.append(r)
            if not params:
                return 0
            cursor = c.cursor()
            cursor.executemany(sql, params)
            affected = cursor.rowcount
            cursor.close()
            return affected

        if conn is not None:
            return _execute(conn)
        return self.pool.execute_with_retry(_execute)

    def insert_order_metrics(
        self,
        records: Iterable[Union[dict, tuple]],
        conn: Optional[sqlite3.Connection] = None,
    ) -> int:
        """
        Upserts windowed order metrics into windowed_order_metrics table.
        """
        sql = """
        INSERT OR REPLACE INTO windowed_order_metrics (
            window_start, window_end, orders_count, line_items_count,
            total_revenue, avg_order_value
        ) VALUES (?, ?, ?, ?, ?, ?);
        """

        def _execute(c: sqlite3.Connection) -> int:
            params = []
            for r in records:
                if isinstance(r, dict):
                    params.append((
                        str(r["window_start"]),
                        str(r["window_end"]),
                        int(r["orders_count"]),
                        int(r["line_items_count"]),
                        float(r["total_revenue"]),
                        float(r["avg_order_value"]),
                    ))
                else:
                    params.append(r)
            if not params:
                return 0
            cursor = c.cursor()
            cursor.executemany(sql, params)
            affected = cursor.rowcount
            cursor.close()
            return affected

        if conn is not None:
            return _execute(conn)
        return self.pool.execute_with_retry(_execute)

    def insert_product_metrics(
        self,
        records: Iterable[Union[dict, tuple]],
        conn: Optional[sqlite3.Connection] = None,
    ) -> int:
        """
        Upserts windowed product metrics into windowed_product_metrics table.
        """
        sql = """
        INSERT OR REPLACE INTO windowed_product_metrics (
            window_start, window_end, stock_code, description,
            units_sold, product_revenue
        ) VALUES (?, ?, ?, ?, ?, ?);
        """

        def _execute(c: sqlite3.Connection) -> int:
            params = []
            for r in records:
                if isinstance(r, dict):
                    params.append((
                        str(r["window_start"]),
                        str(r["window_end"]),
                        str(r["stock_code"]),
                        r.get("description"),
                        int(r["units_sold"]),
                        float(r["product_revenue"]),
                    ))
                else:
                    params.append(r)
            if not params:
                return 0
            cursor = c.cursor()
            cursor.executemany(sql, params)
            affected = cursor.rowcount
            cursor.close()
            return affected

        if conn is not None:
            return _execute(conn)
        return self.pool.execute_with_retry(_execute)

    def compute_and_update_customer_analytics(
        self,
        conn: Optional[sqlite3.Connection] = None,
    ) -> int:
        """
        Aggregates transaction history from processed_transactions and updates customer_analytics.
        """
        sql = """
        INSERT OR REPLACE INTO customer_analytics (
            customer_id, country, total_orders, total_spend, avg_spend_per_order,
            first_order_timestamp, last_order_timestamp, updated_at
        )
        SELECT
            customer_id,
            country,
            COUNT(DISTINCT invoice_no) AS total_orders,
            ROUND(SUM(revenue), 2) AS total_spend,
            ROUND(SUM(revenue) / COUNT(DISTINCT invoice_no), 2) AS avg_spend_per_order,
            MIN(invoice_timestamp) AS first_order_timestamp,
            MAX(invoice_timestamp) AS last_order_timestamp,
            datetime('now') AS updated_at
        FROM processed_transactions
        WHERE customer_id IS NOT NULL AND customer_id != ''
        GROUP BY customer_id, country;
        """

        def _execute(c: sqlite3.Connection) -> int:
            cursor = c.cursor()
            cursor.execute(sql)
            count = cursor.rowcount
            cursor.close()
            return count

        if conn is not None:
            return _execute(conn)
        return self.pool.execute_with_retry(_execute)

    # =========================================================================
    # Analytical Queries (DQL)
    # =========================================================================

    def query_summary_stats(self) -> dict[str, Any]:
        """Returns overall KPI summary across transactions and predictions."""
        sql = """
        SELECT
            (SELECT COUNT(*) FROM processed_transactions) AS total_transactions,
            (SELECT COUNT(DISTINCT invoice_no) FROM processed_transactions) AS total_orders,
            (SELECT COUNT(DISTINCT customer_id) FROM processed_transactions WHERE customer_id IS NOT NULL) AS unique_customers,
            (SELECT COUNT(DISTINCT country) FROM processed_transactions) AS unique_countries,
            (SELECT ROUND(COALESCE(SUM(revenue), 0), 2) FROM processed_transactions) AS total_revenue,
            (SELECT ROUND(COALESCE(AVG(revenue), 0), 2) FROM processed_transactions) AS avg_item_revenue,
            (SELECT COUNT(*) FROM ml_predictions) AS total_predictions,
            (SELECT ROUND(COALESCE(AVG(abs_error), 0), 2) FROM ml_predictions) AS avg_prediction_error;
        """
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            cursor.close()
            return dict(row) if row else {}

    def query_revenue_by_country(self, limit: int = 10) -> list[dict[str, Any]]:
        """Calculates revenue and transaction volume broken down by country."""
        sql = """
        SELECT
            country,
            COUNT(*) AS transactions_count,
            COUNT(DISTINCT invoice_no) AS orders_count,
            ROUND(SUM(revenue), 2) AS country_revenue,
            ROUND(AVG(revenue), 2) AS avg_item_revenue,
            ROUND(100.0 * SUM(revenue) / (SELECT SUM(revenue) FROM processed_transactions), 2) AS revenue_share_pct
        FROM processed_transactions
        GROUP BY country
        ORDER BY country_revenue DESC
        LIMIT ?;
        """
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            cursor.close()
            return rows

    def query_top_selling_products(self, limit: int = 10) -> list[dict[str, Any]]:
        """Returns top products ranked by total revenue."""
        sql = """
        SELECT
            stock_code,
            COALESCE(description, 'N/A') AS description,
            SUM(quantity) AS total_units_sold,
            ROUND(AVG(unit_price), 2) AS avg_unit_price,
            ROUND(SUM(revenue), 2) AS total_product_revenue
        FROM processed_transactions
        GROUP BY stock_code
        ORDER BY total_product_revenue DESC
        LIMIT ?;
        """
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            cursor.close()
            return rows

    def query_hourly_trends(self) -> list[dict[str, Any]]:
        """Analyzes revenue patterns by hour of day (0-23)."""
        sql = """
        SELECT
            CAST(strftime('%H', invoice_timestamp) AS INTEGER) AS hour_of_day,
            COUNT(*) AS transactions_count,
            COUNT(DISTINCT invoice_no) AS orders_count,
            ROUND(SUM(revenue), 2) AS hourly_revenue,
            ROUND(AVG(revenue), 2) AS avg_revenue_per_tx
        FROM processed_transactions
        GROUP BY hour_of_day
        ORDER BY hour_of_day ASC;
        """
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = [dict(r) for r in cursor.fetchall()]
            cursor.close()
            return rows

    def query_prediction_accuracy(self) -> dict[str, Any]:
        """
        Calculates SQL-level ML evaluation metrics (RMSE, MAE, Max Error)
        directly from ml_predictions table.
        """
        sql = """
        SELECT
            COUNT(*) AS sample_count,
            ROUND(AVG(actual_revenue), 2) AS mean_actual,
            ROUND(AVG(predicted_revenue), 2) AS mean_predicted,
            ROUND(AVG(predicted_revenue - actual_revenue), 2) AS mean_bias,
            ROUND(AVG(abs_error), 4) AS calculated_mae,
            ROUND(SQRT(AVG((predicted_revenue - actual_revenue) * (predicted_revenue - actual_revenue))), 4) AS calculated_rmse,
            ROUND(MAX(abs_error), 2) AS max_error
        FROM ml_predictions;
        """
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            cursor.close()
            return dict(row) if row else {}

    def query_top_prediction_discrepancies(self, limit: int = 10) -> list[dict[str, Any]]:
        """Returns the predictions with the highest absolute error for debugging and drift analysis."""
        sql = """
        SELECT
            invoice_no,
            stock_code,
            quantity,
            unit_price,
            country,
            actual_revenue,
            predicted_revenue,
            abs_error,
            model_name,
            prediction_timestamp
        FROM ml_predictions
        ORDER BY abs_error DESC
        LIMIT ?;
        """
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            cursor.close()
            return rows

    def query_customer_leaderboard(self, limit: int = 10) -> list[dict[str, Any]]:
        """Returns highest value customers from customer_analytics table."""
        sql = """
        SELECT
            customer_id,
            country,
            total_orders,
            total_spend,
            avg_spend_per_order,
            first_order_timestamp,
            last_order_timestamp
        FROM customer_analytics
        ORDER BY total_spend DESC
        LIMIT ?;
        """
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            cursor.close()
            return rows
