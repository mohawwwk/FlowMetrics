import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

from frontend.config.settings import config

logger = logging.getLogger("frontend.db_reader")


class ReadOnlyDatabaseService:
    """
    Read-only SQLite query service for Streamlit frontend.
    Enforces URI read-only access (mode=ro) and WAL mode compatibility.
    Guarantees no state mutation or file locking.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path or config.get_absolute_db_path())

    def is_connected(self) -> bool:
        return self.db_path.exists() and self.db_path.is_file()

    def get_connection(self) -> Optional[sqlite3.Connection]:
        if not self.is_connected():
            return None
        try:
            # Connect in explicit read-only URI mode to prevent any possibility of database writes
            uri = f"file:{self.db_path.as_posix()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=10.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            logger.error("Failed to connect to SQLite in read-only mode: %s", e)
            return None

    def execute_query_df(self, query: str, params: Tuple = ()) -> pd.DataFrame:
        """Executes a SQL SELECT query safely and returns a pandas DataFrame."""
        conn = self.get_connection()
        if conn is None:
            return pd.DataFrame()
        try:
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except Exception as e:
            logger.error("SQL query error: %s | Query: %s", e, query)
            return pd.DataFrame()
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def get_table_names(self) -> List[str]:
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        df = self.execute_query_df(query)
        if df.empty:
            return []
        return df["name"].tolist()

    def get_database_stats(self) -> Dict[str, Any]:
        """Fetches high-level executive KPIs across transactions and predictions."""
        if not self.is_connected():
            return {
                "connected": False,
                "total_transactions": 0,
                "total_revenue": 0.0,
                "avg_order_value": 0.0,
                "unique_customers": 0,
                "unique_products": 0,
                "total_predictions": 0,
            }

        q = """
            SELECT 
                COUNT(*) as total_tx,
                COALESCE(SUM(revenue), 0.0) as total_rev,
                COALESCE(AVG(revenue), 0.0) as avg_aov,
                COUNT(DISTINCT customer_id) as unique_cust,
                COUNT(DISTINCT stock_code) as unique_prod
            FROM processed_transactions;
        """
        df_tx = self.execute_query_df(q)
        
        q_pred = "SELECT COUNT(*) as total_pred FROM ml_predictions;"
        df_pred = self.execute_query_df(q_pred)

        total_tx = int(df_tx["total_tx"].iloc[0]) if not df_tx.empty else 0
        total_rev = float(df_tx["total_rev"].iloc[0]) if not df_tx.empty else 0.0
        avg_aov = float(df_tx["avg_aov"].iloc[0]) if not df_tx.empty else 0.0
        unique_cust = int(df_tx["unique_cust"].iloc[0]) if not df_tx.empty else 0
        unique_prod = int(df_tx["unique_prod"].iloc[0]) if not df_tx.empty else 0
        total_pred = int(df_pred["total_pred"].iloc[0]) if not df_pred.empty else 0

        return {
            "connected": True,
            "total_transactions": total_tx,
            "total_revenue": round(total_rev, 2),
            "avg_order_value": round(avg_aov, 2),
            "unique_customers": unique_cust,
            "unique_products": unique_prod,
            "total_predictions": total_pred,
        }

    def get_recent_transactions(self, limit: int = 50) -> pd.DataFrame:
        query = """
            SELECT 
                id, invoice_no, stock_code, description, quantity, 
                unit_price, revenue, country, invoice_timestamp, processed_at
            FROM processed_transactions
            ORDER BY id DESC
            LIMIT ?;
        """
        return self.execute_query_df(query, (limit,))

    def get_recent_predictions(self, limit: int = 50) -> pd.DataFrame:
        query = """
            SELECT 
                id, invoice_no, stock_code, quantity, unit_price,
                country, actual_revenue, predicted_revenue, abs_error,
                model_name, prediction_timestamp
            FROM ml_predictions
            ORDER BY id DESC
            LIMIT ?;
        """
        return self.execute_query_df(query, (limit,))

    def get_top_products(self, limit: int = 10) -> pd.DataFrame:
        query = """
            SELECT 
                stock_code,
                MAX(description) as description,
                SUM(quantity) as total_quantity,
                ROUND(SUM(revenue), 2) as total_revenue
            FROM processed_transactions
            GROUP BY stock_code
            ORDER BY total_revenue DESC
            LIMIT ?;
        """
        return self.execute_query_df(query, (limit,))

    def get_country_distribution(self, limit: int = 10) -> pd.DataFrame:
        query = """
            SELECT 
                country,
                COUNT(*) as transaction_count,
                ROUND(SUM(revenue), 2) as total_revenue
            FROM processed_transactions
            GROUP BY country
            ORDER BY total_revenue DESC
            LIMIT ?;
        """
        return self.execute_query_df(query, (limit,))

    def get_customer_leaderboard(self, limit: int = 10) -> pd.DataFrame:
        query = """
            SELECT 
                customer_id,
                country,
                total_orders,
                total_quantity,
                ROUND(total_spent, 2) as total_spent,
                ROUND(avg_order_value, 2) as avg_order_value
            FROM customer_analytics
            WHERE customer_id IS NOT NULL AND customer_id != ''
            ORDER BY total_spent DESC
            LIMIT ?;
        """
        df = self.execute_query_df(query, (limit,))
        if df.empty:
            # Fallback direct aggregation if customer_analytics table not populated yet
            fallback_query = """
                SELECT 
                    customer_id,
                    country,
                    COUNT(DISTINCT invoice_no) as total_orders,
                    SUM(quantity) as total_quantity,
                    ROUND(SUM(revenue), 2) as total_spent,
                    ROUND(AVG(revenue), 2) as avg_order_value
                FROM processed_transactions
                WHERE customer_id IS NOT NULL AND customer_id != ''
                GROUP BY customer_id, country
                ORDER BY total_spent DESC
                LIMIT ?;
            """
            return self.execute_query_df(fallback_query, (limit,))
        return df

    def get_orders_per_minute(self, limit: int = 30) -> pd.DataFrame:
        query = """
            SELECT 
                window_start,
                window_end,
                orders_count,
                line_items_count,
                ROUND(total_revenue, 2) as total_revenue,
                ROUND(avg_order_value, 2) as avg_order_value
            FROM windowed_order_metrics
            ORDER BY id DESC
            LIMIT ?;
        """
        df = self.execute_query_df(query, (limit,))
        if df.empty:
            # Fallback grouping by minute from processed_transactions
            fallback_q = """
                SELECT 
                    SUBSTR(invoice_timestamp, 1, 16) as window_start,
                    COUNT(DISTINCT invoice_no) as orders_count,
                    COUNT(*) as line_items_count,
                    ROUND(SUM(revenue), 2) as total_revenue,
                    ROUND(AVG(revenue), 2) as avg_order_value
                FROM processed_transactions
                GROUP BY window_start
                ORDER BY window_start DESC
                LIMIT ?;
            """
            return self.execute_query_df(fallback_q, (limit,))
        return df

    def get_hourly_sales(self) -> pd.DataFrame:
        query = """
            SELECT 
                CAST(SUBSTR(invoice_timestamp, 12, 2) AS INTEGER) as hour_of_day,
                COUNT(*) as transaction_count,
                ROUND(SUM(revenue), 2) as total_revenue
            FROM processed_transactions
            WHERE LENGTH(invoice_timestamp) >= 13
            GROUP BY hour_of_day
            ORDER BY hour_of_day ASC;
        """
        return self.execute_query_df(query)

    def browse_table(
        self,
        table_name: str,
        search_query: str = "",
        sort_col: Optional[str] = None,
        sort_order: str = "DESC",
        page: int = 1,
        page_size: int = 25,
    ) -> Tuple[pd.DataFrame, int]:
        """Safely paginates and filters arbitrary tables in read-only mode."""
        valid_tables = self.get_table_names()
        if table_name not in valid_tables:
            return pd.DataFrame(), 0

        # Get count
        count_q = f"SELECT COUNT(*) as total FROM {table_name}"
        total_rows = int(self.execute_query_df(count_q)["total"].iloc[0])

        # Get column names
        cols_q = f"PRAGMA table_info({table_name});"
        cols_df = self.execute_query_df(cols_q)
        all_cols = cols_df["name"].tolist() if not cols_df.empty else []

        # Build query
        where_clause = ""
        params: List[Any] = []
        if search_query and all_cols:
            clauses = [f"CAST({c} AS TEXT) LIKE ?" for c in all_cols]
            where_clause = f" WHERE {' OR '.join(clauses)}"
            params = [f"%{search_query}%"] * len(all_cols)

            # Re-count with filter
            filtered_count_q = f"SELECT COUNT(*) as total FROM {table_name}{where_clause}"
            total_rows = int(self.execute_query_df(filtered_count_q, tuple(params))["total"].iloc[0])

        order_by = ""
        if sort_col and sort_col in all_cols:
            order_by = f" ORDER BY {sort_col} {'ASC' if sort_order.upper() == 'ASC' else 'DESC'}"
        elif "id" in all_cols:
            order_by = " ORDER BY id DESC"

        offset = max(0, (page - 1) * page_size)
        query = f"SELECT * FROM {table_name}{where_clause}{order_by} LIMIT ? OFFSET ?;"
        params.extend([page_size, offset])

        df = self.execute_query_df(query, tuple(params))
        return df, total_rows


# Global instance
db_service = ReadOnlyDatabaseService()
