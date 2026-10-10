import logging
import sqlite3

logger = logging.getLogger("ecommerce-db-schema")

# DDL Statements for Tables
TABLE_PROCESSED_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS processed_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_no TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    description TEXT,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    customer_id TEXT,
    country TEXT NOT NULL,
    invoice_timestamp TEXT NOT NULL,
    revenue REAL NOT NULL,
    processed_at TEXT DEFAULT (datetime('now')),
    CONSTRAINT uq_transaction UNIQUE (invoice_no, stock_code, invoice_timestamp)
);
"""

TABLE_ML_PREDICTIONS = """
CREATE TABLE IF NOT EXISTS ml_predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_no TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    country TEXT NOT NULL,
    actual_revenue REAL NOT NULL,
    predicted_revenue REAL NOT NULL,
    abs_error REAL NOT NULL,
    model_name TEXT NOT NULL,
    prediction_timestamp TEXT DEFAULT (datetime('now'))
);
"""

TABLE_WINDOWED_ORDER_METRICS = """
CREATE TABLE IF NOT EXISTS windowed_order_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    orders_count INTEGER NOT NULL,
    line_items_count INTEGER NOT NULL,
    total_revenue REAL NOT NULL,
    avg_order_value REAL NOT NULL,
    recorded_at TEXT DEFAULT (datetime('now')),
    CONSTRAINT uq_order_window UNIQUE (window_start, window_end)
);
"""

TABLE_WINDOWED_PRODUCT_METRICS = """
CREATE TABLE IF NOT EXISTS windowed_product_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    description TEXT,
    units_sold INTEGER NOT NULL,
    product_revenue REAL NOT NULL,
    recorded_at TEXT DEFAULT (datetime('now')),
    CONSTRAINT uq_product_window UNIQUE (window_start, window_end, stock_code)
);
"""

TABLE_CUSTOMER_ANALYTICS = """
CREATE TABLE IF NOT EXISTS customer_analytics (
    customer_id TEXT PRIMARY KEY,
    country TEXT NOT NULL,
    total_orders INTEGER NOT NULL,
    total_spend REAL NOT NULL,
    avg_spend_per_order REAL NOT NULL,
    first_order_timestamp TEXT,
    last_order_timestamp TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);
"""

# DDL Statements for Indexes
INDEXES = [
    # Fast filtering on transaction temporal queries and lookups
    "CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON processed_transactions (invoice_timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_tx_invoice ON processed_transactions (invoice_no);",
    "CREATE INDEX IF NOT EXISTS idx_tx_customer ON processed_transactions (customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_tx_country ON processed_transactions (country);",
    "CREATE INDEX IF NOT EXISTS idx_tx_stock ON processed_transactions (stock_code);",

    # Predictions query indexes for monitoring accuracy and model drift
    "CREATE INDEX IF NOT EXISTS idx_pred_invoice ON ml_predictions (invoice_no);",
    "CREATE INDEX IF NOT EXISTS idx_pred_abs_error ON ml_predictions (abs_error);",
    "CREATE INDEX IF NOT EXISTS idx_pred_timestamp ON ml_predictions (prediction_timestamp);",

    # Window aggregation indexes for real-time dashboards
    "CREATE INDEX IF NOT EXISTS idx_wom_window ON windowed_order_metrics (window_start, window_end);",
    "CREATE INDEX IF NOT EXISTS idx_wpm_window ON windowed_product_metrics (window_start, window_end);",
    "CREATE INDEX IF NOT EXISTS idx_wpm_stock ON windowed_product_metrics (stock_code);",

    # Customer leaderboard indexes
    "CREATE INDEX IF NOT EXISTS idx_cust_spend ON customer_analytics (total_spend DESC);",
    "CREATE INDEX IF NOT EXISTS idx_cust_orders ON customer_analytics (total_orders DESC);",
]

ALL_TABLES = [
    ("processed_transactions", TABLE_PROCESSED_TRANSACTIONS),
    ("ml_predictions", TABLE_ML_PREDICTIONS),
    ("windowed_order_metrics", TABLE_WINDOWED_ORDER_METRICS),
    ("windowed_product_metrics", TABLE_WINDOWED_PRODUCT_METRICS),
    ("customer_analytics", TABLE_CUSTOMER_ANALYTICS),
]


def create_schema(conn: sqlite3.Connection) -> None:
    """Creates all database tables and indexes if they do not already exist."""
    cursor = conn.cursor()
    for table_name, ddl in ALL_TABLES:
        logger.debug("Ensuring table exists: %s", table_name)
        cursor.execute(ddl)

    for index_ddl in INDEXES:
        logger.debug("Ensuring index exists: %s", index_ddl.strip().split()[4])
        cursor.execute(index_ddl)

    cursor.close()
    logger.info("Database schema initialized with 5 tables and %d indexes.", len(INDEXES))


def get_tables_info(conn: sqlite3.Connection) -> list[dict]:
    """Inspects SQLite database and returns metadata for all user tables."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
    tables = [row[0] for row in cursor.fetchall()]

    table_stats = []
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        row_count = cursor.fetchone()[0]

        cursor.execute(f"PRAGMA table_info({table});")
        columns = [col[1] for col in cursor.fetchall()]

        cursor.execute(f"PRAGMA index_list({table});")
        indexes = [idx[1] for idx in cursor.fetchall()]

        table_stats.append({
            "table_name": table,
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
            "indexes": indexes,
        })
    cursor.close()
    return table_stats
