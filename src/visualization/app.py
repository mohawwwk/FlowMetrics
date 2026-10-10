import logging
import os
import sys
from pathlib import Path

# Insert project root into sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from flask import Flask, jsonify, render_template, request

from src.database.config import DatabaseConfig
from src.database.connection_pool import SQLiteConnectionPool
from src.database.operations import DatabaseOperations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecommerce-web-dashboard")

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
)

config = DatabaseConfig()
pool = SQLiteConnectionPool(config)
ops = DatabaseOperations(pool)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/summary")
def api_summary():
    """Returns headline KPIs across revenue, orders, customers, and ML predictions."""
    summary = ops.query_summary_stats()
    ml_stats = ops.query_prediction_accuracy()
    return jsonify({
        "summary": summary,
        "ml_accuracy": ml_stats,
    })


@app.route("/api/country-breakdown")
def api_country_breakdown():
    """Returns regional sales volume and market share."""
    limit = int(request.args.get("limit", 8))
    data = ops.query_revenue_by_country(limit)
    return jsonify(data)


@app.route("/api/top-products")
def api_top_products():
    """Returns top products by total revenue and units sold."""
    limit = int(request.args.get("limit", 10))
    data = ops.query_top_selling_products(limit)
    return jsonify(data)


@app.route("/api/orders-per-minute")
def api_orders_per_minute():
    """Returns streaming order velocity from windowed metrics."""
    with pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT window_start, window_end, orders_count, line_items_count, total_revenue, avg_order_value "
            "FROM windowed_order_metrics ORDER BY window_start ASC;"
        )
        rows = [dict(r) for r in cursor.fetchall()]
        cursor.close()
    return jsonify(rows)


@app.route("/api/hourly-trends")
def api_hourly_trends():
    """Returns 24h revenue distribution."""
    data = ops.query_hourly_trends()
    return jsonify(data)


@app.route("/api/customer-leaderboard")
def api_customer_leaderboard():
    """Returns top spending customers."""
    limit = int(request.args.get("limit", 10))
    data = ops.query_customer_leaderboard(limit)
    return jsonify(data)


@app.route("/api/streaming-metrics")
def api_streaming_metrics():
    """Returns recent windowed micro-batch metrics."""
    with pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT window_start, window_end, orders_count, line_items_count, total_revenue, avg_order_value "
            "FROM windowed_order_metrics ORDER BY window_start DESC LIMIT 10;"
        )
        rows = [dict(r) for r in cursor.fetchall()]
        cursor.close()
    return jsonify(rows)


@app.route("/api/ml-predictions")
def api_ml_predictions():
    """Returns prediction accuracy, curve points, residual buckets, and top discrepancies."""
    accuracy = ops.query_prediction_accuracy()
    discrepancies = ops.query_top_prediction_discrepancies(10)

    # Fetch sample actual vs predicted points
    with pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, actual_revenue, predicted_revenue, abs_error "
            "FROM ml_predictions ORDER BY id ASC LIMIT 50;"
        )
        points = [dict(r) for r in cursor.fetchall()]

        # Compute error histogram buckets
        cursor.execute("""
            SELECT
                CASE
                    WHEN abs_error < 2 THEN '< £2'
                    WHEN abs_error < 5 THEN '£2 - £5'
                    WHEN abs_error < 10 THEN '£5 - £10'
                    WHEN abs_error < 25 THEN '£10 - £25'
                    ELSE '> £25'
                END as bucket,
                COUNT(*) as count
            FROM ml_predictions
            GROUP BY bucket
            ORDER BY count DESC;
        """)
        buckets = [dict(r) for r in cursor.fetchall()]
        cursor.close()

    return jsonify({
        "accuracy": accuracy,
        "sample_points": points,
        "error_buckets": buckets,
        "discrepancies": discrepancies,
    })


def main():
    port = int(os.environ.get("PORT", 5000))
    logger.info("Starting Grafana-Style Live Dashboard Server on http://localhost:%d", port)
    app.run(host="127.0.0.1", port=port, debug=False)


if __name__ == "__main__":
    main()
