import json
import os
import sys
import unittest
from pathlib import Path

# Insert project root into sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.visualization.app import app


class TestVisualizationAndDashboards(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app.config["TESTING"] = True
        cls.client = app.test_client()
        cls.dashboards_dir = Path(__file__).resolve().parents[1] / "dashboards"

    def test_grafana_revenue_dashboard_json_validity(self):
        """Verifies that the Grafana revenue dashboard JSON is valid and contains all required panels."""
        json_path = self.dashboards_dir / "ecommerce_revenue_analytics.json"
        self.assertTrue(json_path.exists())

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertIn("title", data)
        self.assertEqual(data["uid"], "ecommerce-revenue-analytics")
        panel_titles = [p["title"] for p in data["panels"]]

        self.assertIn("Total Revenue", panel_titles)
        self.assertIn("Average Purchase (AOV)", panel_titles)
        self.assertIn("Orders Per Minute (Streaming Velocity)", panel_titles)
        self.assertIn("Top Products by Revenue", panel_titles)
        self.assertIn("Top Customers Leaderboard", panel_titles)
        self.assertIn("Streaming Metrics (Recent Window Summaries)", panel_titles)

    def test_grafana_ml_dashboard_json_validity(self):
        """Verifies that the Grafana ML predictions dashboard JSON is valid."""
        json_path = self.dashboards_dir / "ecommerce_ml_predictions.json"
        self.assertTrue(json_path.exists())

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data["uid"], "ecommerce-ml-predictions")
        panel_titles = [p["title"] for p in data["panels"]]

        self.assertIn("Total Predictions Scored", panel_titles)
        self.assertIn("Model RMSE", panel_titles)
        self.assertIn("Model MAE", panel_titles)
        self.assertIn("Actual vs Predicted Revenue", panel_titles)
        self.assertIn("Residual Error Distribution", panel_titles)
        self.assertIn("Top Prediction Discrepancies (Model Drift Monitoring)", panel_titles)

    def test_index_route(self):
        """Verifies that the dashboard home page serves HTTP 200."""
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"E-Commerce Analytics Platform", response.data)

    def test_api_summary(self):
        """Verifies /api/summary returns valid KPI metrics."""
        response = self.client.get("/api/summary")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("summary", data)
        self.assertIn("ml_accuracy", data)
        self.assertGreater(data["summary"]["total_transactions"], 0)
        self.assertGreater(data["summary"]["total_revenue"], 0)

    def test_api_country_breakdown(self):
        """Verifies /api/country-breakdown returns regional sales metrics."""
        response = self.client.get("/api/country-breakdown")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        self.assertIn("country", data[0])
        self.assertIn("country_revenue", data[0])

    def test_api_top_products(self):
        """Verifies /api/top-products returns top product rows."""
        response = self.client.get("/api/top-products")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        self.assertIn("stock_code", data[0])
        self.assertIn("total_product_revenue", data[0])

    def test_api_orders_per_minute(self):
        """Verifies /api/orders-per-minute returns windowed streaming velocity."""
        response = self.client.get("/api/orders-per-minute")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        if data:
            self.assertIn("window_start", data[0])
            self.assertIn("orders_count", data[0])

    def test_api_customer_leaderboard(self):
        """Verifies /api/customer-leaderboard returns customer spend rankings."""
        response = self.client.get("/api/customer-leaderboard")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        self.assertIn("customer_id", data[0])
        self.assertIn("total_spend", data[0])

    def test_api_ml_predictions(self):
        """Verifies /api/ml-predictions returns accuracy and residual curves."""
        response = self.client.get("/api/ml-predictions")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("accuracy", data)
        self.assertIn("sample_points", data)
        self.assertIn("error_buckets", data)
        self.assertIn("discrepancies", data)
        self.assertGreater(data["accuracy"]["sample_count"], 0)


if __name__ == "__main__":
    unittest.main()
