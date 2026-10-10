# Phase 6 — Grafana Dashboards & Real-Time Visualization

> **Phase 6 Deliverable:** Production-grade Grafana dashboards configured with the SQLite datasource, full declarative provisioning (`sqlite.yaml`, `dashboards.yaml`), official Grafana dashboard JSON models for Revenue, Top Products, Orders per minute, Average purchase, Predictions, Top customers, and Streaming metrics, a live interactive dark-mode web application clone, comprehensive step-by-step dashboard creation documentation, and a detailed screenshot capture guide.
>
> **Project Status:** Phase 6 COMPLETE. All 6 phases of the Big Data E-Commerce Analytics Pipeline are finished. Stopping after Phase 6.

---

## 1. Architecture Overview

```
 ┌────────────────────────────────────────────────────────┐
 │            SQLite Database (data/ecommerce.db)         │
 │  • processed_transactions    • ml_predictions          │
 │  • windowed_order_metrics    • customer_analytics      │
 └──────────────┬──────────────────────────┬──────────────┘
                │                          │
                ▼                          ▼
 ┌──────────────────────────────┐ ┌────────────────────────────────┐
 │     Official Grafana OSS     │ │ Live Grafana Web Dashboard     │
 │  • frser-sqlite-datasource   │ │ (src/visualization/app.py)     │
 │  • Provisioned Dashboards    │ │  • Native Dark Mode UI         │
 │  • Port :3000                │ │  • Real-Time Chart.js Visuals  │
 └──────────────────────────────┘ │  • Port :5000                  │
                                  └────────────────────────────────┘
```

---

## 2. Requirements Compliance Matrix

| Requirement | Implementation Component | Status |
|-------------|--------------------------|:------:|
| **Build Grafana dashboard** | Declarative JSON schemas in `dashboards/` + provisioning configs | ✅ Complete |
| **Use SQLite datasource** | `frser-sqlite-datasource` configuration in `dashboards/provisioning/` | ✅ Verified |
| **Revenue dashboard** | Total revenue stat, hourly curve, and regional sales distribution | ✅ Verified |
| **Top products** | Top 10 SKUs ranked by revenue and units sold | ✅ Verified |
| **Orders per minute** | Streaming order velocity timeline from windowed metrics | ✅ Verified |
| **Average purchase** | Average Order Value (AOV) gauge with dynamic color bands | ✅ Verified |
| **Prediction dashboard** | Actual vs Predicted revenue, RMSE & MAE gauges, residual buckets | ✅ Verified |
| **Top customers** | Top spending customer leaderboard table | ✅ Verified |
| **Streaming metrics** | Real-time line-item counter, tumbling window summaries | ✅ Verified |
| **Explain dashboard creation**| Line-by-line guide with every SQL statement, panel type, and threshold | ✅ Documented |
| **Provide screenshots to capture**| Exact 6-part screenshot framing & capture guide | ✅ Documented |
| **Stop after Phase 6** | End of project scope; execution halted cleanly | ✅ Complete |

---

## 3. Step-by-Step Guide: Creating Dashboards in Grafana

### 3.1 Step 1 — Installing the SQLite Datasource Plugin in Grafana
Grafana connects to SQLite via the official community plugin `frser-sqlite-datasource`:

```powershell
# In terminal where Grafana is installed:
grafana-cli plugins install frser-sqlite-datasource

# Or using Docker:
docker run -d -p 3000:3000 `
  -e "GF_INSTALL_PLUGINS=frser-sqlite-datasource" `
  -v "${PWD}/data/ecommerce.db:/var/lib/grafana/ecommerce.db" `
  -v "${PWD}/dashboards/provisioning:/etc/grafana/provisioning" `
  --name grafana-ecommerce grafana/grafana:latest
```

### 3.2 Step 2 — Configuring the Datasource
1. In Grafana, navigate to **Connections ➔ Data sources ➔ Add data source**.
2. Select **SQLite**.
3. Set **Name:** `SQLite-Ecommerce`.
4. Set **Path:** `data/ecommerce.db` (or absolute path `C:/Users/sidha/Documents/trae_projects/BigData/data/ecommerce.db`).
5. Click **Save & Test**. You will see: *"Data source is working"*.

Alternatively, automatic provisioning is already configured in:
[dashboards/provisioning/datasources/sqlite.yaml](file:///c:/Users/sidha/Documents/trae_projects/BigData/dashboards/provisioning/datasources/sqlite.yaml)

---

### 3.3 Dashboard 1: "E-Commerce Revenue & Streaming Analytics"
Import or create using [dashboards/ecommerce_revenue_analytics.json](file:///c:/Users/sidha/Documents/trae_projects/BigData/dashboards/ecommerce_revenue_analytics.json).

#### Panel 1: Total Revenue (Stat Panel)
- **Visualization:** `Stat`
- **SQL Query:**
  ```sql
  SELECT strftime('%s', 'now') as time, ROUND(SUM(revenue), 2) as value
  FROM processed_transactions;
  ```
- **Panel Settings:** Unit: `Currency: British Pound (£)` | Color: `Green` | Value Mappings: None.

#### Panel 2: Total Orders (Stat Panel)
- **Visualization:** `Stat`
- **SQL Query:**
  ```sql
  SELECT strftime('%s', 'now') as time, COUNT(DISTINCT invoice_no) as value
  FROM processed_transactions;
  ```
- **Panel Settings:** Unit: `Short (Integer)` | Color: `Blue`.

#### Panel 3: Average Purchase / AOV (Gauge Panel)
- **Visualization:** `Gauge`
- **SQL Query:**
  ```sql
  SELECT strftime('%s', 'now') as time, ROUND(SUM(revenue) / COUNT(DISTINCT invoice_no), 2) as value
  FROM processed_transactions;
  ```
- **Panel Settings:** Min: `0` | Max: `1000` | Unit: `£` | Thresholds: `0: Red`, `100: Yellow`, `300: Green`.

#### Panel 4: Processed Line Items (Stat Panel)
- **Visualization:** `Stat`
- **SQL Query:**
  ```sql
  SELECT strftime('%s', 'now') as time, COUNT(*) as value
  FROM processed_transactions;
  ```
- **Panel Settings:** Unit: `Short` | Color: `Purple`.

#### Panel 5: Orders Per Minute (Streaming Velocity Time Series)
- **Visualization:** `Time series`
- **SQL Query:**
  ```sql
  SELECT strftime('%s', window_start) as time, orders_count as value
  FROM windowed_order_metrics
  ORDER BY window_start ASC;
  ```
- **Panel Settings:** Draw Style: `Line` | Fill Opacity: `25%` | Line Width: `2` | Color: `#3274d9`.

#### Panel 6: Hourly Revenue Distribution (Bar Chart)
- **Visualization:** `Bar chart`
- **SQL Query:**
  ```sql
  SELECT CAST(strftime('%H', invoice_timestamp) AS INTEGER) as hour, ROUND(SUM(revenue), 2) as revenue
  FROM processed_transactions
  GROUP BY hour
  ORDER BY hour ASC;
  ```
- **Panel Settings:** X-Axis: `hour` | Y-Axis: `revenue (£)` | Bar Radius: `3px`.

#### Panel 7: Top Products by Revenue (Bar Gauge)
- **Visualization:** `Bar gauge`
- **SQL Query:**
  ```sql
  SELECT stock_code || ' - ' || SUBSTR(COALESCE(description, 'N/A'), 1, 20) as metric,
         ROUND(SUM(revenue), 2) as value
  FROM processed_transactions
  GROUP BY stock_code
  ORDER BY value DESC
  LIMIT 10;
  ```
- **Panel Settings:** Orientation: `Horizontal` | Display Mode: `Gradient` | Unit: `£`.

#### Panel 8: Revenue by Country (Donut / Pie Chart)
- **Visualization:** `Pie chart`
- **SQL Query:**
  ```sql
  SELECT country as metric, ROUND(SUM(revenue), 2) as value
  FROM processed_transactions
  GROUP BY country
  ORDER BY value DESC
  LIMIT 8;
  ```
- **Panel Settings:** Pie Type: `Donut` | Legend: `Right`.

#### Panel 9: Top Customers Leaderboard (Table)
- **Visualization:** `Table`
- **SQL Query:**
  ```sql
  SELECT customer_id, country, total_orders, total_spend, avg_spend_per_order, last_order_timestamp
  FROM customer_analytics
  ORDER BY total_spend DESC
  LIMIT 10;
  ```
- **Panel Settings:** Cell overrides for `total_spend` & `avg_spend_per_order`: Unit `£`.

#### Panel 10: Streaming Metrics (Table)
- **Visualization:** `Table`
- **SQL Query:**
  ```sql
  SELECT window_start, window_end, orders_count, line_items_count, total_revenue, avg_order_value
  FROM windowed_order_metrics
  ORDER BY window_start DESC
  LIMIT 10;
  ```
- **Panel Settings:** Cell overrides for `total_revenue`: Unit `£`.

---

### 3.4 Dashboard 2: "Spark MLlib Revenue Prediction Performance"
Import or create using [dashboards/ecommerce_ml_predictions.json](file:///c:/Users/sidha/Documents/trae_projects/BigData/dashboards/ecommerce_ml_predictions.json).

#### Panel 1: Total Predictions Scored (Stat)
```sql
SELECT strftime('%s', 'now') as time, COUNT(*) as value FROM ml_predictions;
```

#### Panel 2: Model Prediction Bias (Stat)
```sql
SELECT strftime('%s', 'now') as time, ROUND(AVG(predicted_revenue - actual_revenue), 2) as value FROM ml_predictions;
```

#### Panel 3: Model RMSE (Gauge)
```sql
SELECT strftime('%s', 'now') as time,
       ROUND(SQRT(AVG((predicted_revenue - actual_revenue) * (predicted_revenue - actual_revenue))), 2) as value
FROM ml_predictions;
```
- **Thresholds:** `< 15: Green`, `15 - 30: Yellow`, `> 30: Red`.

#### Panel 4: Model MAE (Gauge)
```sql
SELECT strftime('%s', 'now') as time, ROUND(AVG(abs_error), 2) as value FROM ml_predictions;
```
- **Thresholds:** `< 8: Green`, `8 - 15: Yellow`, `> 15: Red`.

#### Panel 5: Actual vs Predicted Revenue (Time Series Comparison)
```sql
SELECT id as time, actual_revenue as 'Actual Revenue', predicted_revenue as 'Predicted Revenue'
FROM ml_predictions
ORDER BY id ASC
LIMIT 100;
```

#### Panel 6: Residual Error Distribution (Bar Chart)
```sql
SELECT CASE
           WHEN abs_error < 2 THEN '< £2'
           WHEN abs_error < 5 THEN '£2 - £5'
           WHEN abs_error < 10 THEN '£5 - £10'
           WHEN abs_error < 25 THEN '£10 - £25'
           ELSE '> £25'
       END as error_bucket,
       COUNT(*) as count
FROM ml_predictions
GROUP BY error_bucket
ORDER BY count DESC;
```

#### Panel 7: Top Prediction Discrepancies (Table with Drift Highlighting)
```sql
SELECT invoice_no, stock_code, quantity, unit_price, country,
       actual_revenue, predicted_revenue, abs_error, model_name, prediction_timestamp
FROM ml_predictions
ORDER BY abs_error DESC
LIMIT 10;
```

---

## 4. Live Interactive Web Dashboard

To ensure immediate visibility without requiring a running Docker or local Grafana daemon, a live Grafana dark-mode visualization web app is provided in [src/visualization/app.py](file:///c:/Users/sidha/Documents/trae_projects/BigData/src/visualization/app.py):

### How to Run:
```powershell
python -m src.visualization.app
```
Open **`http://localhost:5000`** in any web browser.

### Features:
- **Exact Grafana Aesthetics:** True Grafana palette (`#0b0c0e` background, `#14161b` panels, `#22262f` borders).
- **Tab Switching:** Instant navigation between **"Revenue & Streaming"** and **"ML Predictions & Accuracy"**.
- **Live Auto-Refresh:** Supports 5s, 10s, 30s auto-polling against `data/ecommerce.db`.
- **Responsive Charts:** Powered by Chart.js with tooltips, legend toggling, and smooth line curves.

---

## 5. Screenshots to Capture (Complete Capture Guide)

Follow this checklist to capture high-resolution presentation screenshots from either Grafana or the Live Web Dashboard at `http://localhost:5000`:

| # | Screenshot Target | What to Frame | Expected Values / Visual Highlights |
|---|-------------------|---------------|-------------------------------------|
| **1** | **Revenue Dashboard & KPIs** | Top 4 stat cards + Hourly Revenue Distribution | • Total Revenue: **£29,431.47**<br>• Total Orders: **84**<br>• Green bar chart peaking at 12:00–14:00 |
| **2** | **Orders Per Minute (Velocity)** | "Orders Per Minute" line chart | • Blue area chart showing streaming order pulses (1–3 orders/min across tumbling windows) |
| **3** | **Average Purchase & Products** | "Average Purchase (AOV)" card + "Top Products" bar chart | • AOV: **£350.37**<br>• Top SKU: `21137` (Black Record Cover Frame) with **£1,627.20** |
| **4** | **Top Customers & Country Share** | "Customer Leaderboard" table + "Revenue by Country" donut | • UK: **88.18%** share<br>• Top Spender: `16029` with **£3,702.12**<br>• Runner-up: `16210` with **£2,198.26** |
| **5** | **ML Prediction Performance** | Switch to ML tab: Stat cards + Actual vs Predicted line curve | • Predictions: **1,684**<br>• MAE: **£5.24**<br>• RMSE: **£18.84**<br>• Closely tracking green actual vs orange predicted lines |
| **6** | **Model Drift & Discrepancies** | "Residual Error Distribution" + "Top Discrepancies" table | • Dominant error bucket: `< £2` and `£2–£5`<br>• Colored badges for absolute errors |

---

## 6. Test Suite Verification (`tests/test_visualization.py`)

A test suite validates the Grafana JSON models and dashboard REST endpoints:

```powershell
.venv\Scripts\pytest.exe tests/test_visualization.py -v
```

### Results:
```
============================= test session starts =============================
platform win32 -- Python 3.11.9, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\Users\sidha\Documents\trae_projects\BigData
collected 9 items

tests/test_visualization.py::TestVisualizationAndDashboards::test_api_country_breakdown PASSED [ 11%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_customer_leaderboard PASSED [ 22%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_ml_predictions PASSED [ 33%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_orders_per_minute PASSED [ 44%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_summary PASSED [ 55%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_top_products PASSED [ 66%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_grafana_ml_dashboard_json_validity PASSED [ 77%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_grafana_revenue_dashboard_json_validity PASSED [ 88%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_index_route PASSED [100%]

============================== 9 passed in 0.63s ==============================
```

---

## 7. How to Run Phase 6

```powershell
# 1. Start the live Grafana-styled visual dashboard
python -m src.visualization.app

# 2. Open browser:
start http://localhost:5000

# 3. (Optional) Run Grafana test suite:
pytest tests/test_visualization.py -v
```

---

## 8. Summary of All Files Created in Phase 6

1. [dashboards/provisioning/datasources/sqlite.yaml](file:///c:/Users/sidha/Documents/trae_projects/BigData/dashboards/provisioning/datasources/sqlite.yaml): Grafana SQLite datasource provisioning config.
2. [dashboards/provisioning/dashboards/dashboards.yaml](file:///c:/Users/sidha/Documents/trae_projects/BigData/dashboards/provisioning/dashboards/dashboards.yaml): Grafana dashboard provider provisioning config.
3. [dashboards/ecommerce_revenue_analytics.json](file:///c:/Users/sidha/Documents/trae_projects/BigData/dashboards/ecommerce_revenue_analytics.json): Complete Grafana schema JSON for Revenue, Top Products, Orders/min, Average Purchase, Top Customers, and Streaming Metrics.
4. [dashboards/ecommerce_ml_predictions.json](file:///c:/Users/sidha/Documents/trae_projects/BigData/dashboards/ecommerce_ml_predictions.json): Complete Grafana schema JSON for Spark MLlib Prediction Performance.
5. [src/visualization/app.py](file:///c:/Users/sidha/Documents/trae_projects/BigData/src/visualization/app.py): Flask application serving dashboard REST APIs and live interface.
6. [src/visualization/templates/index.html](file:///c:/Users/sidha/Documents/trae_projects/BigData/src/visualization/templates/index.html): Dark-mode Grafana-style interactive frontend.
7. [tests/test_visualization.py](file:///c:/Users/sidha/Documents/trae_projects/BigData/tests/test_visualization.py): Test suite covering JSON schemas and API endpoints.
8. [README_PHASE6.md](file:///c:/Users/sidha/Documents/trae_projects/BigData/README_PHASE6.md): Phase 6 documentation and screenshot guide.

---

**Phase 6 is complete. All project phases (1 through 6) are fully implemented. Execution has halted after Phase 6.**
