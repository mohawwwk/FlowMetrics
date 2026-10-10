# ApexAnalytics: Real-Time Big Data Frontend Dashboard

A modern, high-performance, corporate analytics dashboard built with **Streamlit**, **Plotly**, and **SQLite Read-Only WAL Integration**. Integrates seamlessly with the existing Big Data backend (Kafka, Spark Structured Streaming, MLlib, and SQLite) without modifying backend logic or schemas.

---

## 1. Architecture & Design Principles

- **Zero Backend Mutation:** Operates strictly in read-only mode (`PRAGMA query_only = ON;` and `?mode=ro`). Does not alter Kafka brokers, Spark streaming jobs, ML models, or SQLite tables.
- **Corporate Dark Aesthetic:** Deep slate/navy palette (`#0b0f19`, `#111827`) with cyan and electric blue accents (`#3b82f6`, `#06b6d4`), rounded cards (`border-radius: 14px`), subtle glassmorphism borders, and smooth transitions.
- **Non-Blocking Telemetry:** Microsecond socket probes for Kafka, Spark checkpoint file-watchers, and safe timeout handling ensure the UI never freezes or crashes, even when backend components are offline.
- **Dynamic Real-Time Refresh:** Built-in auto-refresh (default 3 seconds) using `streamlit-autorefresh` for live streaming operations.

---

## 2. Directory Structure

```
frontend/
├── app.py                      # Main entry point & page router
├── requirements.txt            # Frontend pip dependencies
├── README.md                   # Complete frontend documentation
├── config/                     # Configuration management
│   ├── __init__.py
│   └── settings.py             # Environment variables and fallback settings
├── theme/                      # Theming & Plotly palettes
│   ├── __init__.py
│   └── theme.py                # CSS injector and dark Plotly layout config
├── styles/                     # CSS stylesheets
│   └── custom.css              # Custom corporate glassmorphic stylesheet
├── database/                   # Safe read-only data service
│   ├── __init__.py
│   └── db_reader.py            # Read-only SQLite query service & aggregations
├── services/                   # Diagnostics & health probes
│   ├── __init__.py
│   └── health_service.py       # Kafka, Spark, DB, ML & resource probes
├── components/                 # Reusable UI widgets
│   ├── __init__.py
│   ├── kpi_card.py             # Metric card with animated trend badges
│   └── sidebar.py              # Brand header & option-menu navigation
├── charts/                     # Reusable Plotly visualizations
│   ├── __init__.py
│   └── plotly_charts.py        # Line, Bar, Pie, Scatter, Heatmap, and Area charts
├── pages/                      # 9 Modular Dashboard Views
│   ├── __init__.py
│   ├── home.py                 # Executive overview & architecture status
│   ├── live_analytics.py       # Live revenue, OPM, velocity & streaming tables
│   ├── machine_learning.py     # Model accuracy (R²=0.9745), actual vs predicted, residuals
│   ├── database_explorer.py    # Read-only paginated table viewer with CSV export
│   ├── business_insights.py    # Moving averages, peak hours & strategic suggestions
│   ├── system_health.py        # CPU, RAM, Disk, probe diagnostics & log viewer
│   ├── grafana_embed.py        # Embedded Grafana dashboards & launch links
│   ├── settings_page.py        # Refresh rate, database path & preferences
│   └── about_page.py           # Mission, technology stack & report references
├── utils/                      # Helper functions
│   ├── __init__.py
│   └── helpers.py              # Number, currency, and timestamp formatters
├── assets/                     # Static assets (.gitkeep)
├── static/                     # Static file attachments (.gitkeep)
├── icons/                      # SVG icons (.gitkeep)
├── images/                     # Screenshot placeholders (.gitkeep)
├── cache/                      # Streamlit session cache (.gitkeep)
└── logs/                       # Application logs
    └── frontend.log            # System heartbeat log file
```

---

## 3. Installation Guide

### Prerequisites
- Python 3.11.x
- Existing backend SQLite database at `data/ecommerce.db`

### Step 1: Install Dependencies
```bash
# From project root:
pip install -r frontend/requirements.txt
```

*(All packages install cleanly via standard pip with zero Node.js, npm, Docker, or external build tools).*

### Step 2: Launch Frontend Application
```bash
# Run Streamlit from project root:
streamlit run frontend/app.py
```
Or with explicit port configuration:
```bash
streamlit run frontend/app.py --server.port 8501 --server.address localhost
```

The application will immediately open in your browser at `http://localhost:8501`.

---

## 4. Page Walkthrough & Feature Guide

### 1. Home (`frontend/pages/home.py`)
- **System Health Status Ribbon:** Live pills displaying Kafka, Spark, SQLite, and MLlib operational states with pulsating status dots.
- **Executive KPI Cards:** Total Revenue ($), Processed Orders, Average Order Value (AOV), and ML Predictions Generated.
- **Architecture Flow:** Visual 4-stage pipeline summary (Ingestion $\to$ Stream Processing $\to$ Predictive ML $\to$ Storage).
- **Recent Stream Ingestion:** Live preview table of latest ingested transactions.

### 2. Live Analytics (`frontend/pages/live_analytics.py`)
- **Real-Time Revenue Timeline:** Interactive Plotly spline area chart of incoming sales.
- **Geographic Market Share:** Donut chart of country sales distribution.
- **Windowed Velocity:** 1-minute tumbling window orders-per-minute metric.
- **24-Hour Purchase Density Heatmap:** Visualizes peak purchase hours across the day.
- **Top Products & Customers:** Dynamically ranked commercial leaders.
- **Auto-Refresh:** Automatically refreshes telemetry every 3 seconds.

### 3. Machine Learning (`frontend/pages/machine_learning.py`)
- **Accuracy Metrics:** Supervised evaluation displaying $R^2 = 0.9745$, $\text{RMSE} = \$7.63$, and $\text{MAE} = \$4.76$.
- **Algorithm Comparison Table:** Shows why Linear Regression outperformed Random Forest in accuracy and sub-millisecond inference latency (0.12 ms vs 0.85 ms).
- **Scatter Plot (Actual vs. Predicted):** Interactive scatter diagram with $y = x$ reference line and color-coded absolute error.
- **Residual Distribution Histogram:** Confirms error centering around zero without heteroscedastic skew.
- **Prediction Table:** Real-time log showing transaction features, predicted amounts, actual revenue, and model attribution.

### 4. Database Explorer (`frontend/pages/database_explorer.py`)
- **Table Selector:** Select from `processed_transactions`, `ml_predictions`, `customer_analytics`, `windowed_order_metrics`, or `windowed_product_metrics`.
- **Global Search:** Type-ahead text filter across all columns.
- **Pagination & Sorting:** Sort ascending/descending by any column, navigate pages.
- **CSV Data Export:** One-click download button for filtered result sets.
- **Read-Only Enforced:** Uses SQLite `mode=ro` URI parameter, ensuring no write operations can occur.

### 5. Business Insights (`frontend/pages/business_insights.py`)
- **7-Period Moving Average:** Smoothed revenue trend line filtering out high-frequency micro-batch noise.
- **Peak Purchase Analysis:** Identifies highest conversion hours (typically 12:00–14:00 UTC).
- **Automated Summaries & Recommendations:** Dynamic insights on international market expansion, basket bundling strategies, and inventory reorder alerts.

### 6. System Health (`frontend/pages/system_health.py`)
- **Hardware Telemetry:** Real-time CPU load (%), RAM usage (GB / %), and Disk storage headroom (GB / %).
- **Distributed Services Probes:** Diagnostic ping to Kafka socket (`localhost:9092`), Spark checkpoint validation, and SQLite WAL inspection.
- **Application Event Logs:** In-app terminal code block displaying recent system heartbeats.

### 7. Grafana (`frontend/pages/grafana_embed.py`)
- **Embedded Iframe View:** Renders the pre-existing Grafana dashboards directly inside the Streamlit tab.
- **Direct Launch Buttons:** One-click links to `http://localhost:3000` for deep dashboard drilldowns.

### 8. Settings (`frontend/pages/settings_page.py`)
- Adjust auto-refresh frequency (1–30s).
- Verify and switch database file paths.
- Configure CSV export preferences.

### 9. About (`frontend/pages/about_page.py`)
- Complete architectural specifications, academic references, and document directory index.

---

## 5. Testing & Verification

A dedicated test suite can be run to ensure all frontend services and database queries execute cleanly:

```bash
# Verify all backend integration tests remain 100% green:
pytest tests/test_database.py tests/test_visualization.py tests/test_phase7.py -v
```

To test frontend module imports:
```bash
python -c "from frontend.database.db_reader import db_service; print('DB Connected:', db_service.is_connected(), db_service.get_database_stats())"
python -c "from frontend.services.health_service import health_service; print('Health:', health_service.get_system_resources())"
```

---

## 6. Common Issues & Fixes

| Issue | Cause | Solution |
| :--- | :--- | :--- |
| **"Database file missing" Warning** | The SQLite database at `data/ecommerce.db` has not been initialized. | Run `python -m src.database.cli init-db` or verify the path in the Settings page. |
| **"Kafka Broker: OFFLINE" Warning** | Kafka broker is not running on `localhost:9092`. | Normal if running in mock/offline mode. Start Kafka cluster with `docker-compose up` or your Kafka script if live streaming is desired. |
| **"Spark Streaming: Standby / Ready"** | Streaming consumer process is not actively running in background. | Start the consumer with `python -m src.consumer.spark_streaming`. The frontend will detect active checkpoints and display `READY` or `RUNNING`. |
| **Grafana Iframe refused to connect** | Grafana is not running on port 3000 or browser blocks cross-origin iframes. | Start Grafana (`grafana-server`), or click the direct launch buttons at the top of the Grafana page. |

---

## 7. Performance & Optimization Tips

1. **Connection Re-Use:** `ReadOnlyDatabaseService` reuses lightweight read connections and automatically closes them, avoiding connection exhaustion.
2. **Caching:** Streamlit's `st.cache_data` can be applied to large table browsing if working with datasets $>100\text{MB}$.
3. **Pagination Limit:** Setting page size to 25 records keeps query execution under 2 milliseconds, maintaining smooth 60 FPS UI responsiveness.
4. **Non-Blocking Sockets:** The Kafka health probe uses a 0.3-second timeout so page rendering is never delayed if the network port is unreachable.
