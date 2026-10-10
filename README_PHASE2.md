# Phase 2 — Dataset & Kafka Producer

**Project:** Real-Time E-Commerce Analytics Pipeline using Kafka, Spark Streaming, MLlib and Grafana
**Phase:** 2 of N
**Status:** COMPLETE — Awaiting approval for Phase 3
**Date:** 2026-10-11

> **Scope:** Dataset selection, column analysis, and a fully functional Kafka producer that streams e-commerce transactions from CSV to Kafka. Spark is NOT implemented in this phase.

---

## 1. Dataset Selected

**UCI Online Retail Dataset** — a classic, publicly available e-commerce transaction dataset from a UK-based online gift retailer (Dec 2010 – Dec 2011).

- **Source:** UCI Machine Learning Repository (hosted on GitHub for direct download)
- **Download URL:** `https://raw.githubusercontent.com/gitren111/The-RFM-model-for-e-commerce-data-analysis/main/online_retail.csv`
- **Local path:** `data/raw/online_retail.csv`
- **Size:** ~43 MB, **541,909 rows**, **8 columns**

### Why this dataset?
- Real transactional e-commerce data (not synthetic)
- Contains quantities, prices, customer IDs, countries, timestamps — ideal for revenue analytics, customer segmentation, and ML
- Well-documented and widely used in academia (RFM analysis, churn prediction)
- Directly downloadable as a single CSV — no login/Kaggle API required

---

## 2. Column Explanations (All 8 Columns)

| # | Column | Type | Description | Example |
|---|--------|------|-------------|---------|
| 1 | **InvoiceNo** | string | Unique invoice number. Starts with `C` if the transaction is a cancellation/return. | `536365`, `C536379` |
| 2 | **StockCode** | string | Product (item) code — unique identifier for each product. | `85123A`, `71053` |
| 3 | **Description** | string | Human-readable product name. | `WHITE HANGING HEART T-LIGHT HOLDER` |
| 4 | **Quantity** | int | Number of units purchased. Negative values indicate returns/cancellations. | `6`, `-1` |
| 5 | **InvoiceDate** | string | Date and time of the invoice (`YYYY/MM/DD HH:MM`). | `2010/12/1 8:26` |
| 6 | **UnitPrice** | float | Price per unit in GBP (£). Can be 0 (free items) or negative (adjustments). | `2.55`, `3.39` |
| 7 | **CustomerID** | string (nullable) | Unique customer identifier. Has ~135k nulls (guest/anonymous purchases). | `17850` |
| 8 | **Country** | string | Country of the customer. | `United Kingdom`, `Germany` |

### Data Quality Notes (for later cleaning phases)
- `Description`: 1,454 nulls
- `CustomerID`: 135,080 nulls (~25% of rows)
- `Quantity`: negative values = returns/cancellations (min = -80,995)
- `UnitPrice`: can be 0 or negative (adjustments)
- `InvoiceDate`: stored as string — needs parsing to datetime
- Top country: United Kingdom (495,478 rows, ~91%)

---

## 3. Useful Columns Selected

**All 8 columns are kept** — each serves a purpose in the pipeline:

| Column | Use in Pipeline |
|--------|-----------------|
| `InvoiceNo` | Kafka message **key** (groups line items of the same invoice); detects cancellations (`C` prefix) |
| `StockCode` | Product identifier for product-level analytics |
| `Description` | Product name for dashboards |
| `Quantity` | Core metric — units sold; used in revenue = Quantity × UnitPrice |
| `InvoiceDate` | Timestamp for time-series / windowed streaming aggregations |
| `UnitPrice` | Core metric — used in revenue calculation |
| `CustomerID` | Customer analytics (RFM, segmentation, CLV) |
| `Country` | Geographic analytics / dashboards |

No columns are dropped at the producer stage. Cleaning, filtering, and feature engineering will happen in the Spark processing phase (Phase 3).

---

## 4. Directory Structure (Phase 2)

```
BigData/
├── .env                              # Producer configuration (env vars)
├── requirements.txt                  # Python dependencies
├── data/
│   └── raw/
│       └── online_retail.csv         # UCI Online Retail dataset (541,909 rows)
├── src/
│   ├── __init__.py
│   └── producer/
│       ├── __init__.py
│       ├── config.py                 # ProducerConfig dataclass + .env loader
│       └── kafka_producer.py         # Main Kafka producer entry point
└── README_PHASE2.md                  # This file
```

---

## 5. File Explanations

### 5.1 `.env`
Configuration file loaded by `python-dotenv`. Contains all producer settings:
```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=ecommerce-transactions
CSV_FILE_PATH=data/raw/online_retail.csv
STREAM_DELAY_SECONDS=0.1
MAX_MESSAGES=0
CLIENT_ID=ecommerce-producer
```

### 5.2 `src/producer/config.py`
- Defines `ProducerConfig` dataclass with defaults loaded from `.env`
- `from_overrides(...)` classmethod merges CLI arguments over `.env` values
- `is_unlimited` property: `MAX_MESSAGES <= 0` means stream the entire CSV

### 5.3 `src/producer/kafka_producer.py`
The main producer. Key functions:

| Function | Responsibility |
|----------|----------------|
| `load_csv_rows(csv_path)` | Generator that reads the CSV **row by row** using `csv.DictReader` (memory-efficient — no full DataFrame load) |
| `row_to_json(row)` | Converts a CSV row dict to a JSON string with proper type casting: `Quantity` → `int`, `UnitPrice` → `float`, empty `CustomerID` → `null` |
| `create_producer(config)` | Creates a `KafkaProducer` with `acks="all"`, `retries=3`, UTF-8 serializers |
| `stream(config)` | Main loop: reads rows → converts to JSON → publishes to Kafka → sleeps for `stream_delay_seconds` |
| `parse_args()` / `main()` | CLI argument parsing and entry point |

**Key features:**
- **Row-by-row reading** via generator (no `pandas` load — memory efficient for 541k rows)
- **JSON conversion** with type-safe casting
- **Continuous publishing** to Kafka topic
- **Configurable streaming speed** (`--delay` flag or `STREAM_DELAY_SECONDS` env var)
- **Exception handling**: `FileNotFoundError` for missing CSV, `KafkaError` + generic `Exception` catch per row with retry semantics
- **Graceful shutdown**: `SIGINT`/`SIGTERM` signal handlers set a `_SHUTDOWN` flag for clean flush + close
- **Logging**: `logging` module with timestamp/level/name format; logs every 1,000 messages and final `Sent=X | Failed=Y` summary
- **Kafka message key**: `InvoiceNo` (groups same-invoice line items to the same partition)

---

## 6. How to Run the Producer

### Prerequisites
- Kafka broker running in KRaft mode (see Section 8)
- Python venv activated

### Start the producer (default config from `.env`)
```powershell
cd C:\Users\sidha\Documents\trae_projects\BigData
.\.venv\Scripts\Activate.ps1
python -m src.producer.kafka_producer
```

### Override via CLI flags
```powershell
# Stream only 100 messages with 0.05s delay between them
python -m src.producer.kafka_producer --max-messages 100 --delay 0.05

# Use a different topic and CSV
python -m src.producer.kafka_producer --topic my-topic --csv data/raw/other.csv

# Fast mode: no delay, stream entire CSV
python -m src.producer.kafka_producer --delay 0
```

### CLI Flags
| Flag | Description | Default |
|------|-------------|---------|
| `--bootstrap-servers` | Kafka bootstrap servers | `localhost:9092` |
| `--topic` | Kafka topic name | `ecommerce-transactions` |
| `--csv` | Path to CSV dataset | `data/raw/online_retail.csv` |
| `--delay` | Seconds between messages | `0.1` |
| `--max-messages` | Max messages to send (0 = unlimited) | `0` |
| `--client-id` | Kafka client ID | `ecommerce-producer` |

---

## 7. Expected Kafka Messages

Each message published to `ecommerce-transactions` has:
- **Key:** `InvoiceNo` (string)
- **Value:** JSON object with all 8 columns

### Example message (as seen by a consumer)

```
key=536365, partition=0, offset=0
value:
{
  "InvoiceNo": "536365",
  "StockCode": "85123A",
  "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
  "Quantity": 6,
  "InvoiceDate": "2010/12/1 8:26",
  "UnitPrice": 2.55,
  "CustomerID": "17850",
  "Country": "United Kingdom"
}
```

### Type guarantees in the JSON
- `Quantity` → integer (e.g. `6`)
- `UnitPrice` → float (e.g. `2.55`)
- `CustomerID` → string or `null` (when missing in CSV)
- All other fields → string

---

## 8. Starting Kafka (KRaft Mode — No ZooKeeper)

### One-time setup (already done)
```powershell
$env:JAVA_HOME="C:\jdk-17"
$env:Path="C:\jdk-17\bin;C:\kafka\bin\windows;$env:Path"
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"

# Generate cluster UUID & format storage
$clusterId = kafka-storage.bat random-uuid
kafka-storage.bat format -t $clusterId -c C:\kafka\config\kraft\server.properties
```

### Start the broker
```powershell
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
kafka-server-start.bat C:\kafka\config\kraft\server.properties
```

> **Note:** `KAFKA_HEAP_OPTS` must be set because Kafka's batch script tries to use `wmic` (removed in modern Windows) to detect OS architecture. Setting heap opts explicitly skips that check.

### Create the topic
```powershell
kafka-topics.bat --create --topic ecommerce-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### List / describe topics
```powershell
kafka-topics.bat --list --bootstrap-server localhost:9092
```

---

## 9. Test Results (Phase 2)

All tests were executed against the running Kafka broker:

| Test | Result |
|------|--------|
| Producer connects to Kafka | ✅ Connected to `localhost:9092` |
| Publish 10 messages (delay 0.05s) | ✅ Sent=10, Failed=0 |
| Publish 50 messages (delay 0.02s, configurable speed) | ✅ Sent=50, Failed=0 |
| Total messages in topic | ✅ 60 messages, offsets 0–59 |
| Consumed messages match expected JSON | ✅ Correct types (int/float/string/null) |
| Exception handling (missing CSV file) | ✅ Logs `FileNotFoundError`, exits code 1 |
| Graceful shutdown (flush + close) | ✅ `Producer closed. Sent=X \| Failed=Y` |

---

## 10. Files Created in Phase 2

| File | Purpose |
|------|---------|
| [`.env`](file:///C:/Users/sidha/Documents/trae_projects/BigData/.env) | Producer configuration |
| [`data/raw/online_retail.csv`](file:///C:/Users/sidha/Documents/trae_projects/BigData/data/raw/online_retail.csv) | UCI Online Retail dataset |
| [`src/__init__.py`](file:///C:/Users/sidha/Documents/trae_projects/BigData/src/__init__.py) | Package marker |
| [`src/producer/__init__.py`](file:///C:/Users/sidha/Documents/trae_projects/BigData/src/producer/__init__.py) | Package marker |
| [`src/producer/config.py`](file:///C:/Users/sidha/Documents/trae_projects/BigData/src/producer/config.py) | Config dataclass + `.env` loader |
| [`src/producer/kafka_producer.py`](file:///C:/Users/sidha/Documents/trae_projects/BigData/src/producer/kafka_producer.py) | Main Kafka producer |
| [`README_PHASE2.md`](file:///C:/Users/sidha/Documents/trae_projects/BigData/README_PHASE2.md) | This file |

---

## 11. Phase 3 Preview (Not Started — Awaiting Approval)

Phase 3 will cover:
- Spark Structured Streaming consumer that reads from the Kafka topic
- Data cleaning (nulls, negative quantities/prices, cancellations)
- Feature engineering (revenue, time windows, derived columns)
- Writing cleaned data to SQLite

---

*End of Phase 2. Waiting for approval before proceeding to Phase 3.*
