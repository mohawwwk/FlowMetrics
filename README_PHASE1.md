# Phase 1 — Environment Setup & Architecture Design

**Project:** Real-Time E-Commerce Analytics Pipeline using Kafka, Spark Streaming, MLlib and Grafana
**Phase:** 1 of N (Setup + Architecture)
**Status:** COMPLETE — Awaiting approval for Phase 2
**Date:** 2026-10-11

---

## 1. Project Architecture (Complete)

```
                          ┌─────────────────────────────────────────────────┐
                          │              E-Commerce Dataset                 │
                          │        (CSV / JSON — transactions)              │
                          └──────────────────────┬──────────────────────────┘
                                                 │  (read)
                                                 ▼
                          ┌─────────────────────────────────────────────────┐
                          │           Kafka Producer (Python)                │
                          │   src/producer — pushes transactions to Kafka    │
                          └──────────────────────┬──────────────────────────┘
                                                 │  publish (JSON)
                                                 ▼
                          ┌─────────────────────────────────────────────────┐
                          │           Apache Kafka Broker                     │
                          │   Topic: ecommerce-transactions                   │
                          │   (local, KRaft mode — no ZooKeeper)              │
                          └──────────────────────┬──────────────────────────┘
                                                 │  subscribe / stream
                                                 ▼
                          ┌─────────────────────────────────────────────────┐
                          │     Spark Structured Streaming (PySpark)          │
                          │   src/consumer — consumes from Kafka topic        │
                          └──────────────────────┬──────────────────────────┘
                                                 │
                              ┌──────────────────┼──────────────────┐
                              ▼                  ▼                  ▼
                   ┌─────────────────┐ ┌────────────────┐ ┌──────────────┐
                   │  Data Cleaning   │ │ Feature Eng.   │ │   MLlib      │
                   │  src/processing  │ │ src/processing │ │  src/ml      │
                   │  (nulls, types,  │ │ (aggregates,   │ │ (risk/return │
                   │   dedup, filter) │ │  time windows) │ │  prediction) │
                   └────────┬────────┘ └───────┬────────┘ └──────┬───────┘
                            │                  │                 │
                            └──────────────────┼─────────────────┘
                                               ▼
                                   ┌───────────────────────┐
                                   │   SQLite (sink store)   │
                                   │  tables: transactions,  │
                                   │  features, predictions   │
                                   └───────────┬───────────┘
                                               │  query
                                               ▼
                                   ┌───────────────────────┐
                                   │   Grafana Dashboards    │
                                   │  real-time metrics &    │
                                   │  ML predictions         │
                                   └───────────────────────┘
```

---

## 2. Component Explanations

| # | Component | Version | Role |
|---|-----------|---------|------|
| 1 | **Python** | 3.11 | Host language for the producer, streaming consumer, ML training, and orchestration scripts. |
| 2 | **Apache Spark (PySpark)** | 3.5.3 | Distributed compute engine. Provides Spark SQL (batch/streaming DataFrames), Structured Streaming (Kafka source), and MLlib (machine learning). Runs on the JVM. |
| 3 | **Apache Kafka** | 3.8.0 | Distributed event-streaming / message broker. Buffers incoming e-commerce transactions as a durable, replayable log of events. Spark consumes from it. |
| 4 | **Spark Structured Streaming** | (built into Spark) | Micro-batch stream processing engine inside Spark. Reads the Kafka topic as an unbounded DataFrame and applies transformations incrementally. |
| 5 | **Spark MLlib** | (built into Spark) | Spark's scalable machine-learning library. Used to train a predictive model (e.g. purchase/risk prediction) on the engineered features. |
| 6 | **SQLite** | built-in (Python stdlib) | Lightweight, file-based relational database. Serves as the sink for cleaned records, feature tables, and ML predictions. Zero-install — no server process. |
| 7 | **Grafana** | (to be installed in later phase) | Open-source analytics & visualization platform. Reads from SQLite and renders real-time dashboards (transaction volume, revenue, ML scores). |
| 8 | **Hadoop winutils** | 3.3.6 | Windows helper binary required by Spark's Hadoop `FileSystem` layer for file permissions / temp dirs on Windows. Not a full Hadoop install. |
| 9 | **JDK (Temurin)** | 17.0.20 | Java runtime required to run Spark and Kafka (both are JVM applications). |

---

## 3. Data Flow

1. **Source → Producer:** A Python producer reads the e-commerce transaction dataset (CSV/JSON) from `data/raw/` and publishes each record as a JSON message to the Kafka topic `ecommerce-transactions`.
2. **Kafka:** The broker durably stores the messages in the topic. Multiple consumers can read independently; messages are replayable.
3. **Spark Streaming:** A PySpark Structured Streaming job subscribes to the Kafka topic. It receives each micro-batch as a DataFrame with columns `key`, `value` (raw JSON), `topic`, `partition`, `offset`, `timestamp`.
4. **Processing:** The streaming DataFrame is parsed (JSON → typed columns), cleaned (null handling, type casting, deduplication, filtering), and features are engineered (windowed aggregates, derived columns).
5. **ML Inference / Training:** Engineered features are passed to an MLlib model (training happens on a batch snapshot; inference runs on the stream). Predictions are produced per record.
6. **Sink:** Cleaned records, features, and predictions are written to SQLite tables (via JDBC) for persistence and querying.
7. **Visualization:** Grafana connects to SQLite and renders dashboards that refresh as new rows land in the sink tables.

---

## 4. Folder Structure

```
BigData/
├── data/
│   ├── raw/              # Raw ecommerce dataset (CSV/JSON) — input
│   ├── processed/        # Cleaned / feature-engineered intermediate data
│   └── checkpoint/       # Spark Structured Streaming checkpoints (offsets)
├── src/
│   ├── producer/         # Kafka producer — reads dataset, publishes to topic
│   ├── consumer/         # Spark Structured Streaming consumer — reads Kafka
│   ├── processing/       # Data cleaning + feature engineering logic
│   ├── ml/               # MLlib model training & inference
│   └── visualization/    # SQLite sink writer + Grafana query helpers
├── config/               # Kafka, Spark, pipeline configuration files
├── dashboards/           # Grafana dashboard JSON exports
├── notebooks/            # Exploratory Jupyter notebooks
├── docs/                 # Project documentation
├── tests/                # Unit / integration tests
├── .venv/                # Python 3.11 virtual environment
├── requirements.txt      # Python dependencies
└── README_PHASE1.md      # This file
```

The folder structure above has been **created on disk** (empty directories — no application code yet, per Phase 1 rules).

---

## 5. Tech Stack & Installed Versions

| Tool | Version | Install Path |
|------|---------|--------------|
| JDK (Eclipse Temurin) | 17.0.20.1 | `C:\jdk-17` |
| Apache Spark | 3.5.3 | `C:\spark` |
| Hadoop winutils | 3.3.6 | `C:\hadoop\bin` |
| Apache Kafka (Scala 2.13) | 3.8.0 | `C:\kafka` |
| Python | 3.11.9 | `.venv\Scripts\python.exe` |

**Persistent user environment variables set:**
```
JAVA_HOME           = C:\jdk-17
SPARK_HOME          = C:\spark
HADOOP_HOME         = C:\hadoop
KAFKA_HOME          = C:\kafka
PYSPARK_PYTHON      = C:\Users\sidha\Documents\trae_projects\BigData\.venv\Scripts\python.exe
PYSPARK_DRIVER_PYTHON = C:\Users\sidha\Documents\trae_projects\BigData\.venv\Scripts\python.exe
```
PATH additions (user): `C:\jdk-17\bin`, `C:\spark\bin`, `C:\kafka\bin\windows`.

---

## 6. requirements.txt — Dependencies & Why Each Is Required

See [requirements.txt](file:///C:/Users/sidha/Documents/trae_projects/BigData/requirements.txt).

| Dependency | Version | Why Required |
|------------|---------|--------------|
| **pyspark** | 3.5.3 | The Python API for Apache Spark. Brings Spark SQL, Structured Streaming, and MLlib into Python. This is the core compute engine. |
| **kafka-python** | 2.0.2 | Pure-Python Kafka client. Used by the producer script to publish transaction records to the Kafka topic (and optionally for simple consumers/tests). |
| **pandas** | 2.2.3 | Data manipulation library. Used for initial dataset exploration, converting between Spark DataFrames and pandas, and small-data transformations. |
| **numpy** | 1.26.4 | Numerical computing foundation. Required by pandas and by MLlib feature vectors; used for array math in feature engineering. |
| **findspark** | 2.0.1 | Helper that locates the Spark installation (`SPARK_HOME`) and makes `pyspark` importable in scripts/notebooks where the environment isn't pre-configured. Defensive utility. |
| **python-dotenv** | 1.0.1 | Loads environment variables from a `.env` file. Keeps configuration (Kafka bootstrap servers, topic names, DB paths, etc.) out of source code. |

**Built-in (no install needed):** `sqlite3` (Python standard library) — used as the sink database.

---

## 7. Setup Guide — Windows Terminal Commands

> All commands run in **PowerShell**. Environment variables set with `setx` apply to **new** terminal sessions; reopen your terminal after running them.

### 7.1 Java (JDK 17 — Eclipse Temurin)

```powershell
# Download latest Temurin JDK 17 (Windows x64)
curl.exe -L -o C:\temp\jdk17.zip "https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"

# Extract
New-Item -ItemType Directory -Force -Path C:\temp\jdk_extract | Out-Null
tar -xf C:\temp\jdk17.zip -C C:\temp\jdk_extract
$inner = Get-ChildItem C:\temp\jdk_extract -Directory | Select-Object -First 1
Move-Item $inner.FullName C:\jdk-17

# Set environment variables
setx JAVA_HOME "C:\jdk-17"
# Add C:\jdk-17\bin to user PATH (via System Properties or PowerShell)
```

### 7.2 Apache Spark 3.5.3 + winutils

```powershell
# Download Spark 3.5.3 (pre-built for Hadoop 3)
curl.exe -L -o C:\temp\spark.tgz "https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz"

# Extract
New-Item -ItemType Directory -Force -Path C:\temp\spark_extract | Out-Null
tar -xf C:\temp\spark.tgz -C C:\temp\spark_extract
$inner = Get-ChildItem C:\temp\spark_extract -Directory | Select-Object -First 1
Move-Item $inner.FullName C:\spark

# Download winutils.exe + hadoop.dll for Hadoop 3.3.6
New-Item -ItemType Directory -Force -Path C:\hadoop\bin | Out-Null
curl.exe -L -o C:\hadoop\bin\winutils.exe "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
curl.exe -L -o C:\hadoop\bin\hadoop.dll "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"

# Set environment variables
setx SPARK_HOME "C:\spark"
setx HADOOP_HOME "C:\hadoop"
# Add C:\spark\bin to user PATH
```

### 7.3 Apache Kafka 3.8.0

```powershell
# Download Kafka 3.8.0 (Scala 2.13 build)
curl.exe -L -o C:\temp\kafka.tgz "https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz"

# Extract
New-Item -ItemType Directory -Force -Path C:\temp\kafka_extract | Out-Null
tar -xf C:\temp\kafka.tgz -C C:\temp\kafka_extract
$inner = Get-ChildItem C:\temp\kafka_extract -Directory | Select-Object -First 1
Move-Item $inner.FullName C:\kafka

# Set environment variable
setx KAFKA_HOME "C:\kafka"
# Add C:\kafka\bin\windows to user PATH
```

### 7.4 Python 3.11 Virtual Environment & Dependencies

```powershell
cd C:\Users\sidha\Documents\trae_projects\BigData

# Create venv with Python 3.11
C:\Users\sidha\AppData\Local\Programs\Python\Python311\python.exe -m venv .venv

# Activate
.\.venv\Scripts\Activate.ps1

# Upgrade pip & install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# Point PySpark at this venv's Python
setx PYSPARK_PYTHON "C:\Users\sidha\Documents\trae_projects\BigData\.venv\Scripts\python.exe"
setx PYSPARK_DRIVER_PYTHON "C:\Users\sidha\Documents\trae_projects\BigData\.venv\Scripts\python.exe"
```

---

## 8. How to Verify Every Installation

Open a **fresh** PowerShell terminal (so `setx` variables load) and run:

### 8.1 Java
```powershell
java -version    # expect: openjdk version "17.0.20.1"
javac -version   # expect: javac 17.0.20.1
echo $env:JAVA_HOME   # expect: C:\jdk-17
```

### 8.2 Spark
```powershell
spark-submit --version
# expect: version 3.5.3, Using Scala version 2.12.18, OpenJDK 64-Bit Server VM, 17.0.20.1
echo $env:SPARK_HOME   # expect: C:\spark
```

### 8.3 winutils (Hadoop)
```powershell
winutils.exe ls C:\hadoop
# expect: a directory listing line (no "cannot find" error)
echo $env:HADOOP_HOME   # expect: C:\hadoop
```

### 8.4 Kafka
```powershell
kafka-topics.bat --version
# expect: 3.8.0
echo $env:KAFKA_HOME   # expect: C:\kafka
```

### 8.5 Python dependencies
```powershell
cd C:\Users\sidha\Documents\trae_projects\BigData
.\.venv\Scripts\python.exe -c "import pyspark, kafka, pandas, numpy, findspark, dotenv; print('pyspark', pyspark.__version__); print('pandas', pandas.__version__); print('numpy', numpy.__version__); print('kafka-python', kafka.__version__)"
# expect: pyspark 3.5.3, pandas 2.2.3, numpy 1.26.4, kafka-python 2.0.2
```

### 8.6 End-to-end PySpark smoke test
```powershell
cd C:\Users\sidha\Documents\trae_projects\BigData
.\.venv\Scripts\python.exe -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').appName('smoke').getOrCreate(); print('rows =', s.createDataFrame([(1,'a'),(2,'b'),(3,'c')],['id','v']).count()); s.stop()"
# expect: rows = 3
```

---

## 9. Expected Outputs (Verification Results)

The following outputs were observed during Phase 1 verification:

| Check | Expected Output | Actual (Verified) |
|-------|-----------------|-------------------|
| `java -version` | OpenJDK 17.x | `openjdk version "17.0.20.1"` |
| `javac -version` | javac 17.x | `javac 17.0.20.1` |
| `spark-submit --version` | Spark 3.5.3, Scala 2.12.18 | `version 3.5.3`, `Using Scala version 2.12.18, OpenJDK ... 17.0.20.1` |
| `kafka-topics.bat --version` | 3.8.0 | `3.8.0` |
| `winutils.exe ls C:\hadoop` | directory listing | listed `C:\hadoop` successfully |
| Python imports | no errors, correct versions | `pyspark 3.5.3 \| pandas 2.2.3 \| numpy 1.26.4 \| kafka-python 2.0.2` |
| PySpark smoke test | `rows = 3` | `rows = 3` |

> **Note:** The warning `Unable to load native-hadoop library for your platform... using builtin-java classes where applicable` is **harmless** on Windows and can be ignored.

---

## 10. Files Created / Modified in Phase 1

| File / Directory | Action |
|------------------|--------|
| [requirements.txt](file:///C:/Users/sidha/Documents/trae_projects/BigData/requirements.txt) | Created |
| [README_PHASE1.md](file:///C:/Users/sidha/Documents/trae_projects/BigData/README_PHASE1.md) | Created (this file) |
| `data/raw/`, `data/processed/`, `data/checkpoint/` | Created (empty) |
| `src/producer/`, `src/consumer/`, `src/processing/`, `src/ml/`, `src/visualization/` | Created (empty) |
| `config/`, `dashboards/`, `notebooks/`, `docs/`, `tests/` | Created (empty) |
| `.venv/` | Created (Python 3.11 venv with dependencies) |

**No application code was written** — only environment setup, folder structure, and documentation.

---

## 11. Phase 2 Preview (Not Started — Awaiting Approval)

Phase 2 will cover:
- Obtaining / placing the e-commerce transaction dataset in `data/raw/`
- Writing the Kafka producer (`src/producer/`)
- Writing the Spark Structured Streaming consumer (`src/consumer/`)
- Data cleaning & feature engineering (`src/processing/`)
- Sink to SQLite

---

*End of Phase 1. Waiting for approval before proceeding to Phase 2.*
