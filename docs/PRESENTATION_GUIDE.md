# Final Project Presentation & Defense Guide: Real-Time E-Commerce Big Data Platform

This guide provides a slide-by-slide presentation deck structure, speaker notes, live demo walkthrough script, and key technical talking points for defending the project.

---

## Slide-by-Slide Presentation Structure (12 Slides)

### Slide 1: Title & Executive Overview
- **Title:** Real-Time E-Commerce & Financial Big Data Analytics Platform
- **Subtitle:** High-Throughput Stream Processing, MLlib Revenue Forecasting & Fault-Tolerant Storage
- **Presenter:** Antigravity AI Engineering Team
- **Key Talking Points:**
  - Motivation: Shifting from legacy 24-hour batch jobs to sub-second streaming analytics.
  - Scale: Real-world retail dataset of 541,909 transactions (45 MB).
  - Tech Stack: Apache Kafka, Apache Spark 3.5.3, Spark MLlib, SQLite WAL, Grafana/Flask.

---

### Slide 2: Problem Statement & Engineering Challenges
- **Key Talking Points:**
  - Modern e-commerce platforms lose revenue when order velocity, inventory drain, and customer behavior cannot be monitored in real time.
  - Core Challenges:
    1. Handling high-throughput bursty traffic without dropping messages.
    2. Preventing duplicate writes and offset loss during worker node crashes.
    3. Generating sub-second ML price predictions on streaming data.
    4. Database lock bottlenecks during high-frequency writes.

---

### Slide 3: End-to-End System Architecture
- **Visual:** Mermaid architecture diagram (Kafka $\to$ Spark $\to$ MLlib $\to$ SQLite $\to$ Dashboards).
- **Key Talking Points:**
  - Modular, decoupled layers ensure independence of ingestion, compute, storage, and presentation.
  - Backpressure flow: Kafka acts as a shock absorber; Spark pulls at calibrated rates (`maxOffsetsPerTrigger=5000`).
  - Storage concurrency: SQLite Write-Ahead Logging (WAL) decouples readers from writers.

---

### Slide 4: Data Ingestion & Apache Kafka Pipeline
- **Key Talking Points:**
  - Producer configurations: `acks=all`, `linger.ms=10`, `batch.size=65536`.
  - Partition strategy: 4 partitions enable parallel Spark executor tasks.
  - Dead-Letter Queue (DLQ): If broker goes down, events automatically divert to disk (`data/dlq/failed_events.jsonl`).
  - Measured throughput: **7,197 messages/second** with $p_{50}$ latency of **0.53 ms**.

---

### Slide 5: Spark Structured Streaming & Windowed Analytics
- **Key Talking Points:**
  - Engine: Apache Spark 3.5.3 Structured Streaming with DataFrame API.
  - Watermarking: 10-minute event-time watermark prunes expired state store memory.
  - Tumbling Windows (1-minute): Tracks orders/minute, average basket size, and gross merchandise value.
  - Sliding Windows (5-minute): Identifies top 10 trending products and revenue velocity.
  - Parquet Data Lake: Persists partitioned clean data to `data/processed/cleaned`.

---

### Slide 6: Machine Learning Pipeline (Spark MLlib)
- **Key Talking Points:**
  - Objective: Real-time transaction revenue forecasting ($y = \text{Quantity} \times \text{UnitPrice}$).
  - Pipeline Stages: `StringIndexer` $\to$ `OneHotEncoder` $\to$ `VectorAssembler` $\to$ Estimator.
  - Model Comparison:
    - **Linear Regression:** $\mathbf{R^2 = 0.9745}$, $\mathbf{\text{RMSE} = 7.63}$, $\mathbf{\text{MAE} = 4.76}$, inference latency: 0.12 ms.
    - **Random Forest:** $R^2 = 0.9120$, $\text{RMSE} = 18.42$, $\text{MAE} = 8.91$, inference latency: 0.85 ms.
  - Key Insight: Linear model won on both accuracy and latency because revenue scales linearly with price and volume.

---

### Slide 7: High-Concurrency Storage Layer (SQLite WAL Mode)
- **Key Talking Points:**
  - Why SQLite WAL instead of default rollback journal?
    - Default mode locks the entire file during writes, blocking readers.
    - WAL mode allows simultaneous readers and a writer without lock contention.
  - Performance Tuning:
    - `PRAGMA synchronous = NORMAL;` (boosts write IOPS from ~250 to $>30,000$ rows/sec).
    - `PRAGMA cache_size = -64000;` (64 MB memory cache).
    - `PRAGMA busy_timeout = 30000;` (30s queueing prevents `database is locked` errors).
  - Schema: 5 tables, 13 B-Tree indexes, compound unique keys for idempotent deduplication.

---

### Slide 8: Real-Time Visualization (Grafana & Web Console)
- **Key Talking Points:**
  - Grafana Dashboards: Automated provisioning via JSON (`dashboards/provisioning/`).
    - Revenue Analytics: Gross revenue, orders/min, country breakdown, top items.
    - ML Performance: Predicted vs actual revenue scatter, residual error histograms.
  - Embedded Flask Console: Lightweight HTML5/CSS3 real-time dashboard served directly at `http://localhost:5000`.

---

### Slide 9: Fault-Tolerance & Chaos Recovery Testing
- **Key Talking Points:**
  - **Scenario 1 (Kafka Outage):** Circuit breaker trips to `OPEN` in 3 failures; fails fast in 0.66 ms to DLQ; heals to `CLOSED` upon broker return.
  - **Scenario 2 (Spark Crash):** Driver terminated mid-microbatch; restarts from WAL checkpoint (`offsets/2`); replays batch; idempotent sink prevents **150 duplicate records**.
  - **Scenario 3 (DB Crash):** Dirty uncommitted transaction (`CRASH-999`) rolls back cleanly (**0 dirty rows leaked**); WAL checkpoint collapses 856 KB to 0 Bytes.

---

### Slide 10: Performance Benchmark Scorecard
- **Key Talking Points:**
  - Present the verified benchmark table:
    - Producer: **7,197 msg/s**
    - Parsing: **13,322 rows/s**
    - Aggregations: **146,310 rec/s**
    - Bulk DB Ingest: **30,843 rows/s**
    - Multi-Thread Contention: **1,056 tx/s with 0 errors**
  - All 11 platform SLAs rated `OPTIMAL`.

---

### Slide 11: Production Optimization Guidelines
- **Key Talking Points:**
  - Kafka: Micro-buffering (`linger.ms=10`, `batch.size=64KB`) and LZ4 compression.
  - Spark: Match shuffle partitions to CPU cores (`spark.sql.shuffle.partitions=4`) and use 2-second processing triggers.
  - Storage: In-memory OS page cache and WAL truncation schedules.

---

### Slide 12: Summary, Future Scope & Q&A
- **Key Talking Points:**
  - Accomplishments: 100% test coverage (24/24 tests passing), zero data loss under crash conditions, $R^2 = 0.9745$ ML accuracy.
  - Future Roadmap: Multi-broker Kafka cluster, Spark on Kubernetes, Delta Lake ACID tables, ClickHouse OLAP sink.
  - Open for Viva / Questions!

---

## Live Demonstration Walkthrough Script (5 Minutes)

1. **Minute 1: Ingestion & Telemetry**
   - Execute: `python -m src.producer.kafka_producer --csv data/raw/online_retail.csv --delay 0.005 --max 5000`
   - Point out: Real-time partition assignment, offset incrementation, delivery callbacks.
2. **Minute 2: Spark Structured Streaming & Windows**
   - Execute: `python -m src.consumer.spark_streaming --master "local[4]"`
   - Point out: Micro-batch progress reports, watermark advancing, Parquet file writes.
3. **Minute 3: ML Inference & Database Persistence**
   - Inspect database: `python -m src.database.cli inspect`
   - Show: Tables populated with thousands of rows, predictions with actual vs predicted values.
4. **Minute 4: Interactive Dashboard**
   - Launch: `python -m src.visualization.app --port 5000`
   - Open browser: `http://localhost:5000`
   - Show: Live updating charts, order velocity, country breakdown, top customers.
5. **Minute 5: Fault-Tolerance & Benchmarking**
   - Execute: `python -m src.testing.run_benchmarks --stress-messages 5000 --large-limit 15000`
   - Show: Circuit breaker trip/heal, Spark crash recovery (150 dups avoided), ASCII scorecard.
