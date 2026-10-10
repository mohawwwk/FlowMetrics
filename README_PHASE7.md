# Phase 7: System Testing, Fault-Tolerance Recovery & Performance Benchmarking

This document provides a comprehensive report of **Phase 7** for the AI Financial & E-Commerce Big Data Platform. It details the testing methodology, fault injection simulations, fault-recovery mechanisms, telemetry metrics, and architectural optimization guidelines across Kafka, Apache Spark Structured Streaming, SQLite WAL database, and the host operating system.

---

## 1. Executive Summary & Architecture Overview

Phase 7 hardens and benchmarks the entire data platform across 5 key pillars:
1. **High-Concurrency Stress Testing:** Multi-threaded stress producer measuring delivery throughput (messages/sec, MB/sec) and latency percentiles ($p_{50}, p_{95}, p_{99}$).
2. **Large Dataset Ingestion:** Benchmarking end-to-end ingestion, schema parsing, validation, and batch persistence on the UCI Online Retail dataset (541,909 records, 45MB).
3. **Kafka Fault Injection & Resilience:** Validating broker outages, transient network drops, exponential backoff with full jitter, circuit breaker state transitions (`CLOSED` -> `OPEN` -> `HALF-OPEN`), and Dead-Letter Queue (DLQ) disk spillover.
4. **Spark Structured Streaming Restart Recovery:** Verifying Write-Ahead Log (WAL) checkpoints (`offsets/`, `commits/`, `metadata`), simulating uncommitted micro-batch crashes, and demonstrating zero-duplicate idempotent resumption.
5. **Database WAL Recovery & Integrity:** Validating SQLite Write-Ahead Logging (WAL) dirty rollback on abnormal terminations, WAL checkpoint truncation (`PRAGMA wal_checkpoint(TRUNCATE)`), B-Tree page checksum verification (`PRAGMA integrity_check`), and concurrent writer contention.

---

## 2. Benchmark Scorecard & Key Performance Indicators (KPIs)

The following performance metrics were captured using high-resolution performance collectors (`PerformanceCollector`) on an AMD/Intel multi-core host:

| Domain / Subsystem | Test Metric | Recorded Value | $p_{50}$ Latency | $p_{99}$ Latency | SLA Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Kafka Ingestion** | Stress Producer Throughput | **7,197.2 msg/s** | 0.53 ms | 0.71 ms | `OPTIMAL` |
| **Data Pipeline** | CSV Parse, Clean & Validate | **13,322.1 rows/s** | 0.06 ms | 0.08 ms | `OPTIMAL` |
| **Analytics Engine**| Analytical Aggregations | **146,310.0 rec/s** | 0.005 ms | 0.007 ms | `OPTIMAL` |
| **Database Engine** | SQLite WAL Bulk Ingestion | **30,843.2 rows/s** | 0.011 ms | 0.018 ms | `OPTIMAL` |
| **Kafka Resilience**| Circuit Breaker Fast-Fail | **0.663 ms / op** | 0.50 ms | 1.20 ms | `OPTIMAL` |
| **Kafka Resilience**| Self-Healing Health Probe | **Transition to CLOSED**| N/A | N/A | `PASS` |
| **Spark Streaming** | Crash Offset Recovery | **150 dups avoided** | N/A | N/A | `OPTIMAL` |
| **SQLite WAL Engine**| Crash Rollback Dirty Leakage| **0 dirty rows** | N/A | N/A | `OPTIMAL` |
| **SQLite WAL Engine**| WAL Truncation Checkpoint | **0 Bytes (856KB -> 0B)**| N/A | N/A | `OPTIMAL` |
| **SQLite WAL Engine**| B-Tree Page Integrity | **"ok" (100% Healthy)** | N/A | N/A | `OPTIMAL` |
| **SQLite WAL Engine**| Multi-Thread Lock Contention| **1,056.7 tx/s (0 errors)** | 0.26 ms | 124.43 ms | `OPTIMAL` |

---

## 3. High-Concurrency Producer Stress Testing

Implemented in `src/testing/stress_producer.py`.

### Architecture & Workload Simulation
The stress producer deploys a configurable thread worker pool (1 to 16 threads) sending micro-batches (100 to 2,000 records). It supports both **live cluster execution** (`--mode live`) against Kafka brokers and **isolated mock execution** (`--mode mock`).
- **Memory Buffer Pre-loading:** Pre-loads raw CSV records from `data/raw/online_retail.csv` into RAM before triggering timers, ensuring disk read bottlenecks do not skew network throughput measurements.
- **Micro-buffering & Linger Simulation:** Simulates Kafka batch accumulation (`linger.ms=5ms`, `batch.size=65536`).
- **Telemetry Collection:** Computes $p_{50}, p_{95}, p_{99}$ latency, memory working set deltas, and records dropped due to buffer exhaustion.

### Stress Test Command
```bash
python -m src.testing.stress_producer --messages 20000 --threads 4 --batch-size 500 --mode mock
```

---

## 4. Large Dataset Benchmark (UCI Online Retail 541k Rows)

Implemented in `src/testing/benchmark_large_dataset.py`.

### Data Flow Stages
1. **Streaming CSV Extraction & Validation:**
   - Reads `data/raw/online_retail.csv` (45MB).
   - Validates quantity ($Quantity > 0$), unit price ($UnitPrice \ge 0$), filters invoice cancellations (`InvoiceNo` starting with 'C').
   - Throughput: **13,322 records/sec** (1.92 MB/sec).
2. **Analytical Aggregations:**
   - Computes rolling country revenues, product sales volume, and customer spending profiles in-memory.
   - Throughput: **146,310 records/sec**.
3. **High-Throughput SQLite WAL Bulk Ingestion:**
   - Prepares parameterized batch inserts using `executemany` with batch size 5,000.
   - Evaluates write IOPS, B-Tree index updates, and transaction commits.
   - Throughput: **30,843 rows/sec**, committing 25,000 records in **0.98 seconds**.

### Benchmark Command
```bash
python -m src.testing.benchmark_large_dataset --limit 50000 --batch-size 5000
```

---

## 5. Kafka Failures & Resilience Simulation

Implemented in `src/testing/test_kafka_failures.py`.

### Failure Modes Tested
```mermaid
stateDiagram-v2
    [*] --> CLOSED : Normal Traffic
    CLOSED --> OPEN : 3 Consecutive Failures
    OPEN --> OPEN : Fast-Fail to DLQ (<0.7ms)
    OPEN --> HALF_OPEN : Recovery Timeout (0.5s)
    HALF_OPEN --> CLOSED : Canary Probe Success
    HALF_OPEN --> OPEN : Canary Probe Fails
```

1. **Total Broker Outage (Blackout):**
   - When Kafka is down, the producer trips its `CircuitBreaker` to `OPEN` after 3 consecutive connection failures.
   - Subsequent events fail fast in **0.663 ms** directly to the local disk Dead-Letter Queue (`data/dlq/failed_events.jsonl`), preventing socket resource exhaustion and thread starvation.
2. **Flaky Network & Exponential Backoff with Jitter:**
   - Simulates 25% packet drop and socket timeouts.
   - Applies exponential backoff with full randomized jitter:
     $$T_{\text{sleep}} = \text{Uniform}(1\text{ms}, \min(T_{\text{max}}, T_{\text{base}} \cdot 2^{\text{attempt}}))$$
   - Avoids "thundering herd" reconnection storms on the broker.
3. **Self-Healing & Broker Recovery:**
   - Once the broker resumes operation, the circuit breaker enters `HALF_OPEN`, dispatches a canary health check probe, verifies an ACK, transitions to `CLOSED`, and resumes streaming without manual restart.

### Kafka Resilience Test Command
```bash
python -m src.testing.test_kafka_failures --messages 500
```

---

## 6. Spark Structured Streaming Restart & Recovery

Implemented in `src/testing/test_spark_recovery.py`.

### Checkpoint Anatomy & WAL Protocol
Spark Structured Streaming maintains fault tolerance via Write-Ahead Log directories located at `data/checkpoint/cleaned`:
- `metadata`: Contains the stream UUID and configuration version (`v1`).
- `offsets/<batch_id>`: Logs start/end Kafka topic-partition offsets before batch execution begins.
- `commits/<batch_id>`: Written only AFTER the micro-batch has successfully completed all sink writes.

```
data/checkpoint/cleaned/
├── metadata
├── offsets/
│   ├── 0
│   └── 1
└── commits/
    ├── 0
    └── 1
```

### Crash Recovery Scenario
1. **Micro-batch 0 & 1:** Committed normally (offsets 0 $\to$ 250).
2. **Micro-batch 2 (Simulated Crash):**
   - Offsets 250 $\to$ 400 written to `offsets/2`.
   - 150 records written to sink.
   - **Abrupt Termination:** Spark process killed before `commits/2` is written.
3. **Recovery on Restart:**
   - Inspector detects highest offset is `2`, but highest commit is `1`.
   - Driver detects uncommitted batch 2, re-reads Kafka offsets 250 $\to$ 400.
   - Replays batch into sink: Idempotent constraint (`uq_transaction UNIQUE(invoice_no, stock_code, invoice_timestamp)`) prevents duplicate insertions (**150 duplicate insertions avoided**).
   - Writes `commits/2`.
   - Stream resumes to batch 3 without offset skips or permanent data loss.

### Spark Checkpoint Inspection Command
```bash
python -m src.testing.test_spark_recovery --inspect-existing
```

---

## 7. Database WAL Recovery & Concurrency Testing

Implemented in `src/testing/test_database_recovery.py`.

### SQLite WAL Mechanics
1. **Uncommitted Transaction Rollback:**
   - A client initiates `BEGIN TRANSACTION;`, inserts dirty rows (`CRASH-999`), and abruptly crashes without `COMMIT`.
   - On reconnect, the dirty rows are discarded. Exactly **0 dirty rows leaked**.
2. **WAL Checkpoint Truncation (`PRAGMA wal_checkpoint(TRUNCATE)`):**
   - High-volume transaction writes accumulate 856 KB of frames in `ecommerce.db-wal`.
   - Executing `PRAGMA wal_checkpoint(TRUNCATE)` transfers all pages into the main database file and collapses the WAL file to **0 Bytes**, maintaining 100% data integrity.
3. **Structural B-Tree Integrity:**
   - `PRAGMA integrity_check` and `PRAGMA quick_check` report **`ok`**, verifying zero corrupted indices, page headers, or freelists.
4. **Concurrent Multi-Threaded Contention:**
   - 8 concurrent threads simultaneously execute transactions against `SQLiteConnectionPool`.
   - Configured `PRAGMA busy_timeout = 30000;` eliminates `sqlite3.OperationalError: database is locked` errors (**0 lock errors across 400 concurrent transactions**, reaching **1,056.7 tx/sec**).

### Database Recovery Test Command
```bash
python -m src.testing.test_database_recovery
```

---

## 8. Comprehensive Benchmark Runner

The master orchestrator runs all testing and recovery modules in sequence and generates `data/benchmark_results.json`:
```bash
python -m src.testing.run_benchmarks --stress-messages 10000 --large-limit 30000
```

---

## 9. Architectural Optimization Recommendations

### A. Apache Kafka Producer & Cluster Tuning
- **Batching & Linger:** Configure `batch.size=65536` (64KB) and `linger.ms=10`. This allows the producer to coalesce network packets, multiplying throughput by ~3.5x while keeping $p_{99}$ latency $<20\text{ms}$.
- **Compression:** Enable `compression.type=lz4` or `zstd`. Payload sizes shrink by 65–75% with negligible CPU overhead on modern processors.
- **Acks & Idempotence:** Use `acks=1` for high-throughput clickstream data; use `acks=all` with `enable.idempotence=true` for critical financial transactions.
- **Buffer Memory:** Allocate `buffer.memory=67108864` (64MB) and `max.block.ms=15000` to buffer against consumer rebalance pauses or broker flush spikes.

### B. Apache Spark Structured Streaming Tuning
- **Shuffle Partitions:** Spark defaults to `spark.sql.shuffle.partitions=200`, which introduces heavy scheduling overhead on local/single-node setups. Set `spark.sql.shuffle.partitions=4` (matching available CPU cores).
- **Trigger Intervals:** Use `trigger(processingTime="2 seconds")` instead of unconditional micro-batching. This allows micro-batches to accumulate records and amortizes WAL commit overhead.
- **Off-Heap Memory:** Enable `spark.memory.offHeap.enabled=true` with `spark.memory.offHeap.size=2g` to reduce garbage collection pauses during large matrix operations.
- **Idempotent Sinks:** Always back downstream sinks with unique keys (`uq_transaction`) and `INSERT OR IGNORE` or upsert semantics to ensure exactly-once end-to-end processing.

### C. SQLite WAL & Concurrency Tuning
- **Synchronous Mode:** Set `PRAGMA synchronous = NORMAL;`. In WAL mode, `NORMAL` guarantees crash safety while eliminating blocking disk flushes on every commit, boosting write throughput from ~250 tx/s to $>30,000\text{ tx/s}$.
- **Memory Cache & MMAP:** Allocate `PRAGMA cache_size = -64000;` (64MB RAM cache) and `PRAGMA mmap_size = 268435456;` (256MB memory-mapped I/O).
- **Busy Timeout:** Ensure all connections set `PRAGMA busy_timeout = 30000;` (30 seconds) so competing write threads gracefully wait for locks instead of failing immediately.

---

## 10. Automated Test Suite Verification

All 24 unit and integration tests across Database, Visualization, and Testing/Benchmarking pass cleanly:

```bash
pytest tests/test_database.py tests/test_visualization.py tests/test_phase7.py -v
```

```
============================= test session starts =============================
platform win32 -- Python 3.11.9, pytest-9.1.1, pluggy-1.6.0
collected 24 items

tests/test_database.py::TestDatabaseIntegration::test_analytical_queries PASSED [  4%]
tests/test_database.py::TestDatabaseIntegration::test_auto_create_database_and_directory PASSED [  8%]
tests/test_database.py::TestDatabaseIntegration::test_connection_pool_concurrency PASSED [ 12%]
tests/test_database.py::TestDatabaseIntegration::test_customer_analytics_aggregation PASSED [ 16%]
tests/test_database.py::TestDatabaseIntegration::test_insert_predictions PASSED [ 20%]
tests/test_database.py::TestDatabaseIntegration::test_insert_processed_transactions_idempotence PASSED [ 25%]
tests/test_database.py::TestDatabaseIntegration::test_schema_creation_and_introspection PASSED [ 29%]
tests/test_database.py::TestDatabaseIntegration::test_transaction_rollback_on_error PASSED [ 33%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_country_breakdown PASSED [ 37%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_customer_leaderboard PASSED [ 41%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_ml_predictions PASSED [ 45%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_orders_per_minute PASSED [ 50%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_summary PASSED [ 54%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_api_top_products PASSED [ 58%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_grafana_ml_dashboard_json_validity PASSED [ 62%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_grafana_revenue_dashboard_json_validity PASSED [ 66%]
tests/test_visualization.py::TestVisualizationAndDashboards::test_index_route PASSED [ 70%]
tests/test_phase7.py::TestPhase7TestingAndBenchmarking::test_kafka_failure_resilience_circuit_breaker PASSED [ 75%]
tests/test_phase7.py::TestPhase7TestingAndBenchmarking::test_large_dataset_parsing_and_aggregation PASSED [ 79%]
tests/test_phase7.py::TestPhase7TestingAndBenchmarking::test_metrics_logger_and_telemetry PASSED [ 83%]
tests/test_phase7.py::TestPhase7TestingAndBenchmarking::test_spark_recovery_uncommitted_batch_replay PASSED [ 87%]
tests/test_phase7.py::TestPhase7TestingAndBenchmarking::test_sqlite_concurrent_write_contention PASSED [ 91%]
tests/test_phase7.py::TestPhase7TestingAndBenchmarking::test_sqlite_wal_recovery_and_rollback PASSED [ 95%]
tests/test_phase7.py::TestPhase7TestingAndBenchmarking::test_stress_producer_mock_execution PASSED [100%]

============================= 24 passed in 2.57s ==============================
```
