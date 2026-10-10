# Technical Viva Voce Examination Guide: Questions & Detailed Answers

This document provides 35 technical viva voce examination questions and in-depth answers across all layers of the Big Data & Streaming Analytics platform.

---

## Category 1: Distributed Systems & Apache Kafka

### Q1: What role does Apache Kafka play in this architecture, and why not stream directly into Spark?
**Answer:**  
Kafka functions as a distributed, durable, decoupled commit log. Streaming directly from source clients into Spark would introduce tight coupling; if Spark experiences a garbage collection pause, executor failure, or micro-batch delay, the upstream clients would experience blocking backpressure or drop events. Kafka acts as an asynchronous buffer and shock absorber, persisting events across 4 partitions with replayable offsets.

---

### Q2: What is the difference between Kafka producer acknowledgments `acks=0`, `acks=1`, and `acks=all`? Which one did you use and why?
**Answer:**  
- `acks=0`: Producer does not wait for any broker acknowledgment. Highest throughput, but highest risk of data loss.
- `acks=1`: Producer waits for the partition leader to write the message to its local log. Low latency, but message can be lost if leader crashes before replicas sync.
- `acks=all` (or `-1`): Producer waits for the leader and all in-sync replicas (ISR) to commit the message. Guarantees zero data loss.  
We used `acks=all` with `retries=3` for the primary producer to guarantee financial transaction durability, and `acks=1` during high-throughput stress testing to evaluate maximum throughput.

---

### Q3: What is the purpose of `linger.ms` and `batch.size` in Kafka?
**Answer:**  
By default, Kafka sends records immediately (`linger.ms=0`). By configuring `linger.ms=10` and `batch.size=65536` (64 KB), the producer artificially pauses for up to 10 milliseconds to coalesce individual records into a single network packet. This amortizes TCP socket overhead and increases throughput by ~3.5x with negligible latency impact.

---

### Q4: How does Kafka partition assignment affect Spark Structured Streaming parallelism?
**Answer:**  
In Spark Structured Streaming, each partition of a Kafka topic maps directly to one Spark task in a micro-batch. If a topic has 4 partitions, Spark generates 4 parallel reading tasks per micro-batch, allowing concurrent execution across executor CPU cores.

---

### Q5: What is a Dead-Letter Queue (DLQ), and how is it implemented when Kafka fails?
**Answer:**  
A Dead-Letter Queue stores un-deliverable messages that fail processing or network transmission after exhausting retry policies. In our platform, when the Kafka broker is offline and the `CircuitBreaker` trips to `OPEN`, outgoing transactions are appended to `data/dlq/failed_events.jsonl` on local disk, ensuring zero permanent data loss until broker connectivity is restored.

---

### Q6: What is the Circuit Breaker pattern, and what states does it have?
**Answer:**  
The Circuit Breaker pattern prevents an application from repeatedly attempting an operation that is guaranteed to fail, avoiding thread pool exhaustion. It has three states:
1. `CLOSED`: Normal operation; requests pass through.
2. `OPEN`: Tripped after consecutive failures (e.g., 3); requests fail fast immediately without making network calls.
3. `HALF-OPEN`: After a cooldown timeout, a canary probe is sent. If it succeeds, the breaker resets to `CLOSED`; if it fails, it returns to `OPEN`.

---

### Q7: Why use exponential backoff with full jitter instead of fixed sleep intervals during reconnection?
**Answer:**  
Fixed sleep intervals cause all retrying clients to re-attempt connections simultaneously when a broker restarts, causing a "thundering herd" connection storm that can crash the broker again. Full jitter distributes retry attempts uniformly over time:
$$T_{\text{sleep}} = \text{Uniform}(0, \min(T_{\text{max}}, T_{\text{base}} \cdot 2^{\text{attempt}}))$$

---

## Category 2: Apache Spark Structured Streaming & Windowing

### Q8: What is Apache Spark Structured Streaming, and how does it differ from legacy Spark DStreams?
**Answer:**  
Spark DStreams (Discretized Streams) operated on raw RDDs using processing-time batch intervals. Structured Streaming is built on top of the Spark SQL Catalyst Optimizer and Tungsten execution engine. It models the stream as an unbounded append-only table, allowing declarative SQL queries, native event-time watermarking, and end-to-end exactly-once guarantees.

---

### Q9: What is the difference between Event Time and Processing Time?
**Answer:**  
- **Event Time:** The timestamp embedded inside the record when the event actually occurred at the source (e.g., `InvoiceDate` in retail transactions).
- **Processing Time:** The local clock time of the Spark executor machine when it processes the micro-batch.  
Event time is critical for accurate financial windowing because network delays and out-of-order deliveries can distort processing time calculations.

---

### Q10: What is Watermarking in Spark Structured Streaming, and why is it necessary?
**Answer:**  
Watermarking defines the threshold of allowable lateness for incoming data:
$$\text{Watermark}_t = \max(t_{\text{event}}) - \tau_{\text{delay}}$$
We set a 10-minute watermark. Any late-arriving event older than 10 minutes behind the maximum seen event time is dropped. This allows Spark's state store to safely evict old window aggregates from RAM, preventing out-of-memory errors on unbounded streams.

---

### Q11: What is the difference between a Tumbling Window and a Sliding Window?
**Answer:**  
- **Tumbling Window:** Non-overlapping, contiguous time intervals (e.g., 1-minute window: 10:00–10:01, 10:01–10:02). Used in our project for tracking orders per minute and gross revenue.
- **Sliding Window:** Overlapping intervals with a specified slide duration (e.g., 5-minute window sliding every 1 minute). Used for identifying trending top-selling products over rolling time frames.

---

### Q12: How does Spark Structured Streaming achieve fault tolerance using checkpointing?
**Answer:**  
Spark checkpointing maintains an atomic Write-Ahead Log (WAL) containing:
1. `offsets/<batch_id>`: Records the exact starting and ending Kafka topic-partition offsets before batch execution begins.
2. `commits/<batch_id>`: Written only AFTER the micro-batch has successfully committed all data to the sink.
3. `state/`: Persists intermediate aggregation state for stateful operators.  
If the driver crashes mid-batch, Spark compares the highest offset with the highest commit on restart, detects uncommitted batches, and replays them.

---

### Q13: What is `failOnDataLoss=false`, and why is it configured?
**Answer:**  
In development or fast-retaining Kafka topics, older offsets may be purged by Kafka's log retention policy before Spark reads them. If `failOnDataLoss` is `true`, Spark throws an exception and halts. Setting it to `false` instructs Spark to log a warning and advance to the earliest available offset.

---

### Q14: Why do we set `spark.sql.shuffle.partitions=4` instead of the default 200?
**Answer:**  
The default of 200 shuffle partitions is designed for large multi-node clusters. On a local multi-core machine (or small edge cluster), 200 partitions creates 200 tiny tasks per micro-batch, spending 90% of execution time on JVM thread scheduling and metadata exchange. Setting it to 4 (matching physical CPU cores) reduces scheduling overhead and improves throughput.

---

### Q15: How does an Idempotent Sink guarantee Exactly-Once semantics when micro-batches are replayed?
**Answer:**  
If a crash occurs before `commits/<batch_id>` is written, Spark replays the micro-batch upon restart, sending records to the sink a second time (at-least-once). If the sink is idempotent (e.g., uses SQLite `INSERT OR IGNORE` with unique constraint `uq_transaction(invoice_no, stock_code, invoice_timestamp)`), duplicate records are silently ignored. The combination of at-least-once replay and idempotent writes yields end-to-end **Exactly-Once Semantics**.

---

## Category 3: Machine Learning with Spark MLlib

### Q16: Describe the four stages of the Spark MLlib Pipeline built in Phase 4.
**Answer:**  
1. **`StringIndexer`:** Converts string columns (`Country`, `StockPrefix`) into categorical numerical indices ordered by frequency.
2. **`OneHotEncoder`:** Transforms categorical indices into sparse binary vectors to prevent models from assuming false ordinal relationships.
3. **`VectorAssembler`:** Combines numerical features (`Quantity`, `UnitPrice`, `Hour`, `Month`) and one-hot vectors into a single feature vector column `features`.
4. **Estimator:** Fits a regression algorithm (`LinearRegression` or `RandomForestRegressor`) on the assembled feature vector to predict `revenue`.

---

### Q17: What does `handleInvalid="keep"` do in `StringIndexer` and `OneHotEncoder`?
**Answer:**  
In streaming environments, unseen categories may appear at runtime (e.g., a transaction from a new country). If `handleInvalid` is set to `"error"`, the pipeline crashes. Setting it to `"keep"` assigns unseen categories to a dedicated extra index bucket, allowing streaming inference to continue uninterrupted.

---

### Q18: Why did Linear Regression outperform Random Forest Regressor in this project?
**Answer:**  
The target variable is transaction revenue, defined as:
$$\text{Revenue} = \text{Quantity} \times \text{UnitPrice}$$
Because the relationship is inherently multiplicative/linear with respect to price and volume, a regularized linear model maps directly to the underlying data generation function, achieving:
- Linear Regression: $R^2 = 0.9745$, $\text{RMSE} = 7.63$, inference latency = 0.12 ms.
- Random Forest: $R^2 = 0.9120$, $\text{RMSE} = 18.42$, inference latency = 0.85 ms.  
Random Forest partitions feature space with orthogonal decision splits, struggling to extrapolate continuous linear slopes at extreme values.

---

### Q19: What is ElasticNet regularization in Spark MLlib's Linear Regression?
**Answer:**  
ElasticNet combines $L_1$ (Lasso) and $L_2$ (Ridge) penalties:
$$\text{Penalty} = \lambda \left( \alpha \|\mathbf{w}\|_1 + \frac{1 - \alpha}{2} \|\mathbf{w}\|_2^2 \right)$$
When $\alpha = 1.0$, it is pure Lasso (sparse weights / feature selection); when $\alpha = 0.0$, it is pure Ridge (prevents collinearity blowing up coefficients). Setting $\alpha \in (0, 1)$ balances sparsity and stability.

---

### Q20: Explain the metrics RMSE, MAE, and $R^2$. What do they indicate?
**Answer:**  
- **RMSE (Root Mean Squared Error):** Penalizes large outlier prediction errors quadratically. Expressed in the same units as the target ($7.63).
- **MAE (Mean Absolute Error):** Average magnitude of absolute errors ($4.76). Less sensitive to extreme outliers than RMSE.
- **$R^2$ (Coefficient of Determination):** Proportion of variance in the dependent variable explained by the model ($0.9745 = 97.45\%$ explained variance).

---

### Q21: How is a trained `PipelineModel` saved and loaded for streaming inference?
**Answer:**  
A fitted pipeline is serialized using `fitted_model.save(output_dir)`, which writes stage metadata and model weights to disk. In the streaming application, it is restored via `PipelineModel.load(output_dir)`. The loaded model is then invoked within a streaming micro-batch using `loaded_model.transform(df)`.

---

### Q22: Can Spark MLlib train models directly on streaming DataFrames?
**Answer:**  
Most MLlib estimators require multiple passes over the dataset and cannot `fit()` directly on an unbound streaming DataFrame. The standard enterprise pattern (which we implemented) is:
1. Train and evaluate the model offline on historical / batch data (`data/processed/cleaned`).
2. Serialize the fitted `PipelineModel`.
3. Load the model and call `transform()` for real-time streaming scoring on micro-batches.

---

## Category 4: Databases, Storage Concurrency & SQLite WAL

### Q23: Why did you use Write-Ahead Logging (WAL) mode for SQLite instead of the default rollback journal?
**Answer:**  
In SQLite's default rollback journal mode, writing locks the entire database exclusively, blocking all read queries until the write completes. In WAL mode:
- Changes are appended to a separate `-wal` file.
- Readers read from a snapshot of the database while writers append to the WAL concurrently.
- Readers never block writers, and writers never block readers, enabling high-concurrency stream ingestion.

---

### Q24: What does `PRAGMA synchronous = NORMAL;` do, and why is it safe in WAL mode?
**Answer:**  
In default `FULL` synchronous mode, the operating system syncs data to physical disk on every commit, limiting throughput to ~250 transactions/second. In WAL mode, `NORMAL` only syncs during WAL checkpoints. Transactions remain atomic and crash-safe across application crashes, boosting write IOPS to $>30,000$ rows/second.

---

### Q25: What is `PRAGMA busy_timeout = 30000;` and why is it necessary in a connection pool?
**Answer:**  
Even in WAL mode, SQLite allows only one writer at a time. If two connection pool threads attempt to commit simultaneously, SQLite immediately raises `sqlite3.OperationalError: database is locked` unless a timeout is set. `busy_timeout = 30000` instructs SQLite to sleep and retry internally for up to 30 seconds before raising an error, eliminating lock collision exceptions.

---

### Q26: What happens during `PRAGMA wal_checkpoint(TRUNCATE);`?
**Answer:**  
A WAL checkpoint copies committed frames from the `-wal` file back into the primary `.db` file. The `TRUNCATE` variant additionally locks the WAL and truncates its file length to 0 bytes, reclaiming disk space without interrupting subsequent writes.

---

### Q27: How does `PRAGMA integrity_check` verify database health?
**Answer:**  
It traverses all B-Trees, page checksums, freelists, and index pointers in the SQLite file. It verifies that:
- Every table row has a corresponding index entry.
- B-Tree page headers are valid and uncorrupted.
- No pages are leaked or missing from freelists.  
It returns `"ok"` when the database is 100% structurally healthy.

---

### Q28: How does connection pooling improve SQLite performance?
**Answer:**  
Creating and tearing down SQLite connection handles repeatedly incurs file descriptor overhead and resets PRAGMA caches. Our `SQLiteConnectionPool` pre-allocates 5 tuned connections with 64 MB memory cache (`PRAGMA cache_size = -64000`) and yields them through a thread-safe context manager.

---

## Category 5: Fault-Tolerance, Testing & Performance Benchmarks

### Q29: What happens when a transaction crashes mid-way before `COMMIT`?
**Answer:**  
SQLite follows ACID atomicity. If a process terminates or connection closes before `COMMIT`, the uncommitted frames in the WAL file are marked invalid. On reconnect, the database state reflects only previously committed transactions. In our Phase 7 test, **0 dirty rows** leaked into the table.

---

### Q30: How did you test Spark Structured Streaming driver crashes and recovery?
**Answer:**  
We simulated a micro-batch crash where records for batch 2 (offsets 250 $\to$ 400) were sent to the sink and logged in `offsets/2`, but the process was terminated before `commits/2` was written. On restart, the recovery inspector detected the missing commit, re-queried Kafka for offsets 250 $\to$ 400, re-executed the sink write (where unique constraints prevented **150 duplicates**), and successfully committed batch 2.

---

### Q31: What are latency percentiles ($p_{50}, p_{95}, p_{99}$), and why is average latency misleading?
**Answer:**  
Average latency can be distorted by outliers and fails to capture user experience:
- $p_{50}$ (Median): 50% of requests executed faster than this value.
- $p_{95}$: 95% of requests executed faster than this value.
- $p_{99}$: The slowest 1% tail latency.  
In distributed systems, tail latency ($p_{99}$) reveals garbage collection pauses, disk sync stalls, and network packet drops that averages conceal.

---

### Q32: What throughput numbers did the system achieve during Phase 7 benchmarks?
**Answer:**  
- **Producer Ingestion Rate:** 7,197.2 messages/second ($p_{50} = 0.53\text{ ms}$, $p_{99} = 0.71\text{ ms}$).
- **CSV Parsing & Sanitization:** 13,322.1 rows/second.
- **In-Memory Analytical Aggregations:** 146,310.0 records/second.
- **SQLite WAL Bulk Ingestion:** 30,843.2 rows/second.
- **Multi-Threaded Lock Contention:** 1,056.7 transactions/second with 0 errors across 8 threads.

---

### Q33: How does the platform handle Windows-specific Hadoop dependencies?
**Answer:**  
On Windows, PySpark requires Hadoop native binaries (`winutils.exe` and `hadoop.dll`) to interact with the local filesystem without throwing `UnsatisfiedLinkError: NativeIO$Windows.access0`. The codebase dynamically detects `C:\hadoop\bin` and prepends it to `os.environ["PATH"]` prior to JVM instantiation.

---

### Q34: What is the purpose of Grafana automated provisioning?
**Answer:**  
Rather than requiring manual UI clicking to create datasources and dashboards, Grafana automated provisioning uses YAML configuration files (`dashboards/provisioning/`) to automatically mount SQLite datasources and load JSON dashboard definitions on startup.

---

### Q35: What would be the next architectural step if the dataset scaled to 1 billion transactions per day?
**Answer:**  
1. **Multi-Node Kafka Cluster:** Scale to 12+ partitions across 3 brokers with replication factor 3.
2. **Distributed Spark Cluster:** Run Spark Structured Streaming on Kubernetes (EKS/GKE) with autoscaling worker pods.
3. **Storage Migration:** Replace SQLite with a distributed OLAP database like **ClickHouse** or **Apache Pinot**, paired with **Delta Lake / Apache Iceberg** on Amazon S3 or Google Cloud Storage for historical cold storage.
