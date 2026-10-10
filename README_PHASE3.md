# Phase 3 — Spark Structured Streaming Consumer

> **Phase 3 deliverable:** A Spark Structured Streaming pipeline that consumes JSON
> messages from Kafka, parses them into a typed DataFrame, cleans and enriches the
> data, computes windowed aggregations, and writes cleaned records to Parquet.
> **ML is NOT implemented** (Phase 4).

---

## 1. Architecture

```
┌─────────────┐     JSON      ┌───────────────────────────────────────┐
│   Kafka     │ ────────────▶ │   Spark Structured Streaming (Phase 3) │
│  topic:     │   2560 msgs   │                                       │
│  ecommerce- │               │  1. Read Kafka stream                 │
│  transactions              │  2. from_json → typed DataFrame        │
│             │               │  3. Drop nulls, cancellations, bad qty │
│             │               │  4. Cast types, parse InvoiceDate     │
│             │               │  5. Feature: Revenue = Qty × Price    │
│             │               │  6. Windowed aggregations             │
│             │               │  7. Write cleaned → Parquet           │
└─────────────┘               └───────────────┬───────────────────────┘
                                             │ Parquet
                                             ▼
                                  data/processed/cleaned/
```

### Output modes used

| Query | Output mode | Why |
|-------|-------------|-----|
| `cleaned_records` (console) | `append` | Raw rows are immutable once emitted |
| `cleaned_sink` (Parquet) | `append` | File sink only supports append |
| `order_metrics` | `update` | Tumbling-window state updates as data arrives |
| `rolling_metrics` | `update` | Sliding-window state updates |
| `popular_products` | `complete` | `orderBy().limit()` requires complete mode |
| `top_customers` | `complete` | `orderBy().limit()` requires complete mode |
| `malformed_records` | `append` | One-shot corrupt-row notifications |

---

## 2. Files created in Phase 3

| File | Purpose |
|------|---------|
| `src/consumer/__init__.py` | Package marker |
| `src/consumer/config.py` | `StreamingConfig` dataclass — loads `.env`, supports CLI overrides |
| `src/consumer/schema.py` | `ecommerce_schema()` — `StructType` for JSON parsing (incl. `_corrupt_record`) |
| `src/consumer/spark_streaming.py` | Main consumer: Kafka source, parse, clean, feature-engineer, windows, sinks |
| `.env` (updated) | Added `SPARK_*`, `*_WINDOW_SECONDS`, `WATERMARK_SECONDS`, `CHECKPOINT_DIR`, `PROCESSED_DIR` |

---

## 3. Line-by-line explanation — `spark_streaming.py`

### 3.1 Imports and setup

```python
import argparse, logging, sys, time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.consumer.config import StreamingConfig
from src.consumer.schema import ecommerce_schema
```

- `argparse` — CLI flags (`--max-runtime-seconds`, `--trigger-seconds`, …).
- `logging` — structured logs with timestamps and level.
- `sys, time, Path` — path setup + runtime loop.
- `SparkSession, functions as F` — Spark entry point and DSL functions.
- `DoubleType, IntegerType, TimestampType` — explicit casts so downstream aggregations work.
- `sys.path.insert(...)` — makes `src` importable when run as `-m src.consumer.spark_streaming`.
- Import `StreamingConfig` (env loader) and `ecommerce_schema` (StructType).

### 3.2 `create_spark_session`

```python
spark = (SparkSession.builder
    .master(config.master)                          # local[*]
    .appName(config.app_name)                       # EcommerceStreaming
    .config("spark.jars.packages",                  # auto-download Kafka connector
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config("spark.sql.streaming.checkpointLocation", config.checkpoint_dir)
    .config("spark.sql.shuffle.partitions", "4")    # local perf
    .config("spark.driver.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true")  # Netty on JDK 17
    .config("spark.sql.adaptive.enabled", "false")  # stable streaming plans
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

- `spark.jars.packages` — Ivy fetches `spark-sql-kafka-0-10` + `kafka-clients` at startup.
- `checkpointLocation` — default checkpoint root; each query overrides with a sub-dir.
- `shuffle.partitions=4` — avoids 200 partitions (default) on a laptop.
- `io.netty.tryReflectionSetAccessible=true` — fixes Netty reflective access warnings on JDK 17.
- `adaptive.enabled=false` — AQE can destabilize streaming plans; keep deterministic.

### 3.3 `read_kafka_stream`

```python
return (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", config.bootstrap_servers)   # localhost:9092
    .option("subscribe", config.topic)                             # ecommerce-transactions
    .option("startingOffsets", config.starting_offsets)            # earliest
    .option("failOnDataLoss", "false")        # tolerate topic re-creation
    .option("maxOffsetsPerTrigger", 5000)     # rate-limit per micro-batch
    .load())
```

Returns a streaming DataFrame with columns: `key, value, topic, partition, offset, timestamp, timestampType`.

### 3.4 `parse_and_clean`

```python
parsed = raw_df.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.from_json(
        F.col("value").cast("string"),
        schema,
        options={"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
    ).alias("data"),
)
```

- `from_json` with `PERMISSIVE` mode — malformed JSON is captured in `_corrupt_record` instead of crashing.
- `_corrupt_record` must be declared in the schema (`schema.py`) for this to work.

```python
exploded = parsed.select("kafka_key", ..., "data.InvoiceNo", "data.StockCode", ...,
                         F.col("data._corrupt_record").alias("_corrupt_record"))
```

Flattens the `data` struct into individual columns.

```python
malformed = exploded.filter(F.col("_corrupt_record").isNotNull())

cleaned = (exploded
    .filter(F.col("_corrupt_record").isNull())
    .drop("_corrupt_record")
    .filter(F.col("InvoiceNo").isNotNull() & ... & F.col("Country").isNotNull())
    .filter(~F.col("InvoiceNo").startswith("C"))          # drop cancellations
    .filter(F.col("Quantity") > 0)                         # drop returns
    .filter(F.col("UnitPrice") > 0)                        # drop free/negative
    .withColumn("Quantity", F.col("Quantity").cast(IntegerType()))
    .withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
    .withColumn("InvoiceTimestamp",
                F.to_timestamp(F.col("InvoiceDate"), "yyyy/M/d H:mm"))
    .withColumn("Revenue",
                F.round(F.col("Quantity") * F.col("UnitPrice"), 2)))
```

Cleaning steps:
1. Drop corrupt JSON rows.
2. Drop any row with a null in the 8 key columns.
3. Remove cancellation invoices (`C…`).
4. Remove non-positive `Quantity` (returns) and `UnitPrice`.
5. Cast `Quantity` → int, `UnitPrice` → double.
6. Parse `InvoiceDate` ("2010/12/1 8:26") → `InvoiceTimestamp` (timestamp) using format `yyyy/M/d H:mm`.
7. **Feature engineering:** `Revenue = Quantity × UnitPrice`, rounded to 2 dp.

### 3.5 `build_queries`

```python
cleaned_df = cleaned_df.withWatermark("InvoiceTimestamp", f"{config.watermark_seconds} seconds")
```

Watermark on event time — allows state to be dropped for old windows. Set to 1 s by default so windows emit quickly during short test runs.

**Cleaned sink (Parquet):**
```python
cleaned_sink_query = (cleaned_df.writeStream
    .outputMode("append").format("parquet")
    .option("path", f"{config.processed_dir}/cleaned")
    .option("checkpointLocation", f"{config.checkpoint_dir}/cleaned_sink")
    .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
    .start())
```

**Order metrics (tumbling window, 60 s):**
```python
order_metrics = (cleaned_df
    .groupBy(F.window("InvoiceTimestamp", "60 seconds"))
    .agg(
        F.approx_count_distinct("InvoiceNo").alias("orders_count"),     # streaming-safe distinct
        F.count("*").alias("line_items_count"),
        F.sum("Revenue").alias("total_revenue"),
        F.round(F.sum("Revenue") / F.approx_count_distinct("InvoiceNo"), 2).alias("avg_order_value"),
    )
    .select(F.col("window.start").alias("window_start"), ...))
```

> `approx_count_distinct` replaces `countDistinct` because Spark Structured Streaming
> does **not** support exact distinct aggregations (would require unbounded state).

**Rolling metrics (sliding window, 300 s width / 60 s slide):**
```python
F.window("InvoiceTimestamp", "300 seconds", "60 seconds")
```
Computes `rolling_orders`, `rolling_revenue`, `rolling_avg_line_revenue`, `rolling_avg_order_value`.

**Popular products & top customers** use `.orderBy(...).limit(10)` → `complete` output mode.

### 3.6 `main` runtime loop

```python
queries = build_queries(cleaned_df, malformed_df, config)
if max_runtime > 0:
    end_time = time.time() + max_runtime
    while time.time() < end_time:
        time.sleep(2)
        if not any(q.isActive for q in queries): break
    for q in queries:
        if q.isActive: q.stop()
else:
    spark.streams.awaitAnyTermination()
```

- `--max-runtime-seconds` (default 60) stops all queries after N seconds — useful for tests.
- `0` runs forever until Ctrl+C.

---

## 4. Schema — `schema.py`

```python
StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode",  StringType(), True),
    StructField("Description",StringType(), True),
    StructField("Quantity",   IntegerType(), True),
    StructField("InvoiceDate",StringType(), True),
    StructField("UnitPrice",  StringType(), True),   # string first → cast later
    StructField("CustomerID", StringType(), True),
    StructField("Country",    StringType(), True),
    StructField("_corrupt_record", StringType(), True),  # REQUIRED for PERMISSIVE mode
])
```

`UnitPrice` is read as string then cast to double, because the CSV/JSON may carry non-numeric tokens; catching that at cast time is cleaner than a parse failure.

---

## 5. Config — `config.py`

`StreamingConfig` dataclass reads all settings from `.env` with sensible defaults. `from_overrides(...)` lets CLI flags take precedence. Key fields:

| Field | Default | Meaning |
|-------|---------|---------|
| `bootstrap_servers` | `localhost:9092` | Kafka broker |
| `topic` | `ecommerce-transactions` | Kafka topic |
| `checkpoint_dir` | `data/checkpoint` | Fault-tolerance state |
| `processed_dir` | `data/processed` | Parquet output root |
| `starting_offsets` | `earliest` | Replay from the beginning |
| `trigger_interval_seconds` | 10 | Micro-batch cadence |
| `watermark_seconds` | 1 | Event-time watermark |
| `tumbling_window_seconds` | 60 | Order-metrics window |
| `sliding_window_seconds` / `sliding_slide_seconds` | 300 / 60 | Rolling-metrics window |

---

## 6. Fault tolerance & checkpointing

- Every query has its own `checkpointLocation` under `data/checkpoint/<query>/`.
- On restart, Spark resumes from the last committed Kafka offset for each query.
- `failOnDataLoss=false` prevents a crash if the topic is recreated/offset out of range.
- Parquet sink is idempotent with `append` — reprocessed rows are deduplicated by checkpoint.

---

## 7. Test results

### 7.1 Producer sent 2,000 messages (560 existing → 2,560 total)

```
2026-10-11 01:39:00 | INFO | ecommerce-producer | Producer closed. Sent=2000 | Failed=0
```

### 7.2 Streaming consumer (80 s, trigger 5 s, watermark 1 s)

Exit code **0**. All 7 queries started.

### 7.3 Cleaned Parquet sink — verified

```
Schema: InvoiceTimestamp: timestamp, Revenue: double, Quantity: int, UnitPrice: double
Row count: 1684  (out of 2560 raw → cancellations/nulls/bad values removed)
Sample:
  InvoiceNo=536365 | StockCode=85123A | Qty=6 | Price=2.55 | Revenue=15.30 | 2010-12-01 08:26:00
Aggregate: sum(Quantity)=18,456 | sum(Revenue)=£47,197.01
```

### 7.4 Rolling metrics (sliding window) sample output

```
window_start         |window_end          |rolling_orders|rolling_revenue|rolling_avg_order_value
2010-12-01 09:40:00 |2010-12-01 09:45:00 |1             |599.6          |599.6
2010-12-01 09:44:00 |2010-12-01 09:49:00 |1             |840.8          |840.8
2010-12-01 09:53:00 |2010-12-01 09:58:00 |3             |2194.9         |731.63
2010-12-01 10:37:00 |2010-12-01 10:42:00 |2             |2188.4         |1094.2
```

### 7.5 Top customers sample output

```
window                                     |CustomerID|customer_orders|customer_revenue
{2010-12-01 09:58:00, 2010-12-01 09:59:00}|16029     |1              |6387.84
{2010-12-01 10:19:00, 2010-12-01 10:20:00}|17511     |1              |2727.00
{2010-12-01 08:45:00, 2010-12-01 08:46:00}|12583     |1              |2186.10
...
```

### 7.6 Checkpoint directories created

`data/checkpoint/{cleaned, cleaned_sink, malformed, order_metrics, popular_products, rolling_metrics, top_customers}` — all 7 present, confirming fault-tolerance state was persisted.

---

## 8. How to run

```powershell
# 1. Start Kafka (KRaft mode) — set heap opts to avoid wmic issue:
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
kafka-server-start.bat C:\kafka\config\kraft\server.properties

# 2. (Optional) Produce some data:
python -m src.producer.kafka_producer --max-messages 2000 --delay 0.005

# 3. Run the streaming consumer:
python -m src.consumer.spark_streaming --max-runtime-seconds 80 --trigger-seconds 5 --watermark-seconds 1
```

To run indefinitely: `--max-runtime-seconds 0`.

---

## 9. Known Windows-specific notes

- `C:\hadoop\bin` **must** be on `PATH` for the Hadoop native library (`hadoop.dll`), otherwise the Parquet/Checkpoint `FileContext` throws `UnsatisfiedLinkError: NativeIO$Windows.access0`.
- `KAFKA_HEAP_OPTS` must be set before `kafka-server-start.bat` because the script tries to call the removed `wmic` tool.
- `spark.jars.packages` downloads `spark-sql-kafka-0-10_2.12:3.5.3` on first run (~11 jars cached in `~/.ivy2`).

---

## 10. What's next (Phase 4 — NOT done here)

- Spark MLlib: customer segmentation (RFM → KMeans), churn prediction, revenue forecasting.
- SQLite sink for cleaned + aggregated data.
- Grafana dashboards on SQLite.

**Phase 3 complete. No ML implemented. Stopping here.**
