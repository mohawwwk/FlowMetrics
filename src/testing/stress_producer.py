import argparse
import csv
import json
import logging
import queue
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.producer.config import ProducerConfig
from src.testing.metrics_logger import (
    MetricSummary,
    PerformanceCollector,
    configure_structured_logging,
)

logger = configure_structured_logging("stress-producer")


class MockKafkaBroker:
    """
    High-fidelity simulated Kafka broker for isolated stress-testing and CI environments.
    Simulates partition queues, network socket latency (0.1ms - 2ms), buffer capacity,
    and concurrent thread contention without requiring external JVM infrastructure.
    """

    def __init__(self, max_buffer_size: int = 100000, latency_jitter_ms: float = 0.2):
        self.max_buffer_size = max_buffer_size
        self.latency_jitter_ms = latency_jitter_ms
        self.partitions: Dict[int, queue.Queue] = {p: queue.Queue(maxsize=max_buffer_size) for p in range(4)}
        self.total_received = 0
        self.total_bytes = 0
        self._lock = threading.Lock()
        self.buffer_exhausted_count = 0

    def send(self, topic: str, partition: int, key: Optional[bytes], value: bytes) -> Tuple[int, int]:
        """Simulates Kafka send with network roundtrip and partition queue insertion."""
        part_id = partition % 4
        q = self.partitions[part_id]

        # Simulate microsecond wire latency and lock contention
        if self.latency_jitter_ms > 0:
            time.sleep(random.uniform(0.00005, self.latency_jitter_ms / 1000.0))

        try:
            q.put_nowait((key, value))
            with self._lock:
                offset = self.total_received
                self.total_received += 1
                self.total_bytes += len(value)
            return part_id, offset
        except queue.Full:
            with self._lock:
                self.buffer_exhausted_count += 1
            raise BufferError("Kafka producer buffer memory exhausted (mock partition full)")


class StressTestProducer:
    """
    Multi-threaded, high-concurrency stress testing producer harness.
    Evaluates ingestion throughput, queue contention, p50/p95/p99 delivery latencies,
    and buffer saturation under heavy workload.
    """

    def __init__(
        self,
        config: Optional[ProducerConfig] = None,
        csv_path: Optional[str] = None,
        mode: str = "mock",
        num_threads: int = 4,
        batch_size: int = 500,
    ):
        self.config = config or ProducerConfig()
        self.csv_path = csv_path or self.config.csv_file_path
        self.mode = mode
        self.num_threads = max(1, num_threads)
        self.batch_size = max(1, batch_size)
        self.collector = PerformanceCollector(f"StressProducer_{mode}_{num_threads}threads")
        self.mock_broker = MockKafkaBroker() if mode == "mock" else None
        self.live_producer = None
        self.cached_records: List[str] = []

    def _preload_dataset(self, max_records: int = 50000) -> None:
        """Pre-loads CSV records into memory to isolate Kafka ingestion throughput from disk bottlenecks."""
        path = Path(self.csv_path)
        if not path.exists():
            # Fallback synthetic generation if raw CSV missing
            logger.warning("Dataset not found at %s. Generating synthetic transaction records.", path)
            self.cached_records = [
                json.dumps({
                    "InvoiceNo": str(100000 + i),
                    "StockCode": f"PROD_{i % 500}",
                    "Description": f"Test Product {i % 500}",
                    "Quantity": random.randint(1, 20),
                    "InvoiceDate": "2026-10-11 02:00:00",
                    "UnitPrice": round(random.uniform(1.5, 45.0), 2),
                    "CustomerID": str(12000 + (i % 200)),
                    "Country": "United Kingdom",
                })
                for i in range(max_records)
            ]
            return

        logger.info("Pre-loading up to %d records from %s into memory...", max_records, path)
        loaded = 0
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                payload = {
                    "InvoiceNo": row.get("InvoiceNo", ""),
                    "StockCode": row.get("StockCode", ""),
                    "Description": row.get("Description", ""),
                    "Quantity": int(row["Quantity"]) if row.get("Quantity") not in (None, "") else 1,
                    "InvoiceDate": row.get("InvoiceDate", ""),
                    "UnitPrice": float(row["UnitPrice"]) if row.get("UnitPrice") not in (None, "") else 0.0,
                    "CustomerID": row.get("CustomerID", "") if row.get("CustomerID") not in (None, "") else None,
                    "Country": row.get("Country", "United Kingdom"),
                }
                self.cached_records.append(json.dumps(payload, ensure_ascii=False))
                loaded += 1
                if loaded >= max_records:
                    break
        logger.info("Pre-loaded %d records successfully.", len(self.cached_records))

    def _init_live_producer(self) -> None:
        from kafka import KafkaProducer
        self.live_producer = KafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            client_id="stress-tester",
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            acks=1,  # Tuned for high-throughput stress
            retries=3,
            linger_ms=5,
            batch_size=65536,
            buffer_memory=67108864,  # 64 MB
        )

    def _worker_loop(self, thread_idx: int, records_chunk: List[str]) -> List[Tuple[float, int, bool]]:
        """Worker thread processing a subset of messages with per-message latency logging."""
        results: List[Tuple[float, int, bool]] = []
        is_live = self.mode == "live" and self.live_producer is not None

        for record_str in records_chunk:
            payload_bytes = record_str.encode("utf-8")
            byte_size = len(payload_bytes)
            t0 = time.perf_counter()
            err = False

            try:
                if is_live:
                    future = self.live_producer.send(
                        self.config.topic,
                        key=f"worker-{thread_idx}",
                        value=record_str,
                    )
                    # For stress latency testing, sample asynchronous callbacks
                    future.get(timeout=2.0)
                else:
                    partition = thread_idx % 4
                    self.mock_broker.send(
                        topic=self.config.topic,
                        partition=partition,
                        key=f"worker-{thread_idx}".encode("utf-8"),
                        value=payload_bytes,
                    )
            except Exception as e:
                err = True
            finally:
                t1 = time.perf_counter()
                latency_ms = (t1 - t0) * 1000.0
                results.append((latency_ms, byte_size, err))

        return results

    def run_stress_test(self, total_messages: int = 20000) -> MetricSummary:
        """
        Executes multi-threaded high-throughput stress test.
        Distributes total_messages across thread workers and computes aggregate metrics.
        """
        if not self.cached_records:
            self._preload_dataset(min(total_messages, 50000))

        if self.mode == "live":
            try:
                self._init_live_producer()
                logger.info("Connected to live Kafka cluster at %s", self.config.bootstrap_servers)
            except Exception as e:
                logger.warning("Live Kafka connection failed: %s. Falling back to high-fidelity mock broker.", e)
                self.mode = "mock"
                self.mock_broker = MockKafkaBroker()

        logger.info(
            "Starting Stress Test: Total=%d msgs, Threads=%d, BatchSize=%d, Mode=%s",
            total_messages, self.num_threads, self.batch_size, self.mode
        )

        # Distribute messages across workers
        chunk_size = total_messages // self.num_threads
        worker_chunks: List[List[str]] = []
        num_cached = len(self.cached_records)

        for i in range(self.num_threads):
            start_idx = (i * chunk_size) % num_cached
            count = chunk_size if i < self.num_threads - 1 else (total_messages - i * chunk_size)
            chunk = [self.cached_records[(start_idx + j) % num_cached] for j in range(count)]
            worker_chunks.append(chunk)

        self.collector.start()

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_worker = {
                executor.submit(self._worker_loop, i, worker_chunks[i]): i
                for i in range(self.num_threads)
            }

            for future in as_completed(future_to_worker):
                worker_id = future_to_worker[future]
                worker_results = future.result()
                for lat_ms, bytes_cnt, is_err in worker_results:
                    self.collector.record_operation(lat_ms, bytes_cnt, is_err)

        if self.live_producer:
            self.live_producer.flush()
            self.live_producer.close()

        summary = self.collector.stop()
        summary.extra_metadata["threads"] = self.num_threads
        summary.extra_metadata["batch_size"] = self.batch_size
        summary.extra_metadata["mode"] = self.mode
        if self.mock_broker:
            summary.extra_metadata["buffer_exhausted_events"] = self.mock_broker.buffer_exhausted_count

        logger.info(
            "Stress Test Completed: %.1f msgs/sec | %.2f MB/sec | p50=%.2fms | p95=%.2fms | p99=%.2fms | Memory Delta=%.2fMB",
            summary.throughput_ops_sec,
            summary.throughput_mb_sec,
            summary.p50_latency_ms,
            summary.p95_latency_ms,
            summary.p99_latency_ms,
            summary.memory_delta_mb,
        )
        return summary


def main():
    parser = argparse.ArgumentParser(description="Stress Test Kafka Producer Harness")
    parser.add_argument("--messages", type=int, default=20000, help="Total messages to produce")
    parser.add_argument("--threads", type=int, default=4, help="Number of concurrent producer threads")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size")
    parser.add_argument("--mode", choices=["mock", "live"], default="mock", help="Execution mode")
    args = parser.parse_args()

    producer = StressTestProducer(
        mode=args.mode,
        num_threads=args.threads,
        batch_size=args.batch_size,
    )
    summary = producer.run_stress_test(total_messages=args.messages)
    print("\n--- STRESS TEST RESULT ---")
    print(json.dumps(summary.to_dict(), indent=2))


if __name__ == "__main__":
    main()
