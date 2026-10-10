import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# Ensure project root in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.testing.benchmark_large_dataset import LargeDatasetBenchmark
from src.testing.metrics_logger import PerformanceCollector
from src.testing.stress_producer import StressTestProducer
from src.testing.test_database_recovery import DatabaseRecoveryTester
from src.testing.test_kafka_failures import (
    CircuitBreaker,
    CircuitState,
    DeadLetterQueue,
    KafkaFailureResilienceTester,
)
from src.testing.test_spark_recovery import SparkRecoverySimulator


class TestPhase7TestingAndBenchmarking(unittest.TestCase):
    """
    Comprehensive test suite validating Phase 7 testing harness,
    stress testing, fault tolerance, recovery, and performance metrics.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_phase7_")

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_metrics_logger_and_telemetry(self):
        """Test PerformanceCollector latency percentiles and throughput math."""
        collector = PerformanceCollector("UnitTest_Metrics").start()
        # Record 100 sample latencies (1ms to 100ms)
        for i in range(1, 101):
            collector.record_operation(float(i), byte_count=100)

        summary = collector.stop()
        self.assertEqual(summary.total_operations, 100)
        self.assertEqual(summary.total_bytes, 10000)
        self.assertEqual(summary.error_count, 0)
        self.assertAlmostEqual(summary.p50_latency_ms, 51.0, places=1)
        self.assertAlmostEqual(summary.p95_latency_ms, 96.0, places=1)
        self.assertAlmostEqual(summary.p99_latency_ms, 100.0, places=1)
        self.assertGreater(summary.throughput_ops_sec, 0.0)

    def test_stress_producer_mock_execution(self):
        """Test multi-threaded StressTestProducer throughput and latency percentiles."""
        producer = StressTestProducer(mode="mock", num_threads=2, batch_size=100)
        summary = producer.run_stress_test(total_messages=1000)

        self.assertEqual(summary.total_operations, 1000)
        self.assertEqual(summary.error_count, 0)
        self.assertGreater(summary.throughput_ops_sec, 1000.0)
        self.assertGreater(summary.p50_latency_ms, 0.0)
        self.assertEqual(summary.extra_metadata["threads"], 2)
        self.assertEqual(summary.extra_metadata["mode"], "mock")

    def test_large_dataset_parsing_and_aggregation(self):
        """Test streaming CSV parser, feature aggregations, and bulk SQLite ingestion."""
        benchmark = LargeDatasetBenchmark(batch_size=500)
        results = benchmark.run_all(max_rows=1500)

        self.assertIn("csv_parsing", results)
        self.assertIn("aggregations", results)
        self.assertIn("sqlite_ingestion", results)

        parse_sum = results["csv_parsing"]
        agg_sum = results["aggregations"]
        db_sum = results["sqlite_ingestion"]

        self.assertGreater(parse_sum.total_operations, 1000)
        self.assertEqual(agg_sum.total_operations, parse_sum.total_operations)
        self.assertEqual(db_sum.total_operations, parse_sum.total_operations)
        self.assertGreater(agg_sum.extra_metadata["unique_countries"], 0)
        self.assertGreater(db_sum.extra_metadata["batches_executed"], 0)

    def test_kafka_failure_resilience_circuit_breaker(self):
        """Test circuit breaker tripping, DLQ spillover, and broker healing."""
        dlq_dir = os.path.join(self.temp_dir, "dlq")
        tester = KafkaFailureResilienceTester(dlq_dir=dlq_dir)

        # 1. Test offline broker scenario
        off_res = tester.test_offline_broker_scenario(count=20)
        self.assertEqual(off_res["delivered"], 0)
        self.assertEqual(off_res["circuit_state"], "OPEN")
        self.assertEqual(tester.dlq.count(), 20)

        # 2. Test self-healing
        heal_res = tester.test_broker_recovery_and_healing()
        self.assertTrue(heal_res["probe_success"])
        self.assertEqual(heal_res["final_circuit_state"], "CLOSED")
        self.assertEqual(heal_res["post_recovery_delivered"], 20)

    def test_spark_recovery_uncommitted_batch_replay(self):
        """Test Spark Structured Streaming checkpoint crash detection and recovery."""
        spark_sim = SparkRecoverySimulator(temp_dir=self.temp_dir)
        res = spark_sim.run_recovery_test()

        self.assertEqual(res["status"], "PASSED")
        self.assertEqual(res["crash_batch_id"], 2)
        self.assertEqual(res["recovered_batch_id"], 2)
        self.assertEqual(res["duplicates_prevented"], 150)
        self.assertEqual(res["final_unique_sink_records"], 600)
        self.assertTrue(res["checkpoint_integrity_verified"])

    def test_sqlite_wal_recovery_and_rollback(self):
        """Test SQLite uncommitted transaction rollback, WAL truncation, and integrity."""
        db_tester = DatabaseRecoveryTester()
        try:
            # 1. Rollback test
            r1 = db_tester.test_uncommitted_rollback_on_crash()
            self.assertTrue(r1["rollback_verified"])
            self.assertEqual(r1["dirty_rows_detected"], 0)

            # 2. WAL Checkpoint truncate
            r2 = db_tester.test_wal_checkpoint_and_truncation(insert_count=500)
            self.assertTrue(r2["records_intact"])
            self.assertEqual(r2["wal_size_after_bytes"], 0)

            # 3. Integrity check
            r3 = db_tester.test_integrity_check()
            self.assertEqual(r3["integrity_check"], "ok")
            self.assertEqual(r3["quick_check"], "ok")
        finally:
            db_tester.cleanup()

    def test_sqlite_concurrent_write_contention(self):
        """Test multi-threaded concurrent write lock resilience without database lock errors."""
        db_tester = DatabaseRecoveryTester()
        try:
            r4 = db_tester.test_concurrent_write_contention(num_threads=6, writes_per_thread=25)
            self.assertEqual(r4["lock_errors"], 0)
            self.assertEqual(r4["successful_transactions"], 150)
            self.assertGreater(r4["throughput_tx_sec"], 100.0)
        finally:
            db_tester.cleanup()


if __name__ == "__main__":
    unittest.main()
