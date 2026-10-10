import argparse
import json
import logging
import os
import random
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.producer.config import ProducerConfig
from src.testing.metrics_logger import (
    MetricSummary,
    PerformanceCollector,
    configure_structured_logging,
)

logger = configure_structured_logging("kafka-failures")


class CircuitState(str, Enum):
    CLOSED = "CLOSED"        # Normal operations: requests flow through
    OPEN = "OPEN"            # Tripped: fail fast without attempting network calls
    HALF_OPEN = "HALF_OPEN"  # Testing: send single canary request to test recovery


class CircuitBreaker:
    """
    Production-grade Circuit Breaker pattern implementation for Kafka producers.
    Prevents thread starvation and memory leaks during broker outages.
    """

    def __init__(self, failure_threshold: int = 3, recovery_timeout_sec: float = 1.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout_sec = recovery_timeout_sec
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.last_state_change = time.time()
        self.total_tripped_count = 0

    def can_execute(self) -> bool:
        now = time.time()
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if now - self.last_state_change >= self.recovery_timeout_sec:
                self.state = CircuitState.HALF_OPEN
                self.last_state_change = now
                logger.info("CircuitBreaker transitioned to HALF_OPEN (probing broker health)...")
                return True
            return False
        elif self.state == CircuitState.HALF_OPEN:
            return True
        return False

    def record_success(self) -> None:
        if self.state in (CircuitState.HALF_OPEN, CircuitState.OPEN):
            logger.info("CircuitBreaker probe succeeded. Transitioning to CLOSED (broker healthy).")
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0

    def record_failure(self) -> None:
        self.consecutive_failures += 1
        if self.state == CircuitState.HALF_OPEN or self.consecutive_failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self.last_state_change = time.time()
            self.total_tripped_count += 1
            logger.warning(
                "CircuitBreaker TRIPPED to OPEN after %d consecutive failures. Failing fast for %.1fs.",
                self.consecutive_failures,
                self.recovery_timeout_sec,
            )


class DeadLetterQueue:
    """
    Local disk spillover dead-letter queue (DLQ).
    Stores un-deliverable payloads when broker is down, ensuring zero data loss.
    """

    def __init__(self, dlq_dir: Optional[str] = None):
        self.dlq_dir = Path(dlq_dir or "data/dlq")
        self.dlq_dir.mkdir(parents=True, exist_ok=True)
        self.spill_file = self.dlq_dir / "failed_events.jsonl"
        self.spill_count = 0

    def spill(self, key: str, value: str, error_reason: str) -> None:
        payload = {
            "timestamp": time.time(),
            "key": key,
            "value": value,
            "error_reason": error_reason,
        }
        with open(self.spill_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
        self.spill_count += 1

    def count(self) -> int:
        if not self.spill_file.exists():
            return 0
        with open(self.spill_file, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)


class SimulatedFaultyBroker:
    """
    Simulated broker supporting dynamic fault injection:
    - total blackout (simulate offline broker)
    - flaky network with packet loss probability
    - socket timeouts
    - partition leader elections
    """

    def __init__(self):
        self.is_online = True
        self.failure_rate = 0.0  # 0.0 to 1.0 probability of drop
        self.socket_timeout_sec = 0.05
        self.total_attempts = 0
        self.total_failures = 0
        self.total_successes = 0

    def send(self, topic: str, key: str, value: str) -> Tuple[int, int]:
        self.total_attempts += 1
        if not self.is_online:
            self.total_failures += 1
            raise ConnectionRefusedError(f"Broker connection refused: offline or unreachable")

        if self.failure_rate > 0.0 and random.random() < self.failure_rate:
            self.total_failures += 1
            time.sleep(self.socket_timeout_sec)
            raise TimeoutError("Kafka socket timed out waiting for ACK (simulated network partition)")

        self.total_successes += 1
        return 0, self.total_successes


class KafkaFailureResilienceTester:
    """
    Tests and benchmarks failure recovery strategies under hostile Kafka conditions:
    1. Offline broker detection & fast failure
    2. Exponential backoff with jitter
    3. Circuit breaker state transitions
    4. Dead-letter queue fallback
    """

    def __init__(self, dlq_dir: Optional[str] = None):
        self.dlq = DeadLetterQueue(dlq_dir=dlq_dir)
        self.circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=0.5)
        self.faulty_broker = SimulatedFaultyBroker()

    def send_with_resilience(
        self,
        key: str,
        value: str,
        max_retries: int = 3,
        base_backoff_ms: float = 10.0,
        max_backoff_ms: float = 100.0,
    ) -> bool:
        """
        Production message delivery pattern:
        1. Checks circuit breaker
        2. Applies exponential backoff with full jitter
        3. Spills to DLQ on persistent failure
        """
        if not self.circuit_breaker.can_execute():
            # Circuit breaker is OPEN: fail fast to DLQ without wasting network threads
            self.dlq.spill(key, value, "CircuitBreakerOpen")
            return False

        last_error = None
        for attempt in range(max_retries):
            try:
                self.faulty_broker.send("ecommerce-transactions", key, value)
                self.circuit_breaker.record_success()
                return True
            except Exception as exc:
                last_error = exc
                self.circuit_breaker.record_failure()

                if attempt < max_retries - 1:
                    # Exponential backoff with full jitter: sleep = uniform(0, min(max, base * 2^attempt))
                    upper_bound = min(max_backoff_ms, base_backoff_ms * (2 ** attempt))
                    jitter_sleep = random.uniform(0.001, upper_bound / 1000.0)
                    time.sleep(jitter_sleep)

        # Retries exhausted: spill to Dead-Letter Queue
        self.dlq.spill(key, value, f"RetriesExhausted: {last_error}")
        return False

    def test_offline_broker_scenario(self, count: int = 100) -> Dict[str, Any]:
        """Scenario 1: Broker is completely down."""
        logger.info("--- Testing Scenario 1: Total Broker Offline / Outage ---")
        self.faulty_broker.is_online = False
        t0 = time.perf_counter()

        delivered = 0
        for i in range(count):
            if self.send_with_resilience(f"key-{i}", f'{{"invoice": {i}}}'):
                delivered += 1

        t1 = time.perf_counter()
        elapsed_ms = (t1 - t0) * 1000.0

        result = {
            "scenario": "broker_offline",
            "messages_attempted": count,
            "delivered": delivered,
            "dlq_spilled": self.dlq.count(),
            "circuit_state": self.circuit_breaker.state.value,
            "circuit_tripped_count": self.circuit_breaker.total_tripped_count,
            "elapsed_ms": round(elapsed_ms, 2),
            "fast_fail_avg_ms": round(elapsed_ms / count, 3),
        }
        logger.info(
            "Scenario 1 Result: Delivered=%d | DLQ Spilled=%d | Circuit=%s | Tripped=%d times | FastFail Avg=%.3fms",
            delivered, result["dlq_spilled"], result["circuit_state"], result["circuit_tripped_count"], result["fast_fail_avg_ms"]
        )
        return result

    def test_intermittent_flaky_network(self, count: int = 500, drop_rate: float = 0.3) -> Dict[str, Any]:
        """Scenario 2: Intermittent network drops with exponential backoff recovery."""
        logger.info("--- Testing Scenario 2: Flaky Network with %.0f%% drop rate ---", drop_rate * 100)
        self.faulty_broker.is_online = True
        self.faulty_broker.failure_rate = drop_rate
        self.faulty_broker.socket_timeout_sec = 0.005  # fast timeout simulation
        self.circuit_breaker.state = CircuitState.CLOSED
        self.circuit_breaker.consecutive_failures = 0

        collector = PerformanceCollector("Kafka_Flaky_Recovery").start()

        for i in range(count):
            t0 = time.perf_counter()
            success = self.send_with_resilience(f"key-{i}", f'{{"invoice": {i}}}')
            t1 = time.perf_counter()
            lat_ms = (t1 - t0) * 1000.0
            collector.record_operation(lat_ms, is_error=not success)

        summary = collector.stop()
        result = {
            "scenario": "flaky_network",
            "messages_attempted": count,
            "delivered": count - summary.error_count,
            "dlq_spilled": summary.error_count,
            "p50_latency_ms": summary.p50_latency_ms,
            "p95_latency_ms": summary.p95_latency_ms,
            "p99_latency_ms": summary.p99_latency_ms,
            "recovery_rate_pct": round(((count - summary.error_count) / count) * 100, 2),
        }
        logger.info(
            "Scenario 2 Result: Recovery Rate=%.1f%% | p50=%.2fms | p95=%.2fms | p99=%.2fms",
            result["recovery_rate_pct"],
            result["p50_latency_ms"],
            result["p95_latency_ms"],
            result["p99_latency_ms"],
        )
        return result

    def test_broker_recovery_and_healing(self) -> Dict[str, Any]:
        """Scenario 3: Broker crashes, trips circuit breaker, then heals and recovers."""
        logger.info("--- Testing Scenario 3: Self-Healing & Broker Recovery ---")
        # Step A: Trip breaker
        self.faulty_broker.is_online = False
        for i in range(5):
            self.send_with_resilience(f"k-{i}", f'{{"val": {i}}}')

        assert self.circuit_breaker.state == CircuitState.OPEN, "Circuit breaker should be OPEN"

        # Step B: Broker comes back online
        logger.info("Simulating broker restoration...")
        self.faulty_broker.is_online = True
        self.faulty_broker.failure_rate = 0.0

        # Wait for recovery timeout
        time.sleep(self.circuit_breaker.recovery_timeout_sec + 0.05)

        # Step C: Send canary probe
        probe_success = self.send_with_resilience("probe-key", '{"probe": true}')
        assert probe_success, "Probe message should succeed"
        assert self.circuit_breaker.state == CircuitState.CLOSED, "Circuit breaker should have healed to CLOSED"

        # Step D: Confirm ongoing traffic flows smoothly
        recovered_delivered = sum(
            1 for i in range(20) if self.send_with_resilience(f"rec-{i}", f'{{"val": {i}}}')
        )

        result = {
            "scenario": "self_healing",
            "probe_success": probe_success,
            "final_circuit_state": self.circuit_breaker.state.value,
            "post_recovery_delivered": recovered_delivered,
        }
        logger.info("Scenario 3 Result: Healing verified. Final circuit state=%s", result["final_circuit_state"])
        return result


def main():
    parser = argparse.ArgumentParser(description="Kafka Failure & Resilience Test Harness")
    parser.add_argument("--messages", type=int, default=500, help="Message count for intermittent test")
    args = parser.parse_args()

    temp_dlq = tempfile.mkdtemp(prefix="dlq_test_")
    try:
        tester = KafkaFailureResilienceTester(dlq_dir=temp_dlq)
        r1 = tester.test_offline_broker_scenario(count=100)
        r2 = tester.test_intermittent_flaky_network(count=args.messages, drop_rate=0.25)
        r3 = tester.test_broker_recovery_and_healing()

        print("\n================== KAFKA FAILURE RESILIENCE RESULTS ==================")
        print(json.dumps({"offline_test": r1, "flaky_test": r2, "healing_test": r3}, indent=2))
    finally:
        shutil.rmtree(temp_dlq, ignore_errors=True)


if __name__ == "__main__":
    main()
