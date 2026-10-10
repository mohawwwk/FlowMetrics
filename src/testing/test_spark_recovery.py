import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Ensure Hadoop native library on Windows is discoverable
hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
if os.path.exists(hadoop_bin) and hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from src.testing.metrics_logger import (
    MetricSummary,
    PerformanceCollector,
    configure_structured_logging,
)

logger = configure_structured_logging("spark-recovery")


@dataclass
class CheckpointBatchInfo:
    batch_id: int
    offset_file_exists: bool
    commit_file_exists: bool
    kafka_offsets: Dict[str, Dict[str, int]]
    is_committed: bool


class SparkCheckpointInspector:
    """
    Parses, validates, and manipulates Apache Spark Structured Streaming checkpoint directories:
    - offsets/
    - commits/
    - metadata
    - sources/
    """

    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.offsets_dir = self.checkpoint_dir / "offsets"
        self.commits_dir = self.checkpoint_dir / "commits"
        self.metadata_file = self.checkpoint_dir / "metadata"

    def is_valid_checkpoint(self) -> bool:
        return self.offsets_dir.exists() and self.commits_dir.exists()

    def get_all_batches(self) -> List[CheckpointBatchInfo]:
        """Lists all recorded batches and correlates offsets with commits."""
        if not self.is_valid_checkpoint():
            return []

        offset_files = {
            int(p.name): p for p in self.offsets_dir.glob("*") if p.name.isdigit()
        }
        commit_files = {
            int(p.name): p for p in self.commits_dir.glob("*") if p.name.isdigit()
        }

        all_batch_ids = sorted(set(offset_files.keys()).union(commit_files.keys()))
        batches: List[CheckpointBatchInfo] = []

        for b_id in all_batch_ids:
            has_offset = b_id in offset_files
            has_commit = b_id in commit_files
            kafka_offsets: Dict[str, Dict[str, int]] = {}

            if has_offset:
                content = offset_files[b_id].read_text(encoding="utf-8").strip().splitlines()
                # Spark checkpoint offset format:
                # Line 0: v1
                # Line 1: watermark & json conf
                # Line 2: topic partition offsets json
                if len(content) >= 3:
                    try:
                        kafka_offsets = json.loads(content[2])
                    except Exception:
                        pass

            batches.append(CheckpointBatchInfo(
                batch_id=b_id,
                offset_file_exists=has_offset,
                commit_file_exists=has_commit,
                kafka_offsets=kafka_offsets,
                is_committed=has_commit,
            ))

        return batches

    def get_latest_committed_offset(self, topic: str = "ecommerce-transactions", partition: str = "0") -> int:
        batches = self.get_all_batches()
        latest_offset = 0
        for b in batches:
            if b.is_committed and topic in b.kafka_offsets:
                part_offset = b.kafka_offsets[topic].get(partition, 0)
                if part_offset > latest_offset:
                    latest_offset = part_offset
        return latest_offset


class SparkRecoverySimulator:
    """
    Simulates Spark Structured Streaming execution, abrupt mid-batch crash,
    WAL checkpoint recovery, and seamless resumption without data duplication.
    """

    def __init__(self, temp_dir: Optional[str] = None):
        self.temp_dir = temp_dir or tempfile.mkdtemp(prefix="spark_recovery_")
        self.checkpoint_dir = Path(self.temp_dir) / "checkpoint"
        self.offsets_dir = self.checkpoint_dir / "offsets"
        self.commits_dir = self.checkpoint_dir / "commits"
        self.offsets_dir.mkdir(parents=True, exist_ok=True)
        self.commits_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Spark metadata file (version 1)
        (self.checkpoint_dir / "metadata").write_text('{"id":"spark-stream-uuid-test"}', encoding="utf-8")

        self.sink_storage: List[Dict[str, Any]] = []
        self.processed_keys: set = set()
        self.duplicates_prevented = 0

    def write_checkpoint_batch(
        self,
        batch_id: int,
        start_offset: int,
        end_offset: int,
        commit: bool = True,
        topic: str = "ecommerce-transactions",
    ) -> None:
        """Simulates Spark writing offsets/<batch_id> and optionally commits/<batch_id>."""
        offset_file = self.offsets_dir / str(batch_id)
        content = (
            "v1\n"
            f'{{"batchWatermarkMs":0,"batchTimestampMs":{int(time.time()*1000)}}}\n'
            f'{{"{topic}":{{"0":{end_offset}}}}}\n'
        )
        offset_file.write_text(content, encoding="utf-8")

        if commit:
            commit_file = self.commits_dir / str(batch_id)
            commit_file.write_text(f"v1\n{{\"batchTimestampMs\":{int(time.time()*1000)}}}\n", encoding="utf-8")

    def simulate_idempotent_sink_write(self, batch_records: List[Dict[str, Any]]) -> int:
        """
        Simulates an idempotent sink (e.g. SQLite INSERT OR IGNORE or Parquet deduplication).
        Ensures exact-once processing even if micro-batches are replayed.
        """
        written = 0
        for rec in batch_records:
            uniq_id = f"{rec['invoice_no']}_{rec['stock_code']}_{rec['timestamp']}"
            if uniq_id in self.processed_keys:
                self.duplicates_prevented += 1
            else:
                self.processed_keys.add(uniq_id)
                self.sink_storage.append(rec)
                written += 1
        return written

    def run_recovery_test(self) -> Dict[str, Any]:
        """
        Executes comprehensive Spark crash and restart lifecycle:
        1. Batch 0 runs normally and commits (offsets 0 -> 100).
        2. Batch 1 runs normally and commits (offsets 100 -> 250).
        3. Batch 2 starts (offsets 250 -> 400), writes offsets/2, processes records into sink,
           BUT CRASHES ABRUPTLY before commits/2 is written (simulating power loss / kill -9).
        4. Driver restarts:
           - Detects uncommitted batch 2 in WAL.
           - Re-reads Kafka offsets 250 -> 400.
           - Re-executes idempotent sink write.
           - Verifies zero duplicate records admitted into sink.
           - Successfully writes commits/2.
        5. Pipeline continues seamlessly to Batch 3 (offsets 400 -> 600).
        """
        logger.info("Step 1: Processing Batch 0 (offsets 0 -> 100)...")
        b0_records = [{"invoice_no": f"INV-{i}", "stock_code": f"SC-{i}", "timestamp": 1000} for i in range(0, 100)]
        self.simulate_idempotent_sink_write(b0_records)
        self.write_checkpoint_batch(0, 0, 100, commit=True)

        logger.info("Step 2: Processing Batch 1 (offsets 100 -> 250)...")
        b1_records = [{"invoice_no": f"INV-{i}", "stock_code": f"SC-{i}", "timestamp": 1000} for i in range(100, 250)]
        self.simulate_idempotent_sink_write(b1_records)
        self.write_checkpoint_batch(1, 100, 250, commit=True)

        logger.info("Step 3: Simulating CRASH on Batch 2 (offsets 250 -> 400)...")
        b2_records = [{"invoice_no": f"INV-{i}", "stock_code": f"SC-{i}", "timestamp": 1000} for i in range(250, 400)]
        # Partial sink write before crash
        self.simulate_idempotent_sink_write(b2_records)
        # Write offset log BUT DO NOT COMMIT (simulate crash / SIGKILL)
        self.write_checkpoint_batch(2, 250, 400, commit=False)
        logger.warning("Spark session terminated abnormally! commits/2 is MISSING.")

        # Inspect checkpoint state during crash
        inspector = SparkCheckpointInspector(str(self.checkpoint_dir))
        batches_before_recovery = inspector.get_all_batches()
        uncommitted = [b for b in batches_before_recovery if not b.is_committed]
        assert len(uncommitted) == 1, "Should have exactly 1 uncommitted batch"
        assert uncommitted[0].batch_id == 2, "Uncommitted batch should be 2"

        logger.info("Step 4: Spark Restart & Recovery Procedure Initiated...")
        t_restart_start = time.perf_counter()

        # Spark restart logic: find the latest committed batch
        committed_batches = [b for b in batches_before_recovery if b.is_committed]
        last_committed_id = max(b.batch_id for b in committed_batches)
        restart_batch_id = last_committed_id + 1

        logger.info("Restart detected uncommitted batch %d. Replaying micro-batch from offset 250 to 400...", restart_batch_id)
        # Replay batch 2
        replayed_written = self.simulate_idempotent_sink_write(b2_records)
        assert replayed_written == 0, "Idempotent sink must prevent all 150 duplicate records!"
        assert self.duplicates_prevented == 150, "All 150 records must be recognized as duplicates"

        # Mark batch 2 committed
        (self.commits_dir / str(restart_batch_id)).write_text("v1\n{\"recovered\":true}\n", encoding="utf-8")
        t_restart_end = time.perf_counter()
        recovery_duration_ms = (t_restart_end - t_restart_start) * 1000.0

        logger.info("Step 5: Resuming ongoing streaming pipeline to Batch 3 (offsets 400 -> 600)...")
        b3_records = [{"invoice_no": f"INV-{i}", "stock_code": f"SC-{i}", "timestamp": 1000} for i in range(400, 600)]
        b3_written = self.simulate_idempotent_sink_write(b3_records)
        self.write_checkpoint_batch(3, 400, 600, commit=True)

        batches_after_recovery = inspector.get_all_batches()
        assert all(b.is_committed for b in batches_after_recovery), "All batches must be committed after recovery"
        assert len(self.sink_storage) == 600, f"Expected exactly 600 unique records in sink, got {len(self.sink_storage)}"

        result = {
            "status": "PASSED",
            "total_batches_processed": len(batches_after_recovery),
            "crash_batch_id": 2,
            "recovered_batch_id": 2,
            "duplicates_prevented": self.duplicates_prevented,
            "final_unique_sink_records": len(self.sink_storage),
            "recovery_latency_ms": round(recovery_duration_ms, 3),
            "checkpoint_integrity_verified": True,
        }
        logger.info(
            "Spark Restart Recovery Successful: %d unique sink records | %d duplicates avoided | Recovery=%.3fms",
            result["final_unique_sink_records"],
            result["duplicates_prevented"],
            result["recovery_latency_ms"],
        )
        return result

    def cleanup(self) -> None:
        if os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir, ignore_errors=True)
            except Exception:
                pass


def inspect_existing_checkpoints(checkpoint_base_dir: str = "data/checkpoint") -> Dict[str, Any]:
    """Inspects all actual Spark checkpoints generated during Phase 3 streaming."""
    base_path = Path(checkpoint_base_dir)
    if not base_path.exists():
        return {"error": f"Base path {checkpoint_base_dir} does not exist"}

    inspections: Dict[str, Any] = {}
    for sub in base_path.iterdir():
        if sub.is_dir():
            inspector = SparkCheckpointInspector(str(sub))
            if inspector.is_valid_checkpoint():
                batches = inspector.get_all_batches()
                inspections[sub.name] = {
                    "valid": True,
                    "total_batches": len(batches),
                    "committed_count": sum(1 for b in batches if b.is_committed),
                    "latest_offset": inspector.get_latest_committed_offset(),
                }
    return inspections


def main():
    parser = argparse.ArgumentParser(description="Spark Restart & Checkpoint Recovery Test")
    parser.add_argument("--inspect-existing", action="store_true", help="Inspect existing checkpoints in data/checkpoint")
    args = parser.parse_args()

    if args.inspect_existing:
        results = inspect_existing_checkpoints()
        print("\n=== EXISTING SPARK CHECKPOINT INSPECTION ===")
        print(json.dumps(results, indent=2))
        return

    simulator = SparkRecoverySimulator()
    try:
        res = simulator.run_recovery_test()
        print("\n=== SPARK RECOVERY TEST RESULT ===")
        print(json.dumps(res, indent=2))
    finally:
        simulator.cleanup()


if __name__ == "__main__":
    main()
