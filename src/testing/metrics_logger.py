import ctypes
import json
import logging
import os
import sys
import time
import tracemalloc
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class ProcessMemoryHelper:
    """Cross-platform helper to query process Resident Set Size (RSS) memory in MB."""

    @staticmethod
    def get_memory_mb() -> float:
        if sys.platform == "win32":
            try:
                class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                    _fields_ = [
                        ("cb", ctypes.c_ulong),
                        ("PageFaultCount", ctypes.c_ulong),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t),
                    ]

                pmc = PROCESS_MEMORY_COUNTERS()
                pmc.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
                handle = ctypes.windll.kernel32.GetCurrentProcess()
                if ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(pmc), pmc.cb):
                    return round(pmc.WorkingSetSize / (1024 * 1024), 2)
            except Exception:
                pass

        # Fallback to tracemalloc current memory
        traced = tracemalloc.get_traced_memory()[0]
        return round(traced / (1024 * 1024), 2)


@dataclass
class MetricSummary:
    name: str
    total_operations: int
    total_bytes: int
    duration_seconds: float
    throughput_ops_sec: float
    throughput_mb_sec: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    avg_latency_ms: float
    start_memory_mb: float
    peak_memory_mb: float
    memory_delta_mb: float
    error_count: int = 0
    extra_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PerformanceCollector:
    """
    High-resolution performance telemetry collector.
    Measures duration, operation throughput, latency percentiles (p50, p95, p99),
    memory deltas, and error distributions.
    """

    def __init__(self, name: str):
        self.name = name
        self.latencies_ms: List[float] = []
        self.total_bytes = 0
        self.error_count = 0
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.start_memory_mb = 0.0
        self.peak_memory_mb = 0.0
        self.metadata: Dict[str, Any] = {}

    def start(self) -> "PerformanceCollector":
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        self.start_memory_mb = ProcessMemoryHelper.get_memory_mb()
        self.peak_memory_mb = self.start_memory_mb
        self.start_time = time.perf_counter()
        return self

    def record_operation(self, latency_ms: float, byte_count: int = 0, is_error: bool = False) -> None:
        self.latencies_ms.append(latency_ms)
        self.total_bytes += byte_count
        if is_error:
            self.error_count += 1

        # Check memory periodically
        if len(self.latencies_ms) % 1000 == 0:
            current_mem = ProcessMemoryHelper.get_memory_mb()
            if current_mem > self.peak_memory_mb:
                self.peak_memory_mb = current_mem

    def stop(self) -> MetricSummary:
        self.end_time = time.perf_counter()
        final_mem = ProcessMemoryHelper.get_memory_mb()
        if final_mem > self.peak_memory_mb:
            self.peak_memory_mb = final_mem

        duration = max(self.end_time - (self.start_time or self.end_time), 0.000001)
        total_ops = len(self.latencies_ms)

        if total_ops > 0:
            sorted_lat = sorted(self.latencies_ms)
            p50 = sorted_lat[int(0.50 * total_ops)]
            p95 = sorted_lat[min(int(0.95 * total_ops), total_ops - 1)]
            p99 = sorted_lat[min(int(0.99 * total_ops), total_ops - 1)]
            min_lat = sorted_lat[0]
            max_lat = sorted_lat[-1]
            avg_lat = sum(sorted_lat) / total_ops
        else:
            p50 = p95 = p99 = min_lat = max_lat = avg_lat = 0.0

        throughput_ops = round(total_ops / duration, 2)
        throughput_mb = round((self.total_bytes / (1024 * 1024)) / duration, 2)

        return MetricSummary(
            name=self.name,
            total_operations=total_ops,
            total_bytes=self.total_bytes,
            duration_seconds=round(duration, 4),
            throughput_ops_sec=throughput_ops,
            throughput_mb_sec=throughput_mb,
            p50_latency_ms=round(p50, 3),
            p95_latency_ms=round(p95, 3),
            p99_latency_ms=round(p99, 3),
            min_latency_ms=round(min_lat, 3),
            max_latency_ms=round(max_lat, 3),
            avg_latency_ms=round(avg_lat, 3),
            start_memory_mb=self.start_memory_mb,
            peak_memory_mb=self.peak_memory_mb,
            memory_delta_mb=round(self.peak_memory_mb - self.start_memory_mb, 2),
            error_count=self.error_count,
            extra_metadata=self.metadata,
        )


def configure_structured_logging(
    log_name: str = "ecommerce-testing",
    level: int = logging.INFO,
    log_file: Optional[str] = None,
) -> logging.Logger:
    """Configures structured format logging with optional JSON output file."""
    logger = logging.getLogger(log_name)
    logger.setLevel(level)
    logger.propagate = False

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # Console handler with clean timestamp format (UTF-8 safe)
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    console_handler = logging.StreamHandler(sys.stdout)
    console_format = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # Optional file handler
    if log_file:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(file_path), encoding="utf-8")
        file_handler.setFormatter(console_format)
        logger.addHandler(file_handler)

    return logger
