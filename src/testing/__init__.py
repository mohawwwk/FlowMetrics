"""
Testing, fault-tolerance validation, and benchmarking suite for the AI Financial / E-Commerce Platform.
"""
from src.testing.metrics_logger import MetricSummary, PerformanceCollector, configure_structured_logging

__all__ = [
    "MetricSummary",
    "PerformanceCollector",
    "configure_structured_logging",
]
