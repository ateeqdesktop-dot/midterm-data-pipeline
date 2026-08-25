"""Automatic engine selection by input size."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RouteDecision:
    engine: str
    size_bytes: int
    size_mb: float
    threshold_mb: float
    reason: str


def choose_engine(path: Path, threshold_mb: float = 200.0) -> RouteDecision:
    size_bytes = path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    if size_mb <= threshold_mb:
        engine = "python_batch"
        reason = f"{size_mb:.3f}MB <= {threshold_mb:.3f}MB threshold; streaming batches avoid Spark startup overhead"
    else:
        engine = "pyspark"
        reason = f"{size_mb:.3f}MB > {threshold_mb:.3f}MB threshold; Spark DataFrame partitions provide parallel processing"
    return RouteDecision(engine, size_bytes, round(size_mb, 6), threshold_mb, reason)
