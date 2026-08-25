"""Pipeline orchestration facade."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from batch_loader import run_batch
from file_router import choose_engine
from spark_loader import run_spark


def run_pipeline(
    input_path: Path,
    repo: Any,
    reports_path: Path,
    threshold_mb: float = 200.0,
    batch_size: int = 1000,
    partitions: int = 4,
    spark_master: str = "local[*]",
) -> Any:
    decision = choose_engine(input_path, threshold_mb)
    if decision.engine == "python_batch":
        return run_batch(input_path, repo, reports_path, batch_size)
    return run_spark(input_path, repo, reports_path, partitions, spark_master)
