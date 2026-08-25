"""Metrics models and JSON reporting."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class RunMetrics:
    run_id: str
    file_name: str
    file_size_mb: float
    engine_used: str
    rows_read: int = 0
    raw_loaded: int = 0
    valid_count: int = 0
    corrected_count: int = 0
    quarantine_count: int = 0
    elapsed_seconds: float = 0.0
    throughput: float = 0.0
    batch_size: int | None = None
    partitions: int | None = None
    error_case_counts: dict[str, int] = field(default_factory=dict)
    inserted_count: int = 0
    updated_count: int = 0
    unchanged_count: int = 0
    batches: int = 0
    batch_timings: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def finalize(self, elapsed: float) -> None:
        self.elapsed_seconds = round(elapsed, 6)
        self.throughput = round(self.rows_read / elapsed, 3) if elapsed else 0.0


def write_results(metrics: RunMetrics, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(metrics)
    payload["consistency_check"] = (
        metrics.raw_loaded
        == metrics.valid_count + metrics.corrected_count + metrics.quarantine_count
    )
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def read_results(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
