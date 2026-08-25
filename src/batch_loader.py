from __future__ import annotations

import csv
import time
import uuid
from pathlib import Path
from typing import Any

from metrics import RunMetrics, write_results
from quality_rules import clean_order


def run_batch(
    input_path: Path, repo: Any, reports_path: Path, batch_size: int = 1000
) -> RunMetrics:
    """Run the small-file path with bounded memory and Mongo insert_many batches."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    started = time.perf_counter()
    run_id = uuid.uuid4().hex[:12]
    size_mb = input_path.stat().st_size / (1024 * 1024)
    metrics = RunMetrics(
        run_id, input_path.name, round(size_mb, 6), "python_batch", batch_size=batch_size
    )
    raw_batch: list[dict[str, Any]] = []
    valid_batch: list[dict[str, Any]] = []
    quarantine_batch: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def flush() -> None:
        nonlocal raw_batch, valid_batch, quarantine_batch
        if not (raw_batch or valid_batch or quarantine_batch):
            return
        batch_started = time.perf_counter()
        batch_number = metrics.batches + 1
        try:
            if raw_batch:
                repo.insert_raw(raw_batch)
            outcome = repo.upsert_final(valid_batch, quarantine_batch)
        except Exception as exc:
            metrics.notes.append(f"batch={batch_number} failed: {exc.__class__.__name__}: {exc}")
            print(f"[python_batch] batch={batch_number} failed: {exc}")
            raise
        metrics.inserted_count += outcome["inserted_count"]
        metrics.updated_count += outcome["updated_count"]
        metrics.unchanged_count += outcome["unchanged_count"]
        metrics.batches += 1
        elapsed = time.perf_counter() - batch_started
        rate = len(raw_batch) / elapsed if elapsed else 0.0
        metrics.batch_timings.append(
            {
                "batch_number": metrics.batches,
                "rows": len(raw_batch),
                "elapsed_seconds": round(elapsed, 6),
                "throughput_rows_per_second": round(rate, 3),
            }
        )
        print(
            f"[python_batch] batch={metrics.batches} rows={len(raw_batch)} "
            f"elapsed_seconds={elapsed:.6f} throughput={rate:.3f} rows/sec"
        )
        raw_batch = []
        valid_batch = []
        quarantine_batch = []

    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, raw in enumerate(reader, start=2):
            raw_record = dict(raw)
            raw_batch.append(
                {
                    "run_id": run_id,
                    "source_file": input_path.name,
                    "source_row_number": row_number,
                    "ingested_at": time.time(),
                    "engine_used": "python_batch",
                    "raw_record": raw_record,
                }
            )
            metrics.rows_read += 1
            metrics.raw_loaded += 1
            cleaned = clean_order(raw_record)
            cleaned.update(
                {
                    "run_id": run_id,
                    "source_file": input_path.name,
                    "source_row_number": row_number,
                    "raw_record": raw_record,
                }
            )
            if cleaned["order_id"] and cleaned["order_id"] in seen_ids:
                cleaned["quality_status"] = "quarantined"
                cleaned["error_codes"] = ["DUPLICATE_ORDER_ID"]
                cleaned["error_details"] = [
                    {"code": "DUPLICATE_ORDER_ID", "detail": "duplicate in this run"}
                ]
            if cleaned["order_id"]:
                seen_ids.add(cleaned["order_id"])

            if cleaned["quality_status"] == "valid":
                metrics.valid_count += 1
                valid_batch.append(cleaned)
            elif cleaned["quality_status"] == "corrected":
                metrics.corrected_count += 1
                valid_batch.append(cleaned)
            else:
                metrics.quarantine_count += 1
                quarantine_batch.append(cleaned)
                for code in cleaned["error_codes"]:
                    metrics.error_case_counts[code] = metrics.error_case_counts.get(code, 0) + 1

            if len(raw_batch) >= batch_size:
                flush()
    flush()
    metrics.finalize(time.perf_counter() - started)
    write_results(metrics, reports_path)
    return metrics
