"""Single entry point for the hybrid orders ELT pipeline."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from batch_loader import run_batch
from file_router import choose_engine
from mongo_setup import MemoryRepository, MongoRepository
from spark_loader import run_spark


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Hybrid Python Batch/PySpark MongoDB ELT pipeline")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--reports", type=Path, default=Path("reports/results.json"))
    parser.add_argument("--engine", choices=("auto", "python_batch", "pyspark"), default="auto")
    parser.add_argument("--threshold-mb", type=float, default=200.0)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--spark-master", default="local[*]")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--mongo-database", default=os.getenv("MONGO_DATABASE", "midterm_orders"))
    parser.add_argument("--backend", choices=("mongo", "memory"), default="memory")
    parser.add_argument("--check-idempotency", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    route = choose_engine(args.input, args.threshold_mb)
    engine = route.engine if args.engine == "auto" else args.engine
    print(
        json.dumps(
            {"file_size_mb": route.size_mb, "engine_used": engine, "reason": route.reason},
            ensure_ascii=False,
        )
    )
    repo = (
        MemoryRepository(args.mongo_database)
        if args.backend == "memory"
        else MongoRepository(args.mongo_uri, args.mongo_database)
    )
    try:
        if engine == "python_batch":
            metrics = run_batch(args.input, repo, args.reports, args.batch_size)
        else:
            metrics = run_spark(args.input, repo, args.reports, args.partitions, args.spark_master)
        result = {
            "run_id": metrics.run_id,
            "engine_used": metrics.engine_used,
            "counts": repo.counts(),
            "metrics": metrics.__dict__,
        }
        if args.check_idempotency:
            before = repo.counts()["orders_validated"]
            rerun = (
                run_batch(args.input, repo, args.reports, args.batch_size)
                if engine == "python_batch"
                else run_spark(args.input, repo, args.reports, args.partitions, args.spark_master)
            )
            after = repo.counts()["orders_validated"]
            result["idempotency"] = {
                "before_validated": before,
                "after_validated": after,
                "unchanged": rerun.unchanged_count,
                "passed": before == after,
            }
            report_payload = json.loads(args.reports.read_text(encoding="utf-8"))
            report_payload["idempotency"] = result["idempotency"]
            args.reports.write_text(
                json.dumps(report_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0
    finally:
        repo.close()


if __name__ == "__main__":
    raise SystemExit(main())
