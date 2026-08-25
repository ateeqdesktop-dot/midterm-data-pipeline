from __future__ import annotations

import importlib.metadata
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "verification_report.json"
REQUIRED_FILES = [
    "README.md",
    "requirements.txt",
    "config/settings.py",
    "data/.gitkeep",
    "src/main.py",
    "src/file_router.py",
    "src/create_small_sample.py",
    "src/batch_loader.py",
    "src/spark_loader.py",
    "src/quality_rules.py",
    "src/elt_pipeline.py",
    "src/incremental_loader.py",
    "src/mongo_setup.py",
    "src/metrics.py",
    "tests/test_cleaning_rules.py",
    "tests/test_classification.py",
    "tests/test_pipeline.py",
    "reports/results.md",
    "reports/results.json",
    "docs/architecture.md",
    "docs/pdf_requirements_traceability.md",
    "Dockerfile",
    "docker-compose.yml",
    ".github/workflows/ci.yml",
]
REQUIRED_PACKAGES = ["pyspark", "pymongo", "mongomock", "pandas", "pytest", "ruff"]
REQUIRED_METRICS = [
    "run_id",
    "file_name",
    "file_size_mb",
    "engine_used",
    "rows_read",
    "raw_loaded",
    "valid_count",
    "corrected_count",
    "quarantine_count",
    "elapsed_seconds",
    "throughput",
    "error_case_counts",
    "inserted_count",
    "updated_count",
    "unchanged_count",
]


def command_check(
    name: str, command: list[str], env: dict[str, str] | None = None
) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env={**os.environ, **(env or {})},
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "name": name,
        "command": " ".join(command),
        "passed": completed.returncode == 0,
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-1000:],
        "stderr_tail": completed.stderr[-1000:],
    }


def main() -> int:
    results: dict[str, Any] = {"project_root": str(ROOT), "checks": []}
    missing = [path for path in REQUIRED_FILES if not (ROOT / path).is_file()]
    results["required_files"] = {"passed": not missing, "missing": missing}
    versions: dict[str, str] = {}
    missing_packages: list[str] = []
    for package in REQUIRED_PACKAGES:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            missing_packages.append(package)
    results["packages"] = {
        "passed": not missing_packages,
        "versions": versions,
        "missing": missing_packages,
    }

    env = {"PYTHONPATH": str(ROOT / "src")}
    results["checks"].extend(
        [
            command_check("pytest", [sys.executable, "-m", "pytest", "-q"], env),
            command_check("ruff_check", ["ruff", "check", "src", "tests", "scripts"], env),
            command_check(
                "ruff_format", ["ruff", "format", "--check", "src", "tests", "scripts"], env
            ),
            command_check(
                "compileall",
                [sys.executable, "-m", "compileall", "-q", "src", "tests", "scripts"],
                env,
            ),
        ]
    )

    metrics_path = ROOT / "reports" / "results.json"
    try:
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        missing_metrics = [key for key in REQUIRED_METRICS if key not in metrics]
        consistency = metrics.get("consistency_check") is True
        idempotency = metrics.get("idempotency", {}).get("passed") is True
        results["results_json"] = {
            "passed": not missing_metrics and consistency and idempotency,
            "missing_metrics": missing_metrics,
            "consistency_check": consistency,
            "idempotency_passed": idempotency,
        }
    except (OSError, json.JSONDecodeError) as exc:
        results["results_json"] = {"passed": False, "error": str(exc)}

    traceability = (ROOT / "docs" / "pdf_requirements_traceability.md").read_text(encoding="utf-8")
    required_sections = [
        "6.1",
        "6.2",
        "6.3",
        "6.4",
        "6.5",
        "6.6",
        "6.7",
        "6.8",
        "6.9",
        "6.10",
        "6.11",
        "6.12",
        "9",
        "10",
        "11",
        "12",
        "13",
        "14",
    ]
    results["pdf_traceability"] = {
        "passed": all(section in traceability for section in required_sections),
        "missing_sections": [
            section for section in required_sections if section not in traceability
        ],
    }

    try:
        from pymongo import MongoClient

        client = MongoClient(
            os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017"), serverSelectionTimeoutMS=2000
        )
        client.admin.command("ping")
        db = client[os.getenv("MONGO_DATABASE", "midterm_orders")]
        results["mongodb"] = {
            "passed": True,
            "collections": {
                name: db[name].count_documents({})
                for name in ("orders_raw", "orders_validated", "orders_quarantine")
            },
            "validated_indexes": [item["name"] for item in db.orders_validated.list_indexes()],
            "quarantine_indexes": [item["name"] for item in db.orders_quarantine.list_indexes()],
        }
        client.close()
    except (OSError, TimeoutError, ConnectionError, RuntimeError) as exc:  # pragma: no cover
        results["mongodb"] = {"passed": False, "error": str(exc)}

    core_checks = [
        results["required_files"],
        results["packages"],
        results["results_json"],
        results["pdf_traceability"],
    ]
    core_checks.extend(results["checks"])
    results["passed"] = all(check.get("passed", False) for check in core_checks)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(
        json.dumps(results, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
