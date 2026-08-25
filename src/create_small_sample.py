"""Create a reproducible CSV sample without loading the source into memory."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def create_sample(input_path: Path, output_path: Path, rows: int) -> int:
    if rows <= 0:
        raise ValueError("rows must be positive")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with input_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        with output_path.open("w", encoding="utf-8", newline="") as target:
            writer = csv.writer(target)
            try:
                writer.writerow(next(reader))
            except StopIteration:
                return 0
            written = 0
            for row in reader:
                if written >= rows:
                    break
                writer.writerow(row)
                written += 1
    return written


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, required=True)
    args = parser.parse_args()
    count = create_sample(args.input, args.output, args.rows)
    print(f"sample_rows={count} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
