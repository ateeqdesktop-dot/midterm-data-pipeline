#!/usr/bin/env python3
"""
Root entry point for file_router per assignment section 6.2.
Can be imported or run directly from command line:
    python file_router.py data/sample_orders.csv
"""
import sys
import argparse
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.file_router import route_file

def main():
    parser = argparse.ArgumentParser(description="Inspect file and route to batch or pyspark engine.")
    parser.add_argument("file_path", nargs="?", default="data/sample_orders.csv", help="Path to input CSV file")
    args = parser.parse_args()
    engine = route_file(args.file_path)
    print(f"Routing outcome: {engine}")

if __name__ == "__main__":
    main()
