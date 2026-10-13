#!/usr/bin/env python3
"""
Root entry point for create_small_sample.py as specified in Midterm PDF (section 6.1):
python create_small_sample.py --input orders_huge_mixed_quality.csv --rows 100000
"""
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.create_small_sample import main

if __name__ == "__main__":
    main()
