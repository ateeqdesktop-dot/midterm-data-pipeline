#!/usr/bin/env python3
"""
Root entry point for FastAPI application.
Exposes 'app' so 'uvicorn app:app --host 0.0.0.0 --port 8000' works natively.
Can also be executed directly via 'python app.py'.
"""
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api.app import app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
