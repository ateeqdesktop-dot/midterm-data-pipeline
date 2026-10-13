#!/usr/bin/env python3
"""
Launcher script to run the FastAPI server with standard settings.
Usage:
    python run_api.py
    python run_api.py --port 8000 --host 0.0.0.0
"""
import sys
import argparse
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn

def main():
    parser = argparse.ArgumentParser(description="Run the Big Data Phase 2 Unified FastAPI server")
    parser.add_argument("--host", default="0.0.0.0", help="Host IP to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload on code change")
    args = parser.parse_args()

    print(f"Starting Big Data FastAPI server on http://{args.host}:{args.port}")
    print(f"API Docs available at http://{args.host}:{args.port}/docs")
    uvicorn.run("src.api.app:app", host=args.host, port=args.port, reload=args.reload)

if __name__ == "__main__":
    main()
