#!/usr/bin/env bash
# ==============================================================================
# 🚀 BIG DATA PLATFORM - PHASE 2 / FINAL PROJECT
# Master 1-Click Execution & Testing Runner
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================================================"
echo "  🚀 Starting Big Data Phase 2 Full Execution & Verification"
echo "========================================================================"

# 1. Check / Initialize Virtual Environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment 'venv'..."
    python3 -m venv venv
    ./venv/bin/pip install --upgrade pip
    ./venv/bin/pip install -r requirements.txt
fi

# 2. Check MongoDB Daemon
if ! pgrep -x "mongod" > /dev/null; then
    echo "Starting local MongoDB daemon..."
    mkdir -p data/db_test logs
    TCMALLOC_PER_CPU_CACHES=0 mongod --dbpath data/db_test --bind_ip 127.0.0.1 --logpath logs/mongod.log --fork || {
        echo "Warning: Could not auto-fork mongod. Please ensure MongoDB is running on port 27017."
    }
fi

# 3. Execute the Comprehensive Verification Engine
./venv/bin/python run_and_verify.py "$@"
