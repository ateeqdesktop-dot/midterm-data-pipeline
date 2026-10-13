#!/usr/bin/env bash
# ==============================================================================
# Script to start Spark Worker on Node 2 (Physical Server / VM 2)
# Usage: ./scripts/start_spark_worker.sh <SPARK_MASTER_URL> [WORKER_HOST]
# Example: ./scripts/start_spark_worker.sh spark://192.168.1.100:7077 192.168.1.101
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SPARK_BIN="$PROJECT_ROOT/venv/lib/python3.12/site-packages/pyspark/bin/spark-class"
LOGS_DIR="$PROJECT_ROOT/logs/spark"
mkdir -p "$LOGS_DIR"

MASTER_URL="${1:-${SPARK_MASTER_URL:-spark://127.0.0.1:7077}}"
WORKER_HOST="${2:-${WORKER_HOST:-0.0.0.0}}"
WORKER_WEBUI_PORT="${WORKER_WEBUI_PORT:-8081}"
WORKER_CORES="${WORKER_CORES:-$(nproc)}"
WORKER_MEMORY="${WORKER_MEMORY:-4g}"

echo "=================================================="
echo "Cleaning up any existing Spark Worker..."
pkill -f "org.apache.spark.deploy.worker.Worker" || true
sleep 1

echo "Starting Spark Worker connecting to $MASTER_URL..."
echo "  - Worker Host: $WORKER_HOST"
echo "  - Cores: $WORKER_CORES"
echo "  - Memory: $WORKER_MEMORY"

setsid "$SPARK_BIN" org.apache.spark.deploy.worker.Worker \
    "$MASTER_URL" \
    --host "$WORKER_HOST" \
    --webui-port "$WORKER_WEBUI_PORT" \
    --cores "$WORKER_CORES" \
    --memory "$WORKER_MEMORY" \
    < /dev/null > "$LOGS_DIR/worker.log" 2>&1 &

sleep 2

echo "[SUCCESS] Spark Worker is RUNNING and connected to Master!"
echo "  - Worker Web UI: http://<NODE_2_IP>:$WORKER_WEBUI_PORT"
echo "  - Log: $LOGS_DIR/worker.log"
echo "=================================================="
