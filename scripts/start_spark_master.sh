#!/usr/bin/env bash
# ==============================================================================
# Script to start Spark Master on Node 1 (Physical Server / VM 1)
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SPARK_BIN="$PROJECT_ROOT/venv/lib/python3.12/site-packages/pyspark/bin/spark-class"
LOGS_DIR="$PROJECT_ROOT/logs/spark"
mkdir -p "$LOGS_DIR"

# Master IP can be passed as $1 or via MASTER_HOST env var; defaults to 0.0.0.0 (or LAN IP)
MASTER_HOST="${1:-${MASTER_HOST:-0.0.0.0}}"
MASTER_PORT="${MASTER_PORT:-7077}"
MASTER_WEBUI_PORT="${MASTER_WEBUI_PORT:-8080}"

echo "=================================================="
echo "Cleaning up any existing Spark Master..."
pkill -f "org.apache.spark.deploy.master.Master" || true
sleep 1

echo "Starting Spark Master on host: $MASTER_HOST, port: $MASTER_PORT..."
setsid "$SPARK_BIN" org.apache.spark.deploy.master.Master \
    --host "$MASTER_HOST" \
    --port "$MASTER_PORT" \
    --webui-port "$MASTER_WEBUI_PORT" \
    < /dev/null > "$LOGS_DIR/master.log" 2>&1 &

sleep 2

echo "[SUCCESS] Spark Master is RUNNING!"
echo "  - Connect Workers to: spark://<NODE_1_IP>:$MASTER_PORT"
echo "  - Web UI: http://<NODE_1_IP>:$MASTER_WEBUI_PORT"
echo "  - Log: $LOGS_DIR/master.log"
echo "=================================================="
