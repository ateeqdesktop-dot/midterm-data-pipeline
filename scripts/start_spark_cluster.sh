#!/usr/bin/env bash
# ==============================================================================
# Script to start a persistent Apache Spark Standalone Cluster (Master + Worker)
# Path A: Spark Standalone Cluster
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SPARK_BIN="$PROJECT_ROOT/venv/lib/python3.12/site-packages/pyspark/bin/spark-class"
LOGS_DIR="$PROJECT_ROOT/logs/spark"
mkdir -p "$LOGS_DIR"

export SPARK_LOCAL_IP="127.0.0.1"
MASTER_IP="127.0.0.1"
MASTER_PORT="7077"
MASTER_WEBUI_PORT="8080"
WORKER_WEBUI_PORT="8081"

echo "=================================================="
echo "Cleaning up any old Spark processes..."
pkill -f "org.apache.spark.deploy.master.Master" || true
pkill -f "org.apache.spark.deploy.worker.Worker" || true
sleep 1

echo "Starting Spark Master daemon on spark://$MASTER_IP:$MASTER_PORT..."
setsid "$SPARK_BIN" org.apache.spark.deploy.master.Master \
    --host "$MASTER_IP" \
    --port "$MASTER_PORT" \
    --webui-port "$MASTER_WEBUI_PORT" \
    < /dev/null > "$LOGS_DIR/master.log" 2>&1 &
sleep 2

echo "Starting Spark Worker daemon connected to spark://$MASTER_IP:$MASTER_PORT..."
setsid "$SPARK_BIN" org.apache.spark.deploy.worker.Worker \
    "spark://$MASTER_IP:$MASTER_PORT" \
    --host "$MASTER_IP" \
    --webui-port "$WORKER_WEBUI_PORT" \
    < /dev/null > "$LOGS_DIR/worker.log" 2>&1 &
sleep 2

echo "=================================================="
echo "[SUCCESS] Spark Standalone Cluster is RUNNING as background daemon!"
echo "  - Master URL: spark://$MASTER_IP:$MASTER_PORT"
echo "  - Master Web UI: http://$MASTER_IP:$MASTER_WEBUI_PORT"
echo "  - Worker Web UI: http://$MASTER_IP:$WORKER_WEBUI_PORT"
echo "  - Logs: $LOGS_DIR/"
echo "=================================================="
