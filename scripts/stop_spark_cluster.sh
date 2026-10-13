#!/usr/bin/env bash
# ==============================================================================
# Script to stop the local Apache Spark Standalone Cluster (Master + Worker)
# ==============================================================================

echo "=================================================="
echo "Stopping Spark Standalone Cluster..."
echo "=================================================="

# Stop Worker
if pgrep -f "org.apache.spark.deploy.worker.Worker" > /dev/null; then
    echo "[INFO] Stopping Spark Worker..."
    pkill -f "org.apache.spark.deploy.worker.Worker" || true
else
    echo "[INFO] No Spark Worker running."
fi

# Stop Master
if pgrep -f "org.apache.spark.deploy.master.Master" > /dev/null; then
    echo "[INFO] Stopping Spark Master..."
    pkill -f "org.apache.spark.deploy.master.Master" || true
else
    echo "[INFO] No Spark Master running."
fi

echo "[SUCCESS] Spark Cluster stopped."
echo "=================================================="
