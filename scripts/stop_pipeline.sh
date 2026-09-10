#!/bin/bash
###############################################################################
# stop_pipeline.sh — FINAL VERSION
#
# Gracefully stops the producer and Spark streaming job. Uses the saved PID
# files when available; if a PID file is missing or stale, falls back to
# finding the process by name so nothing is ever left running silently.
# Does NOT touch HDFS data, Spark checkpoints, or Kafka offsets.
###############################################################################

set -uo pipefail

BASE_DIR="/home/student/Kafka_Spark_Pipline"
RUN_DIR="$BASE_DIR/run"

PRODUCER_PID_FILE="$RUN_DIR/producer.pid"
SPARK_PID_FILE="$RUN_DIR/spark.pid"

PRODUCER_PATTERN="generate_orders.py"
SPARK_PATTERN="spark-submit"

GRACE_SECONDS=10

echo "=== stop_pipeline.sh : $(date) ==="

kill_pid_gracefully() {
    local pid="$1" label="$2"
    echo "[$label] Sending SIGTERM to PID $pid..."
    kill -TERM "$pid" 2>/dev/null

    local waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$GRACE_SECONDS" ]; do
        sleep 1
        waited=$((waited + 1))
    done

    if kill -0 "$pid" 2>/dev/null; then
        echo "[$label] Still running after ${GRACE_SECONDS}s — forcing SIGKILL."
        kill -KILL "$pid" 2>/dev/null
        sleep 1
    fi

    if kill -0 "$pid" 2>/dev/null; then
        echo "[$label] WARNING: PID $pid still appears alive."
        return 1
    else
        echo "[$label] Stopped."
        return 0
    fi
}

stop_by_pidfile_or_pattern() {
    local label="$1" pid_file="$2" pattern="$3"
    local pid=""

    # 1. Try the PID file first
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "[$label] PID file exists but process $pid is dead. Cleaning up."
            pid=""
            rm -f "$pid_file"
        fi
    fi

    # 2. Fall back to searching by process name (catches orphans / stale-file cases)
    if [ -z "$pid" ]; then
        pid=$(pgrep -f "$pattern" | head -n1)
        if [ -n "$pid" ]; then
            echo "[$label] No valid PID file — found running process by name (PID $pid)."
        fi
    fi

    if [ -z "$pid" ]; then
        echo "[$label] Not running."
        return
    fi

    kill_pid_gracefully "$pid" "$label"
    rm -f "$pid_file"

    # 3. Double-check: kill any remaining stragglers matching the pattern
    for leftover in $(pgrep -f "$pattern"); do
        echo "[$label] Cleaning up leftover process $leftover."
        kill -9 "$leftover" 2>/dev/null
    done
}

stop_by_pidfile_or_pattern "Producer" "$PRODUCER_PID_FILE" "$PRODUCER_PATTERN"
stop_by_pidfile_or_pattern "Spark Streaming" "$SPARK_PID_FILE" "$SPARK_PATTERN"

echo ""
echo "=== Pipeline stopped. ==="
echo "HDFS Parquet data, Spark checkpoints, and Kafka offsets were left untouched."
echo "Run ./start_pipeline.sh again to resume ingestion seamlessly."