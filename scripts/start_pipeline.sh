
#!/bin/bash
###############################################################################
# start_pipeline.sh — FINAL VERSION
#
# Brings up the entire stack in the correct order and launches the pipeline:
#   HDFS (NameNode/DataNode) -> permissions fix -> leave safe mode ->
#   YARN (ResourceManager/NodeManager) -> Hive (metastore + HiveServer2) ->
#   Kafka topic check -> HDFS dirs -> Hive DDL -> producer + Spark job
###############################################################################

set -uo pipefail

# ============================== CONFIG ======================================
BASE_DIR="/home/student/Kafka_Spark_Pipline"

PRODUCER_SCRIPT="$BASE_DIR/Producer/generate_orders.py"
SPARK_SCRIPT="$BASE_DIR/Spark/spark_streaming.py"
HIVE_DDL="$BASE_DIR/hive/create_table.sql"

KAFKA_BIN_DIR="$(find / -maxdepth 6 -name kafka-topics.sh 2>/dev/null -exec dirname {} \; | head -n 1)"
BOOTSTRAP_SERVERS="localhost:9092"
KAFKA_TOPIC="topic1_logs"

HDFS_DATA_PATH="/user/hive/warehouse/ecommerce_dw.db/streaming_orders"
HDFS_CHECKPOINT_PATH="/user/spark/checkpoints/streaming_orders"
NAMENODE_DATA_DIR="/home/hadoop/hadoopdata/hdfs/namenode"
DATANODE_DATA_DIR="/home/hadoop/hadoopdata/hdfs/datanode"

SPARK_VERSION="3.1.2"
SCALA_BINARY_VERSION="2.12"

RUN_DIR="$BASE_DIR/run"
LOG_DIR="$BASE_DIR/logs"
# ============================================================================

mkdir -p "$RUN_DIR" "$LOG_DIR"
PRODUCER_PID_FILE="$RUN_DIR/producer.pid"
SPARK_PID_FILE="$RUN_DIR/spark.pid"
PRODUCER_LOG="$LOG_DIR/producer.log"
SPARK_LOG="$LOG_DIR/spark_streaming.log"

warn() { echo "    [WARN] $1"; }
info() { echo "    -> $1"; }
fail() { echo "[FATAL] $1"; exit 1; }

wait_for_jvm_procs() {
    # $1 = space-separated process names to wait for, $2 = max seconds
    local names="$1" max_wait="$2" waited=0 ok
    while [ "$waited" -lt "$max_wait" ]; do
        ok=true
        for n in $names; do
            jps | grep -q "$n" || ok=false
        done
        [ "$ok" = true ] && return 0
        sleep 5
        waited=$((waited + 5))
    done
    return 1
}

echo "=== start_pipeline.sh : $(date) ==="

# -----------------------------------------------------------------------
# 1. HDFS: fix permissions, start, wait, leave safe mode
# -----------------------------------------------------------------------
echo "[1/8] Preparing and starting HDFS..."

sudo chown -R student:student "$NAMENODE_DATA_DIR" "$DATANODE_DATA_DIR" 2>/dev/null
sudo chmod -R 755 "$NAMENODE_DATA_DIR" "$DATANODE_DATA_DIR" 2>/dev/null

if ! jps | grep -q "NameNode" || ! jps | grep -q "DataNode"; then
    start-dfs.sh >> "$LOG_DIR/hdfs_start.log" 2>&1
fi

if wait_for_jvm_procs "NameNode DataNode" 60; then
    info "NameNode and DataNode are up."
    HDFS_OK=true
else
    warn "HDFS still not up after 60s — continuing anyway, later steps may fail."
    HDFS_OK=false
fi

if [ "$HDFS_OK" = true ]; then
    for i in $(seq 1 12); do
        MODE=$(hdfs dfsadmin -safemode get 2>/dev/null)
        echo "$MODE" | grep -qi "OFF" && break
        hdfs dfsadmin -safemode leave >/dev/null 2>&1
        sleep 5
    done
    info "Safe mode handling done ($(hdfs dfsadmin -safemode get 2>/dev/null))."
fi

# -----------------------------------------------------------------------
# 2. YARN: start, wait
# -----------------------------------------------------------------------
echo "[2/8] Starting YARN..."

if ! jps | grep -q "ResourceManager" || ! jps | grep -q "NodeManager"; then
    start-yarn.sh >> "$LOG_DIR/yarn_start.log" 2>&1
fi

if wait_for_jvm_procs "ResourceManager NodeManager" 60; then
    info "ResourceManager and NodeManager are up."
else
    warn "YARN still not up after 60s — Hive queries needing MapReduce may hang/fail."
fi

# -----------------------------------------------------------------------
# 3. Hive: start metastore + HiveServer2 if not already running
# -----------------------------------------------------------------------
echo "[3/8] Starting Hive services..."

if ! jps | grep -q "RunJar"; then
    nohup hive --service metastore >> "$LOG_DIR/hive_metastore.log" 2>&1 &
    sleep 10
    nohup hiveserver2 >> "$LOG_DIR/hiveserver2.log" 2>&1 &
    sleep 15
fi

if jps | grep -q "RunJar"; then
    info "Hive metastore / HiveServer2 running."
else
    warn "Hive services did not start cleanly — DDL step may fail."
fi

# -----------------------------------------------------------------------
# 4. Kafka broker + topic
# -----------------------------------------------------------------------
echo "[4/8] Checking Kafka broker and topic..."

if [ -z "$KAFKA_BIN_DIR" ] || [ ! -f "$KAFKA_BIN_DIR/kafka-topics.sh" ]; then
    fail "Could not locate kafka-topics.sh. Set KAFKA_BIN_DIR manually at the top of this script."
fi

if ! "$KAFKA_BIN_DIR/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" --list >/dev/null 2>&1; then
    fail "Kafka broker is not responding at $BOOTSTRAP_SERVERS. Start Kafka/Zookeeper first."
fi
info "Kafka broker is reachable."

if "$KAFKA_BIN_DIR/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" --list | grep -qx "$KAFKA_TOPIC"; then
    info "Topic '$KAFKA_TOPIC' already exists."
else
    "$KAFKA_BIN_DIR/kafka-topics.sh" --create --topic "$KAFKA_TOPIC" \
        --bootstrap-server "$BOOTSTRAP_SERVERS" --partitions 1 --replication-factor 1 \
        || fail "Failed to create Kafka topic."
    info "Topic '$KAFKA_TOPIC' created."
fi

# -----------------------------------------------------------------------
# 5. HDFS directories
# -----------------------------------------------------------------------
echo "[5/8] Ensuring HDFS directories exist..."

if [ "$HDFS_OK" = true ]; then
    hdfs dfs -mkdir -p "$HDFS_DATA_PATH"       && info "$HDFS_DATA_PATH ready."
    hdfs dfs -mkdir -p "$HDFS_CHECKPOINT_PATH" && info "$HDFS_CHECKPOINT_PATH ready."
else
    warn "Skipping — HDFS not healthy."
fi

# -----------------------------------------------------------------------
# 6. Hive DDL — use 'hive -f' directly (avoids beeline impersonation/embedded-mode issues)
# -----------------------------------------------------------------------
echo "[6/8] Running Hive DDL..."

if [ ! -s "$HIVE_DDL" ]; then
    warn "$HIVE_DDL is empty or missing — skipping."
elif command -v hive >/dev/null 2>&1; then
    hive -f "$HIVE_DDL" >> "$LOG_DIR/hive_ddl.log" 2>&1 \
        && info "Hive DDL executed." \
        || warn "Hive DDL failed. Check $LOG_DIR/hive_ddl.log"
else
    warn "'hive' command not found on PATH — skipping DDL."
fi

# -----------------------------------------------------------------------
# 7. Launch producer
# -----------------------------------------------------------------------
echo "[7/8] Launching producer..."

if [ -f "$PRODUCER_PID_FILE" ] && kill -0 "$(cat "$PRODUCER_PID_FILE")" 2>/dev/null; then
    info "Producer already running (PID $(cat "$PRODUCER_PID_FILE"))."
else
    [ -f "$PRODUCER_SCRIPT" ] || fail "Producer script not found at $PRODUCER_SCRIPT"
    nohup python3 "$PRODUCER_SCRIPT" >> "$PRODUCER_LOG" 2>&1 &
    echo $! > "$PRODUCER_PID_FILE"
    info "Producer started (PID $(cat "$PRODUCER_PID_FILE")). Log: $PRODUCER_LOG"
fi

# -----------------------------------------------------------------------
# 8. Launch Spark Structured Streaming job
# -----------------------------------------------------------------------
echo "[8/8] Launching Spark streaming job..."

if [ -f "$SPARK_PID_FILE" ] && kill -0 "$(cat "$SPARK_PID_FILE")" 2>/dev/null; then
    info "Spark job already running (PID $(cat "$SPARK_PID_FILE"))."
else
    [ -f "$SPARK_SCRIPT" ] || fail "Spark script not found at $SPARK_SCRIPT"
    nohup spark-submit \
        --packages "org.apache.spark:spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}:${SPARK_VERSION}" \
        "$SPARK_SCRIPT" >> "$SPARK_LOG" 2>&1 &
    echo $! > "$SPARK_PID_FILE"
    info "Spark job started (PID $(cat "$SPARK_PID_FILE")). Log: $SPARK_LOG"
fi

echo ""
echo "=== Pipeline started. ==="
echo "Tail logs with:"
echo "  tail -f $PRODUCER_LOG"
echo "  tail -f $SPARK_LOG"
echo "Stop everything with: ./stop_pipeline.sh"
