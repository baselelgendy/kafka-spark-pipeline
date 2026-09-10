# ShopPulse Global — Real-Time Order Ingestion Pipeline

**Samsung Innovation Campus — Data Platform Engineering Task**

A fault-tolerant, real-time ingestion pipeline that replaces a 12-hour-latency
nightly batch job with a continuous stream: **Kafka → Spark Structured
Streaming → HDFS (Parquet) → Hive (external table)**.

---

## 1. Architecture Overview

```
 ┌──────────────┐      ┌───────────┐      ┌────────────────────┐      ┌──────────┐      ┌──────┐
 │ generate_     │ JSON │           │      │ Spark Structured    │ Parquet│          │ SQL  │      │
 │ orders.py     ├─────►│  Kafka    ├─────►│ Streaming            ├───────►│  HDFS    ├─────►│ Hive │
 │ (Producer)    │ msgs │ topic1_   │      │ (spark_streaming.py) │ files  │          │ table│      │
 └──────────────┘      │ logs      │      └────────────────────┘      └──────────┘      └──────┘
                        └───────────┘
```

1. **`generate_orders.py`** continuously simulates live order transactions
   and publishes them as JSON to the Kafka topic `topic1_logs`.
2. **`spark_streaming.py`** consumes `topic1_logs`, parses the JSON payload,
   converts `order_time` to a native timestamp, computes the derived metric
   `total_amount = quantity * price`, and writes the enriched records to
   HDFS as Parquet in append mode (10-second micro-batch trigger).
3. **`create_table.sql`** defines a Hive **external table** on top of the
   HDFS Parquet location, plus six analytical queries for reporting.
4. **`start_pipeline.sh` / `stop_pipeline.sh`** automate bringing the whole
   stack up and down, with health checks and graceful shutdown.

### Message schema (Kafka → Spark)

| Field | Type | Rule |
|---|---|---|
| `order_id` | Integer | Auto-increment, starts at 1001 |
| `customer_id` | Integer | Random, 100–500 |
| `product_id` | Integer | Random, 1–50 |
| `quantity` | Integer | Random, 1–10 |
| `price` | Double | Random, $5.00–$500.00 |
| `order_time` | String → Timestamp | UTC, `YYYY-MM-DD HH:MM:SS` |
| `total_amount` | Double (derived) | `quantity * price`, added by Spark |

---

## 2. Directory Structure

```
Kafka_Spark_Pipline/
├── Producer/
│   └── generate_orders.py       # Kafka producer (Task 1)
├── Spark/
│   └── spark_streaming.py       # Spark Structured Streaming job (Task 2)
├── hive/
│   └── create_table.sql         # External table DDL + 6 analytical queries (Task 3)
├── scripts/
│   ├── start_pipeline.sh        # Brings up HDFS, YARN, Hive, Kafka topic, then the pipeline
│   └── stop_pipeline.sh         # Gracefully stops producer + Spark job
├── run/                         # PID files (created automatically)
├── logs/                        # Producer / Spark / Hive logs (created automatically)
└── README.md
```

---

## 3. Prerequisites

| Component | Notes |
|---|---|
| Hadoop (HDFS + YARN) | NameNode, DataNode, ResourceManager, NodeManager |
| Apache Kafka | Broker reachable at `localhost:9092` |
| Apache Spark 3.1.2 | `spark-submit` on PATH |
| Apache Hive | `hive` CLI, metastore, HiveServer2 |
| Python 3 | `kafka-python` installed (`pip3 install kafka-python`) |

---

## 4. Setup Instructions

### 4.1 Install the Python Kafka client
```bash
pip3 install kafka-python
```

### 4.2 Create the Kafka topic (if not already created by `start_pipeline.sh`)
```bash
kafka-topics.sh --create \
  --topic topic1_logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Verify:
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 4.3 Fix common HDFS permission issues (one-time, if needed)
If `NameNode`/`DataNode` fail to start with a "Directory is not readable"
error, the storage directories are owned by the wrong user:
```bash
sudo chown -R student:student /home/hadoop/hadoopdata/hdfs/namenode
sudo chown -R student:student /home/hadoop/hadoopdata/hdfs/datanode
sudo chmod -R 755 /home/hadoop/hadoopdata/hdfs/namenode
sudo chmod -R 755 /home/hadoop/hadoopdata/hdfs/datanode
```

---

## 5. Deployment Guide

### 5.1 Start everything
```bash
chmod +x scripts/start_pipeline.sh scripts/stop_pipeline.sh
./scripts/start_pipeline.sh
```

`start_pipeline.sh` performs, in order:
1. Starts HDFS (NameNode/DataNode) and leaves Safe Mode automatically.
2. Starts YARN (ResourceManager/NodeManager).
3. Starts the Hive metastore and HiveServer2 (if not already running).
4. Verifies the Kafka broker and creates `topic1_logs` if it doesn't exist.
5. Creates the required HDFS directories:
   - `/user/hive/warehouse/ecommerce_dw.db/streaming_orders`
   - `/user/spark/checkpoints/streaming_orders`
6. Runs `hive/create_table.sql` to create the external table and register
   the analytical queries.
7. Launches `generate_orders.py` and `spark_streaming.py` as supervised
   background processes, saving their PIDs under `run/`.

### 5.2 Stop everything
```bash
./scripts/stop_pipeline.sh
```
Gracefully terminates the producer and Spark job via their PIDs (falling
back to a name-based search if a PID file is missing) — **HDFS Parquet
data, Spark checkpoints, and Kafka offsets are left untouched**, so the
pipeline resumes cleanly on the next `start_pipeline.sh` run.

---

## 6. Verification

| Check | Command |
|---|---|
| Hadoop/YARN/Hive processes | `jps` |
| Kafka messages arriving | `kafka-console-consumer.sh --topic topic1_logs --bootstrap-server localhost:9092 --from-beginning` |
| Producer logs | `tail -f logs/producer.log` |
| Spark job logs | `tail -f logs/spark_streaming.log` |
| Parquet files landing on HDFS | `hdfs dfs -ls /user/hive/warehouse/ecommerce_dw.db/streaming_orders` |
| Hive table has data | `hive -e "SELECT COUNT(*) FROM ecommerce_dw.streaming_orders;"` |
| YARN application view (Web UI) | `http://localhost:8088` |
| NameNode file browser (Web UI) | `http://localhost:9870` |
| Hue table browser | `http://localhost:8888` |

## 6.1 Failure Recovery Verification

- **Producer restart:** stop and restart `generate_orders.py` — Spark
  resumes consuming new messages seamlessly (Kafka retains committed
  offsets, no restart needed on the Spark side).
- **Spark restart:** stop and restart `spark_streaming.py` — the job
  resumes from the last committed offset stored in its checkpoint
  location (`/user/spark/checkpoints/streaming_orders`), without dropping
  messages or corrupting existing Parquet part-files.

---

## 7. Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'kafka'` | Missing Python client | `pip3 install kafka-python` |
| NameNode/DataNode won't start, "Directory is not readable" | Wrong ownership on HDFS data dirs | See §4.3 |
| `start-dfs.sh` says `Cannot set priority of datanode process` | Stale PID file / orphaned process | `pkill -9 -f datanode && sudo rm -f /tmp/hadoop-*-datanode.pid`, then retry |
| `hdfs dfs` commands hang or fail | HDFS still in Safe Mode after startup | `hdfs dfsadmin -safemode leave` |
| `beeline` hangs for a long time | Beeline opened an embedded Hive session instead of connecting to the running HiveServer2 | Use `hive -f script.sql` instead, or `beeline -u "jdbc:hive2://localhost:10000/"` |
| `User: student is not allowed to impersonate student` | Hive `doAs` impersonation misconfigured | Prefer `hive -f` over `beeline` for DDL execution |
| MapReduce job hangs retrying `ResourceManager at 0.0.0.0:8032` | YARN not running | `start-yarn.sh`, then verify `ResourceManager`/`NodeManager` in `jps` |
| YARN UI (`:8088`) shows no running application | `spark-submit` was run without `--master yarn` (local mode) | Expected — the job still runs and writes to HDFS correctly; check `logs/spark_streaming.log` and HDFS output instead |

---

## 8. Deliverables Checklist

- [x] `producer/generate_orders.py`
- [x] `spark/spark_streaming.py`
- [x] `hive/create_table.sql` (external table + 6 analytical queries)
- [x] `scripts/start_pipeline.sh`
- [x] `scripts/stop_pipeline.sh`
- [x] `README.md`
- [ ] Verification evidence package (Kafka console stream, HDFS listing,
      Hive query outputs, checkpoint-recovery test screenshots)
=======
# kafka-spark-pipeline
>>>>>>> cd6304bf557d27331280b3369e35c2d5d8d678a1
