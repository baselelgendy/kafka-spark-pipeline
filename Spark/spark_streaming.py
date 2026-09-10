"""
spark/spark_streaming.py

Spark Structured Streaming application that:
  - Ingests raw order events from Kafka topic `topic1_logs`
  - Casts the binary Kafka `value` into a UTF-8 string and unpacks the JSON
    payload using an explicit schema
  - Parses `order_time` into a native Spark TimestampType
  - Computes the derived metric total_amount = quantity * price
  - Streams the enriched records to HDFS as Parquet in append mode

Matches Task 2 spec:
  - Data storage path:  /user/hive/warehouse/ecommerce_dw.db/streaming_orders
  - Checkpoint path:    /user/spark/checkpoints/streaming_orders
  - Trigger:            micro-batch interval of 10 seconds
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, round as spark_round
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    DecimalType,
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "topic1_logs"

HDFS_OUTPUT_PATH = "/user/hive/warehouse/ecommerce_dw.db/streaming_orders"
CHECKPOINT_PATH = "/user/spark/checkpoints/streaming_orders"

TRIGGER_INTERVAL = "10 seconds"


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("KafkaOrdersStreaming")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Define JSON Schema (matches producer payload)
# --------------------------------------------------

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_time", StringType(), True),
])


# --------------------------------------------------
# 3. Read from Kafka
# --------------------------------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


# --------------------------------------------------
# 4. Cast Kafka value to string and parse JSON
# --------------------------------------------------

orders = raw_stream.select(
    from_json(
        col("value").cast("string"),
        order_schema
    ).alias("order")
).select("order.*")


# --------------------------------------------------
# 5. Parse order_time into a native TimestampType
# --------------------------------------------------

orders = orders.withColumn(
    "order_time",
    to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss")
)


# --------------------------------------------------
# 6. Feature enrichment: total_amount = quantity * price
# --------------------------------------------------

orders = orders.withColumn(
    "total_amount",
    spark_round((col("quantity") * col("price")).cast(DecimalType(12, 2)), 2)
)


# --------------------------------------------------
# 7. Write stream to HDFS as Parquet (append mode)
# --------------------------------------------------

query = (
    orders.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", HDFS_OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

query.awaitTermination()
