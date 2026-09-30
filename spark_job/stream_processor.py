"""
Spark Structured Streaming job.

Reads raw clickstream JSON events from Kafka, applies a watermark + 5-minute
tumbling window aggregation (event count and unique users per product/event
type), and writes each micro-batch to Postgres via foreachBatch (JDBC).

Run inside the spark container (local[*] mode), e.g.:

    docker exec -it spark /opt/spark/bin/spark-submit \
        --master "local[*]" \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
        /opt/spark-job/stream_processor.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, approx_count_distinct, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "clickstream-events"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/clickstream"
POSTGRES_PROPERTIES = {
    "user": "de_user",
    "password": "de_pass",
    "driver": "org.postgresql.Driver",
}

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("device", StringType()),
    StructField("country", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("quantity", IntegerType()),
])


def write_batch_to_postgres(batch_df, batch_id: int):
    """foreachBatch sink: upsert-style write of one micro-batch to Postgres."""
    if batch_df.rdd.isEmpty():
        return

    row_count = batch_df.count()
    print(f"[batch {batch_id}] writing {row_count} aggregated rows to Postgres")

    (
        batch_df.write
        .mode("append")
        .jdbc(url=POSTGRES_URL, table="product_activity_5min", properties=POSTGRES_PROPERTIES)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("ClickstreamWindowedAggregation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_timestamp")))
    )

    # Late events beyond 2 minutes past the watermark are dropped from the window
    windowed = (
        events
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("product_id"),
            col("event_type"),
        )
        .agg(
            count("*").alias("event_count"),
            approx_count_distinct("user_id").alias("unique_users"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("event_type"),
            col("event_count"),
            col("unique_users"),
        )
    )

    query = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", "/opt/spark-job/checkpoints/clickstream_agg")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
