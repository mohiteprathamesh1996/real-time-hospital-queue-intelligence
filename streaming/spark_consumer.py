import os
from pyspark.sql import SparkSession
from streaming.spark_session import create_spark_session
from pyspark.sql.functions import col, current_timestamp
from delta import configure_spark_with_delta_pip

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "hospital-events"

BRONZE_PATH = "./data/bronze/hospital_events"
CHECKPOINT_PATH = "./checkpoints/bronze_hospital_events"


# def create_spark_session() -> SparkSession:
#     return (
#         SparkSession.builder
#         .appName("HospitalQueueBronze")
#         .master("local[*]")
#         .config(
#             "spark.sql.extensions",
#             "io.delta.sql.DeltaSparkSessionExtension",
#         )
#         .config(
#             "spark.sql.catalog.spark_catalog",
#             "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#         )
#         .config(
#             "spark.jars.packages",
#             ",".join([
#                 "io.delta:delta-spark_4.2_2.13:4.4.0",
#                 "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
#             ]),
#         )
#         .getOrCreate()
#     )


def main() -> None:
    spark = create_spark_session("HospitalQueueBronze")
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("HOSPITAL QUEUE — KAFKA → BRONZE")
    print("=" * 80)

    kafka_events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze_events = (
        kafka_events
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("event_key"),
            col("value").cast("string").alias("event_json"),
            current_timestamp().alias("bronze_ingestion_timestamp"),
        )
    )

    query = (
        bronze_events
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(BRONZE_PATH)
    )

    print(f"Kafka topic:       {KAFKA_TOPIC}")
    print(f"Bronze path:       {BRONZE_PATH}")
    print(f"Checkpoint path:   {CHECKPOINT_PATH}")
    print()
    print("Streaming to Bronze...")
    print("Press Ctrl+C to stop.")
    print("=" * 80)

    query.awaitTermination()


if __name__ == "__main__":
    main()