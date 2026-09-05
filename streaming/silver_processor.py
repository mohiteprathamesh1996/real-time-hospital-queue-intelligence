from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    when,
    unix_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

from streaming.spark_session import create_spark_session


BRONZE_PATH = "./data/bronze/hospital_events"
SILVER_PATH = "./data/silver/hospital_events"
QUARANTINE_PATH = "./data/quarantine/hospital_events"

SILVER_CHECKPOINT_PATH = "./checkpoints/silver_hospital_events"
QUARANTINE_CHECKPOINT_PATH = "./checkpoints/quarantine_hospital_events"


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("lab_id", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("ingestion_time", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("patient_type", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("appointment_id", StringType(), True),
        StructField("staff_id", StringType(), True),
    ]
)


def main() -> None:
    spark = create_spark_session(
        "HospitalQueueSilver"
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("HOSPITAL QUEUE — BRONZE → SILVER")
    print("=" * 80)

    # ------------------------------------------------------------------
    # Read Bronze Delta as a streaming source
    # ------------------------------------------------------------------

    bronze = (
        spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
    )

    # ------------------------------------------------------------------
    # Parse raw JSON
    # ------------------------------------------------------------------

    parsed = (
        bronze
        .withColumn(
            "event",
            from_json(
                col("event_json"),
                EVENT_SCHEMA,
            ),
        )
    )

    # ------------------------------------------------------------------
    # Identify malformed / invalid events
    # ------------------------------------------------------------------

    validated = (
        parsed
        .withColumn(
            "quarantine_reason",
            when(
                col("event").isNull(),
                "MALFORMED_JSON",
            )
            .when(
                col("event.event_id").isNull(),
                "MISSING_EVENT_ID",
            )
            .when(
                col("event.event_type").isNull(),
                "MISSING_EVENT_TYPE",
            )
            .when(
                col("event.patient_id").isNull(),
                "MISSING_PATIENT_ID",
            )
            .when(
                col("event.lab_id").isNull(),
                "MISSING_LAB_ID",
            )
            .when(
                col("event.event_time").isNull(),
                "MISSING_EVENT_TIME",
            ),
        )
    )

    # ------------------------------------------------------------------
    # Quarantine invalid rows
    # ------------------------------------------------------------------

    quarantine = (
        validated
        .filter(
            col("quarantine_reason").isNotNull()
        )
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "event_key",
            "event_json",
            "bronze_ingestion_timestamp",
            "quarantine_reason",
            current_timestamp().alias(
                "quarantine_timestamp"
            ),
        )
    )

    # ------------------------------------------------------------------
    # Valid records
    # ------------------------------------------------------------------

    valid = (
        validated
        .filter(
            col("quarantine_reason").isNull()
        )
        .select(
            col("event.event_id").alias("event_id"),
            col("event.event_type").alias("event_type"),
            col("event.patient_id").alias("patient_id"),
            col("event.lab_id").alias("lab_id"),
            col("event.event_time").alias("event_time"),
            col("event.ingestion_time").alias("ingestion_time"),
            col("event.source").alias("source"),
            col("event.schema_version").alias("schema_version"),
            col("event.patient_type").alias("patient_type"),
            col("event.priority").alias("priority"),
            col("event.appointment_id").alias("appointment_id"),
            col("event.staff_id").alias("staff_id"),
            col("kafka_timestamp"),
            col("bronze_ingestion_timestamp"),
            col("event_key"),
            col("topic"),
            col("partition"),
            col("offset"),
        )
    )

    # ------------------------------------------------------------------
    # Add event latency
    # ------------------------------------------------------------------

    enriched = (
        valid
        .withColumn(
            "event_lag_seconds",
            unix_timestamp(
                col("bronze_ingestion_timestamp")
            )
            - unix_timestamp(
                col("event_time")
            ),
        )
        .withColumn(
            "is_late",
            col("event_lag_seconds") > 300,
        )
    )

    # ------------------------------------------------------------------
    # Deduplicate
    #
    # Watermark is required for stateful streaming deduplication.
    # ------------------------------------------------------------------

    silver = (
        enriched
        .withWatermark(
            "event_time",
            "30 minutes",
        )
        .dropDuplicates(
            ["event_id"]
        )
    )

    # ------------------------------------------------------------------
    # Silver writer
    # ------------------------------------------------------------------

    silver_query = (
        silver
        .writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            SILVER_CHECKPOINT_PATH,
        )
        .start(SILVER_PATH)
    )

    # ------------------------------------------------------------------
    # Quarantine writer
    # ------------------------------------------------------------------

    quarantine_query = (
        quarantine
        .writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            QUARANTINE_CHECKPOINT_PATH,
        )
        .start(QUARANTINE_PATH)
    )

    print(f"Bronze source:      {BRONZE_PATH}")
    print(f"Silver path:        {SILVER_PATH}")
    print(f"Quarantine path:    {QUARANTINE_PATH}")
    print()
    print("Silver processing started...")
    print("Press Ctrl+C to stop.")
    print("=" * 80)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()