from pyspark.sql.functions import (
    avg,
    col,
    count,
    sum as spark_sum,
    when,
    window,
)

from streaming.spark_session import create_spark_session


SILVER_PATH = "./data/silver/hospital_events"

GOLD_PATH = "./data/gold/operational_metrics"
GOLD_CHECKPOINT_PATH = "./checkpoints/gold_operational_metrics"


def main():
    spark = create_spark_session(
        "HospitalQueueGold"
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("HOSPITAL QUEUE — SILVER → GOLD")
    print("=" * 80)

    silver = (
        spark.readStream
        .format("delta")
        .load(SILVER_PATH)
    )

    metrics = (
        silver
        .withWatermark(
            "event_time",
            "30 minutes",
        )
        .groupBy(
            window(
                col("event_time"),
                "5 minutes",
            ),
            col("lab_id"),
        )
        .agg(
            count(
                when(
                    col("event_type") == "PATIENT_ARRIVAL",
                    True,
                )
            ).alias("arrivals"),

            count(
                when(
                    col("event_type") == "SERVICE_STARTED",
                    True,
                )
            ).alias("services_started"),

            count(
                when(
                    col("event_type") == "SERVICE_COMPLETED",
                    True,
                )
            ).alias("services_completed"),

            count(
                when(
                    col("event_type") == "PATIENT_DEPARTED",
                    True,
                )
            ).alias("departures"),

            avg(
                "event_lag_seconds"
            ).alias(
                "avg_event_lag_seconds"
            ),

            spark_sum(
                when(
                    col("is_late") == True,
                    1,
                ).otherwise(0)
            ).alias(
                "late_event_count"
            ),
        )
        .select(
            col("window.start").alias(
                "window_start"
            ),
            col("window.end").alias(
                "window_end"
            ),
            col("lab_id"),
            col("arrivals"),
            col("services_started"),
            col("services_completed"),
            col("departures"),
            col("avg_event_lag_seconds"),
            col("late_event_count"),
        )
    )

    query = (
        metrics
        .writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            GOLD_CHECKPOINT_PATH,
        )
        .start(
            GOLD_PATH
        )
    )

    print(f"Silver source:      {SILVER_PATH}")
    print(f"Gold path:          {GOLD_PATH}")
    print()
    print("Gold processing started...")
    print("Press Ctrl+C to stop.")
    print("=" * 80)

    query.awaitTermination()


if __name__ == "__main__":
    main()