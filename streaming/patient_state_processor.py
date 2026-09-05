from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    max as spark_max,
    when,
)
from pyspark.sql.types import StructType

from streaming.spark_session import create_spark_session


SILVER_PATH = "./data/silver/hospital_events"
PATIENT_STATE_PATH = "./data/gold/patient_state"
CHECKPOINT_PATH = "./checkpoints/patient_state"


def aggregate_patient_events(batch_df):
    return (
        batch_df
        .groupBy(
            "patient_id",
            "lab_id",
        )
        .agg(
            spark_max(
                when(
                    col("event_type") == "PATIENT_ARRIVAL",
                    col("event_time"),
                )
            ).alias("arrival_time"),

            spark_max(
                when(
                    col("event_type") == "REGISTRATION_COMPLETED",
                    col("event_time"),
                )
            ).alias("registration_completed_time"),

            spark_max(
                when(
                    col("event_type") == "QUEUE_ENTERED",
                    col("event_time"),
                )
            ).alias("queue_entry_time"),

            spark_max(
                when(
                    col("event_type") == "SERVICE_STARTED",
                    col("event_time"),
                )
            ).alias("service_start_time"),

            spark_max(
                when(
                    col("event_type") == "SERVICE_COMPLETED",
                    col("event_time"),
                )
            ).alias("service_end_time"),

            spark_max(
                when(
                    col("event_type") == "PATIENT_DEPARTED",
                    col("event_time"),
                )
            ).alias("departure_time"),

            spark_max("patient_type").alias(
                "patient_type"
            ),

            spark_max("priority").alias(
                "priority"
            ),

            spark_max("staff_id").alias(
                "staff_id"
            ),
        )
    )


def upsert_patient_state(
    batch_df,
    batch_id,
):
    if batch_df.isEmpty():
        return

    updates = aggregate_patient_events(
        batch_df
    )

    spark = batch_df.sparkSession

    if not DeltaTable.isDeltaTable(
        spark,
        PATIENT_STATE_PATH,
    ):
        (
            updates
            .write
            .format("delta")
            .mode("overwrite")
            .save(PATIENT_STATE_PATH)
        )

        return

    target = DeltaTable.forPath(
        spark,
        PATIENT_STATE_PATH,
    )

    (
        target.alias("target")
        .merge(
            updates.alias("source"),
            """
            target.patient_id = source.patient_id
            AND target.lab_id = source.lab_id
            """,
        )
        .whenMatchedUpdate(
            set={
                "arrival_time":
                    "coalesce(source.arrival_time, target.arrival_time)",

                "registration_completed_time":
                    """
                    coalesce(
                        source.registration_completed_time,
                        target.registration_completed_time
                    )
                    """,

                "queue_entry_time":
                    """
                    coalesce(
                        source.queue_entry_time,
                        target.queue_entry_time
                    )
                    """,

                "service_start_time":
                    """
                    coalesce(
                        source.service_start_time,
                        target.service_start_time
                    )
                    """,

                "service_end_time":
                    """
                    coalesce(
                        source.service_end_time,
                        target.service_end_time
                    )
                    """,

                "departure_time":
                    """
                    coalesce(
                        source.departure_time,
                        target.departure_time
                    )
                    """,

                "patient_type":
                    """
                    coalesce(
                        source.patient_type,
                        target.patient_type
                    )
                    """,

                "priority":
                    """
                    coalesce(
                        source.priority,
                        target.priority
                    )
                    """,

                "staff_id":
                    """
                    coalesce(
                        source.staff_id,
                        target.staff_id
                    )
                    """,
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    spark = create_spark_session(
        "HospitalPatientState"
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    silver = (
        spark.readStream
        .format("delta")
        .load(SILVER_PATH)
    )

    query = (
        silver
        .writeStream
        .foreachBatch(
            upsert_patient_state
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH,
        )
        .start()
    )

    print("=" * 80)
    print("PATIENT STATE PROCESSOR")
    print("=" * 80)

    print(f"Silver source: {SILVER_PATH}")
    print(f"Patient state: {PATIENT_STATE_PATH}")
    print()
    print("Processing patient lifecycles...")
    print("Press Ctrl+C to stop.")

    query.awaitTermination()


if __name__ == "__main__":
    main()