from pyspark.sql.functions import (
    col,
    unix_timestamp,
    when,
)

from streaming.spark_session import create_spark_session


PATIENT_STATE_PATH = "./data/gold/patient_state"
PATIENT_METRICS_PATH = "./data/gold/patient_metrics"


QUEUE_SLA_MINUTES = 15.0


def main():
    spark = create_spark_session(
        "HospitalPatientMetrics"
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    patient_state = (
        spark.read
        .format("delta")
        .load(PATIENT_STATE_PATH)
    )

    complete_patients = (
        patient_state
        .filter(
            col("arrival_time").isNotNull()
            & col("queue_entry_time").isNotNull()
            & col("service_start_time").isNotNull()
            & col("service_end_time").isNotNull()
            & col("departure_time").isNotNull()
        )
    )

    patient_metrics = (
        complete_patients

        .withColumn(
            "registration_minutes",
            (
                unix_timestamp(
                    col("registration_completed_time")
                )
                - unix_timestamp(
                    col("arrival_time")
                )
            ) / 60.0,
        )

        .withColumn(
            "queue_wait_minutes",
            (
                unix_timestamp(
                    col("service_start_time")
                )
                - unix_timestamp(
                    col("queue_entry_time")
                )
            ) / 60.0,
        )

        .withColumn(
            "service_minutes",
            (
                unix_timestamp(
                    col("service_end_time")
                )
                - unix_timestamp(
                    col("service_start_time")
                )
            ) / 60.0,
        )

        .withColumn(
            "total_journey_minutes",
            (
                unix_timestamp(
                    col("departure_time")
                )
                - unix_timestamp(
                    col("arrival_time")
                )
            ) / 60.0,
        )

        .withColumn(
            "sla_breached",
            col("queue_wait_minutes")
            > QUEUE_SLA_MINUTES,
        )

        .withColumn(
            "waited",
            col("queue_wait_minutes") > 0,
        )

        .select(
            "patient_id",
            "lab_id",
            "patient_type",
            "priority",
            "staff_id",

            "arrival_time",
            "registration_completed_time",
            "queue_entry_time",
            "service_start_time",
            "service_end_time",
            "departure_time",

            "registration_minutes",
            "queue_wait_minutes",
            "service_minutes",
            "total_journey_minutes",

            "waited",
            "sla_breached",
        )
    )

    (
        patient_metrics
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .save(
            PATIENT_METRICS_PATH
        )
    )

    print("=" * 80)
    print("PATIENT METRICS CREATED")
    print("=" * 80)

    print(
        f"Patients written: "
        f"{patient_metrics.count()}"
    )

    spark.stop()


if __name__ == "__main__":
    main()