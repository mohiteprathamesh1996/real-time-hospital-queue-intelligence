from pyspark.sql.functions import (
    avg,
    max as spark_max,
    sum as spark_sum,
    when,
)

from streaming.spark_session import create_spark_session


PATIENT_METRICS_PATH = "./data/gold/patient_metrics"


def main():
    spark = create_spark_session(
        "InspectPatientMetrics"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    df = (
        spark.read
        .format("delta")
        .load(PATIENT_METRICS_PATH)
    )

    print("=" * 90)
    print("PATIENT METRICS")
    print("=" * 90)

    print(
        f"Patients: {df.count()}"
    )

    print()

    summary = (
        df
        .agg(
            avg(
                "registration_minutes"
            ).alias(
                "avg_registration_minutes"
            ),

            avg(
                "queue_wait_minutes"
            ).alias(
                "avg_queue_wait_minutes"
            ),

            spark_max(
                "queue_wait_minutes"
            ).alias(
                "max_queue_wait_minutes"
            ),

            avg(
                "service_minutes"
            ).alias(
                "avg_service_minutes"
            ),

            avg(
                "total_journey_minutes"
            ).alias(
                "avg_total_journey_minutes"
            ),

            spark_sum(
                when(
                    df.sla_breached,
                    1,
                ).otherwise(0)
            ).alias(
                "sla_breaches"
            ),
        )
    )

    summary.show(
        truncate=False
    )

    print()

    df.orderBy(
        "arrival_time"
    ).show(
        20,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()