from pyspark.sql.functions import (
    col,
    max as spark_max,
    sum as spark_sum,
)

from streaming.spark_session import create_spark_session


PATH = "./data/gold/operational_metrics_5m"


def main():
    spark = create_spark_session(
        "ValidateGoldMetrics"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    df = (
        spark.read
        .format("delta")
        .load(PATH)
    )

    totals = (
        df
        .agg(
            spark_sum(
                "patients_arrived"
            ).alias(
                "total_patients"
            ),

            spark_sum(
                col("avg_queue_wait_minutes")
                * col("patients_arrived")
            ).alias(
                "weighted_wait_sum"
            ),

            spark_sum(
                col("avg_service_minutes")
                * col("patients_arrived")
            ).alias(
                "weighted_service_sum"
            ),

            spark_sum(
                col("avg_total_journey_minutes")
                * col("patients_arrived")
            ).alias(
                "weighted_journey_sum"
            ),

            spark_sum(
                "sla_breaches"
            ).alias(
                "total_sla_breaches"
            ),

            spark_max(
                "max_queue_wait_minutes"
            ).alias(
                "max_queue_wait"
            ),
        )
        .collect()[0]
    )

    total_patients = totals["total_patients"]

    avg_wait = (
        totals["weighted_wait_sum"]
        / total_patients
    )

    avg_service = (
        totals["weighted_service_sum"]
        / total_patients
    )

    avg_journey = (
        totals["weighted_journey_sum"]
        / total_patients
    )

    print("=" * 80)
    print("STREAMING GOLD VALIDATION SUMMARY")
    print("=" * 80)

    print(
        f"Total patients:       "
        f"{total_patients}"
    )

    print(
        f"Average queue wait:   "
        f"{avg_wait:.2f} minutes"
    )

    print(
        f"Maximum queue wait:   "
        f"{totals['max_queue_wait']:.2f} minutes"
    )

    print(
        f"Average service time: "
        f"{avg_service:.2f} minutes"
    )

    print(
        f"Average journey time: "
        f"{avg_journey:.2f} minutes"
    )

    print(
        f"SLA breaches:         "
        f"{totals['total_sla_breaches']}"
    )

    spark.stop()


if __name__ == "__main__":
    main()