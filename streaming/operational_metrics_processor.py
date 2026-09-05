from pyspark.sql.functions import (
    avg,
    col,
    count,
    expr,
    max as spark_max,
    percentile_approx,
    sum as spark_sum,
    when,
    window,
)

from streaming.spark_session import create_spark_session


PATIENT_METRICS_PATH = "./data/gold/patient_metrics"
OPERATIONAL_METRICS_PATH = "./data/gold/operational_metrics_5m"

WINDOW_DURATION = "5 minutes"
QUEUE_SLA_MINUTES = 15.0


def main():
    spark = create_spark_session(
        "HospitalOperationalMetrics"
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    patient_metrics = (
        spark.read
        .format("delta")
        .load(PATIENT_METRICS_PATH)
    )

    metrics = (
        patient_metrics
        .groupBy(
            window(
                col("arrival_time"),
                WINDOW_DURATION,
            ),
            col("lab_id"),
        )
        .agg(
            count("*").alias(
                "patients_arrived"
            ),

            avg(
                "queue_wait_minutes"
            ).alias(
                "avg_queue_wait_minutes"
            ),

            percentile_approx(
                "queue_wait_minutes",
                0.95,
            ).alias(
                "p95_queue_wait_minutes"
            ),

            spark_max(
                "queue_wait_minutes"
            ).alias(
                "max_queue_wait_minutes"
            ),

            avg(
                "registration_minutes"
            ).alias(
                "avg_registration_minutes"
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
                    col("waited"),
                    1,
                ).otherwise(0)
            ).alias(
                "patients_who_waited"
            ),

            spark_sum(
                when(
                    col("sla_breached"),
                    1,
                ).otherwise(0)
            ).alias(
                "sla_breaches"
            ),
        )
        .withColumn(
            "sla_breach_rate",
            (
                col("sla_breaches")
                / col("patients_arrived")
            ) * 100,
        )
        .withColumn(
            "sla_compliance_rate",
            100 - col("sla_breach_rate"),
        )
        .select(
            col("window.start").alias(
                "window_start"
            ),
            col("window.end").alias(
                "window_end"
            ),
            col("lab_id"),
            col("patients_arrived"),
            col("patients_who_waited"),
            col("avg_queue_wait_minutes"),
            col("p95_queue_wait_minutes"),
            col("max_queue_wait_minutes"),
            col("avg_registration_minutes"),
            col("avg_service_minutes"),
            col("avg_total_journey_minutes"),
            col("sla_breaches"),
            col("sla_breach_rate"),
            col("sla_compliance_rate"),
        )
    )

    (
        metrics
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .save(
            OPERATIONAL_METRICS_PATH
        )
    )

    print("=" * 80)
    print("5-MINUTE OPERATIONAL METRICS CREATED")
    print("=" * 80)

    print(
        f"Windows written: "
        f"{metrics.count()}"
    )

    spark.stop()


if __name__ == "__main__":
    main()