from pyspark.sql.functions import (
    col,
    lit,
    when,
)

from streaming.spark_session import create_spark_session


OPERATIONAL_METRICS_PATH = "./data/gold/operational_metrics_5m"
QUEUE_METRICS_PATH = "./data/gold/queue_metrics_5m"

OUTPUT_PATH = "./data/gold/operational_state_5m"


def main():
    spark = create_spark_session(
        "HospitalOperationalState"
    )

    spark.sparkContext.setLogLevel("WARN")

    operational = (
        spark.read
        .format("delta")
        .load(OPERATIONAL_METRICS_PATH)
        .alias("o")
    )

    queue = (
        spark.read
        .format("delta")
        .load(QUEUE_METRICS_PATH)
        .alias("q")
    )

    state = (
        operational
        .join(
            queue,
            col("o.window_start")
            == col("q.timestamp"),
            "left",
        )
        .select(
            col("o.window_start"),
            col("o.window_end"),
            col("o.lab_id"),

            col("o.patients_arrived"),
            col("o.patients_who_waited"),

            col("o.avg_queue_wait_minutes"),
            col("o.p95_queue_wait_minutes"),
            col("o.max_queue_wait_minutes"),

            col("o.avg_registration_minutes"),
            col("o.avg_service_minutes"),
            col("o.avg_total_journey_minutes"),

            col("o.sla_breaches"),
            col("o.sla_breach_rate"),
            col("o.sla_compliance_rate"),

            col("q.patients_waiting"),
            col("q.patients_in_service"),
            col("q.utilization_percentage"),
        )
        .fillna(
            {
                "patients_waiting": 0,
                "patients_in_service": 0,
                "utilization_percentage": 0.0,
            }
        )
    )

    # -------------------------------------------------------------
    # Queue pressure
    # -------------------------------------------------------------

    state = (
        state
        .withColumn(
            "queue_pressure",
            when(
                col("patients_waiting") == 0,
                lit("NONE"),
            )
            .when(
                col("patients_waiting") <= 3,
                lit("LOW"),
            )
            .when(
                col("patients_waiting") <= 7,
                lit("MODERATE"),
            )
            .otherwise(
                lit("HIGH")
            ),
        )
    )

    # -------------------------------------------------------------
    # Operational status
    #
    # RED:
    #   serious queue or SLA deterioration
    #
    # AMBER:
    #   early warning / capacity pressure
    #
    # GREEN:
    #   normal operation
    # -------------------------------------------------------------

    state = (
        state
        .withColumn(
            "operational_status",
            when(
                (col("patients_waiting") >= 8)
                | (col("sla_compliance_rate") < 80),
                lit("RED"),
            )
            .when(
                (col("patients_waiting") >= 4)
                | (col("utilization_percentage") >= 100)
                | (col("sla_compliance_rate") < 95),
                lit("AMBER"),
            )
            .otherwise(
                lit("GREEN")
            ),
        )
    )

    (
        state
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .save(OUTPUT_PATH)
    )

    print("=" * 80)
    print("UNIFIED OPERATIONAL STATE CREATED")
    print("=" * 80)
    print(
        f"Rows written: {state.count()}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
    