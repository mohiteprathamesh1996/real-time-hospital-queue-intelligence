from pyspark.sql.functions import col

from streaming.spark_session import create_spark_session


PATH = "./data/gold/operational_state_5m"


def main():
    spark = create_spark_session(
        "InspectOperationalState"
    )

    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .format("delta")
        .load(PATH)
    )

    print("=" * 130)
    print("HOSPITAL OPERATIONAL STATE")
    print("=" * 130)

    print(f"Windows: {df.count()}")
    print()

    (
        df
        .select(
            "window_start",
            "patients_arrived",
            "patients_waiting",
            "patients_in_service",
            "utilization_percentage",
            "avg_queue_wait_minutes",
            "p95_queue_wait_minutes",
            "sla_compliance_rate",
            "queue_pressure",
            "operational_status",
        )
        .orderBy("window_start")
        .show(
            200,
            truncate=False,
        )
    )

    print()
    print("=" * 130)
    print("NON-GREEN WINDOWS")
    print("=" * 130)

    (
        df
        .filter(
            col("operational_status") != "GREEN"
        )
        .select(
            "window_start",
            "patients_waiting",
            "utilization_percentage",
            "p95_queue_wait_minutes",
            "sla_compliance_rate",
            "queue_pressure",
            "operational_status",
        )
        .orderBy("window_start")
        .show(
            200,
            truncate=False,
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()