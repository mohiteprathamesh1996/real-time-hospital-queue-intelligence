from pyspark.sql.functions import col

from streaming.spark_session import create_spark_session


PATH = "./data/gold/staffing_decisions_5m"


def main():
    spark = create_spark_session(
        "InspectStaffingDecisions"
    )

    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .format("delta")
        .load(PATH)
    )

    print("=" * 110)
    print("STAFFING DECISION SUMMARY")
    print("=" * 110)

    print(f"Total windows: {df.count()}")

    intervention_count = (
        df
        .filter(col("intervention_required"))
        .count()
    )

    print(
        f"Intervention windows: "
        f"{intervention_count}"
    )

    print()
    print("=" * 110)
    print("DECISION DISTRIBUTION")
    print("=" * 110)

    (
        df
        .groupBy(
            "severity",
            "additional_staff",
        )
        .count()
        .orderBy(
            "additional_staff",
            "severity",
        )
        .show(
            truncate=False
        )
    )

    print()
    print("=" * 110)
    print("INTERVENTION WINDOWS")
    print("=" * 110)

    (
        df
        .filter(
            col("intervention_required")
        )
        .select(
            "timestamp",
            "lab_id",
            "severity",
            "additional_staff",
            "reason",
        )
        .orderBy("timestamp")
        .show(
            200,
            truncate=False,
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()