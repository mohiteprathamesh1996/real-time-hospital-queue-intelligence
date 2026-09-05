from pyspark.sql.functions import (
    avg,
    col,
    sum as spark_sum,
)

from streaming.spark_session import (
    create_spark_session,
)


PATH = (
    "./data/gold/"
    "stateful_intervention_backtest"
)

WINDOW_MINUTES = 5


def main():

    spark = create_spark_session(
        "InspectStatefulInterventions"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    df = (
        spark.read
        .format("delta")
        .load(PATH)
    )

    print("=" * 110)
    print(
        "STATEFUL INTERVENTION TIMELINE"
    )
    print("=" * 110)

    (
        df
        .orderBy("timestamp")
        .show(
            200,
            truncate=False,
        )
    )

    print()

    print("=" * 110)
    print("ACTION DISTRIBUTION")
    print("=" * 110)

    (
        df
        .groupBy(
            "policy_action"
        )
        .count()
        .orderBy(
            "policy_action"
        )
        .show(
            truncate=False
        )
    )

    print()

    print("=" * 110)
    print("RESOURCE CONSUMPTION")
    print("=" * 110)

    summary = (
        df
        .agg(
            spark_sum(
                "active_additional_staff"
            ).alias(
                "staff_window_units"
            ),

            avg(
                "active_additional_staff"
            ).alias(
                "avg_incremental_staff"
            ),
        )
        .collect()[0]
    )

    staff_window_units = (
        summary[
            "staff_window_units"
        ]
        or 0
    )

    staff_minutes = (
        staff_window_units
        * WINDOW_MINUTES
    )

    staff_hours = (
        staff_minutes
        / 60
    )

    print(
        f"Staff-window units:      "
        f"{staff_window_units}"
    )

    print(
        f"Incremental staff-min:   "
        f"{staff_minutes}"
    )

    print(
        f"Incremental staff-hours: "
        f"{staff_hours:.2f}"
    )

    print(
        f"Average extra staff:     "
        f"{summary['avg_incremental_staff']:.2f}"
    )

    spark.stop()


if __name__ == "__main__":
    main()