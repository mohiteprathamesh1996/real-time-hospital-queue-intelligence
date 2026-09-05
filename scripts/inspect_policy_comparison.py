from pyspark.sql.functions import (
    avg,
    col,
    count,
    max as spark_max,
    sum as spark_sum,
)

from streaming.spark_session import (
    create_spark_session,
)


PATH = (
    "./data/gold/policy_comparison_stateful"
)

WINDOW_MINUTES = 5


def main():
    spark = create_spark_session(
        "InspectPolicyComparison"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    df = (
        spark.read
        .format("delta")
        .load(PATH)
    )

    print("=" * 120)
    print("STATEFUL POLICY COMPARISON")
    print("=" * 120)

    print()

    print("=" * 120)
    print("RESOURCE CONSUMPTION BY POLICY")
    print("=" * 120)

    resource_summary = (
        df
        .groupBy("policy")
        .agg(
            count("*").alias(
                "windows"
            ),

            spark_sum(
                "active_additional_staff"
            ).alias(
                "staff_window_units"
            ),

            avg(
                "active_additional_staff"
            ).alias(
                "avg_extra_staff"
            ),

            spark_max(
                "active_additional_staff"
            ).alias(
                "peak_extra_staff"
            ),
        )
        .orderBy("policy")
    )

    resource_summary.show(
        truncate=False
    )

    rows = (
        resource_summary
        .collect()
    )

    print()

    for row in rows:
        staff_windows = (
            row["staff_window_units"]
            or 0
        )

        staff_minutes = (
            staff_windows
            * WINDOW_MINUTES
        )

        staff_hours = (
            staff_minutes
            / 60.0
        )

        print(
            f"{row['policy']}:"
        )

        print(
            f"  Staff-window units: "
            f"{staff_windows}"
        )

        print(
            f"  Incremental minutes: "
            f"{staff_minutes}"
        )

        print(
            f"  Incremental hours:   "
            f"{staff_hours:.2f}"
        )

        print(
            f"  Peak extra staff:    "
            f"{row['peak_extra_staff']}"
        )

        print()

    print("=" * 120)
    print("ACTION DISTRIBUTION")
    print("=" * 120)

    (
        df
        .groupBy(
            "policy",
            "policy_action",
        )
        .count()
        .orderBy(
            "policy",
            "policy_action",
        )
        .show(
            truncate=False
        )
    )

    print()

    print("=" * 120)
    print("COUNTERFACTUAL POLICY TIMELINE")
    print("=" * 120)

    (
        df
        .filter(
            col("policy")
            == "COUNTERFACTUAL"
        )
        .filter(
            col("active_additional_staff")
            > 0
        )
        .select(
            "timestamp",
            "instantaneous_recommendation",
            "policy_action",
            "staff_change",
            "active_additional_staff",
            "counterfactual_decision",
            "objective",
        )
        .orderBy(
            "timestamp"
        )
        .show(
            200,
            truncate=False,
        )
    )

    print()

    print("=" * 120)
    print("POLICY RESOURCE SAVINGS")
    print("=" * 120)

    summary = {
        row["policy"]: (
            row["staff_window_units"]
            or 0
        )
        for row in rows
    }

    static_units = (
        summary.get(
            "STATIC_RULE",
            0,
        )
    )

    counterfactual_units = (
        summary.get(
            "COUNTERFACTUAL",
            0,
        )
    )

    saved_units = (
        static_units
        - counterfactual_units
    )

    static_hours = (
        static_units
        * WINDOW_MINUTES
        / 60.0
    )

    counterfactual_hours = (
        counterfactual_units
        * WINDOW_MINUTES
        / 60.0
    )

    saved_hours = (
        static_hours
        - counterfactual_hours
    )

    if static_units:
        reduction_percentage = (
            saved_units
            / static_units
            * 100
        )
    else:
        reduction_percentage = 0.0

    print(
        f"Static-rule staff-hours:       "
        f"{static_hours:.2f}"
    )

    print(
        f"Counterfactual staff-hours:    "
        f"{counterfactual_hours:.2f}"
    )

    print(
        f"Incremental staff-hours saved: "
        f"{saved_hours:.2f}"
    )

    print(
        f"Resource reduction:            "
        f"{reduction_percentage:.1f}%"
    )

    print()
    print(
        "These values represent simulated "
        "incremental staffing capacity under "
        "the same persistence policy."
    )

    spark.stop()


if __name__ == "__main__":
    main()