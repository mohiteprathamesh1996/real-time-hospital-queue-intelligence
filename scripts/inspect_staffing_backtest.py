from pyspark.sql.functions import (
    avg,
    col,
    count,
    sum as spark_sum,
)

from streaming.spark_session import create_spark_session


PATH = "./data/gold/staffing_decision_backtest"


def main():
    spark = create_spark_session(
        "InspectStaffingBacktest"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    df = (
        spark.read
        .format("delta")
        .load(PATH)
    )

    print("=" * 130)
    print("COUNTERFACTUAL DECISION BACKTEST")
    print("=" * 130)

    print(
        f"Windows evaluated: {df.count()}"
    )

    print()

    # ------------------------------------------------------------
    # Counterfactual decision distribution
    # ------------------------------------------------------------

    print("=" * 130)
    print("COUNTERFACTUAL DECISION DISTRIBUTION")
    print("=" * 130)

    (
        df
        .groupBy(
            "counterfactual_decision"
        )
        .count()
        .orderBy(
            "counterfactual_decision"
        )
        .show(
            truncate=False
        )
    )

    print()

    # ------------------------------------------------------------
    # Objective distribution
    # ------------------------------------------------------------

    print("=" * 130)
    print("OPTIMIZATION OBJECTIVE DISTRIBUTION")
    print("=" * 130)

    (
        df
        .groupBy(
            "objective"
        )
        .count()
        .orderBy(
            "objective"
        )
        .show(
            truncate=False
        )
    )

    print()

    # ------------------------------------------------------------
    # Rule vs model
    # ------------------------------------------------------------

    print("=" * 130)
    print("RULE VS MODEL")
    print("=" * 130)

    (
        df
        .groupBy(
            "comparison"
        )
        .count()
        .orderBy(
            "comparison"
        )
        .show(
            truncate=False
        )
    )

    print()

    # ------------------------------------------------------------
    # Detailed decisions
    # ------------------------------------------------------------

    print("=" * 130)
    print("DETAILED DECISIONS")
    print("=" * 130)

    (
        df
        .select(
            "timestamp",
            "severity",

            "initial_waiting",
            "initial_in_service",
            "future_arrivals",

            "already_breached_patients",
            "maximum_possible_sla",

            "rule_additional_staff",

            "counterfactual_decision",
            "objective",

            "model_additional_staff",

            "baseline_predicted_sla",
            "baseline_predicted_p95_wait",

            "recommended_predicted_sla",
            "recommended_predicted_p95_wait",
            "recommended_predicted_avg_wait",
            "recommended_predicted_max_wait",

            "comparison",
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

    # ------------------------------------------------------------
    # Damage-control windows
    # ------------------------------------------------------------

    print("=" * 130)
    print("DAMAGE CONTROL WINDOWS")
    print("=" * 130)

    damage_control = (
        df
        .filter(
            col("counterfactual_decision")
            == "DAMAGE_CONTROL"
        )
    )

    print(
        f"Damage-control windows: "
        f"{damage_control.count()}"
    )

    (
        damage_control
        .select(
            "timestamp",
            "severity",

            "initial_waiting",
            "initial_in_service",
            "future_arrivals",

            "already_breached_patients",
            "maximum_possible_sla",

            "rule_additional_staff",
            "model_additional_staff",

            "baseline_predicted_sla",
            "baseline_predicted_p95_wait",

            "recommended_predicted_sla",
            "recommended_predicted_p95_wait",
            "recommended_predicted_avg_wait",
            "recommended_predicted_max_wait",

            "reason",
        )
        .orderBy(
            "timestamp"
        )
        .show(
            100,
            truncate=False,
        )
    )

    print()

    # ------------------------------------------------------------
    # Search-limit windows
    # ------------------------------------------------------------

    print("=" * 130)
    print("SEARCH LIMIT REACHED")
    print("=" * 130)

    search_limit = (
        df
        .filter(
            col("counterfactual_decision")
            == "SEARCH_LIMIT_REACHED"
        )
    )

    print(
        f"Search-limit windows: "
        f"{search_limit.count()}"
    )

    (
        search_limit
        .select(
            "timestamp",
            "severity",
            "maximum_possible_sla",
            "model_additional_staff",
            "recommended_predicted_sla",
            "recommended_predicted_p95_wait",
            "reason",
        )
        .orderBy(
            "timestamp"
        )
        .show(
            100,
            truncate=False,
        )
    )

    print()

    # ------------------------------------------------------------
    # Staffing efficiency
    # ------------------------------------------------------------

    print("=" * 130)
    print("STAFFING EFFICIENCY")
    print("=" * 130)

    resolved = (
        df
        .filter(
            col("model_additional_staff")
            .isNotNull()
        )
    )

    (
        resolved
        .agg(
            count("*").alias(
                "resolved_windows"
            ),

            spark_sum(
                "rule_additional_staff"
            ).alias(
                "rule_staff_actions"
            ),

            spark_sum(
                "model_additional_staff"
            ).alias(
                "model_staff_actions"
            ),

            avg(
                "rule_additional_staff"
            ).alias(
                "avg_rule_staff"
            ),

            avg(
                "model_additional_staff"
            ).alias(
                "avg_model_staff"
            ),
        )
        .show(
            truncate=False
        )
    )

    print()

    # ------------------------------------------------------------
    # Rule overstaffing
    # ------------------------------------------------------------

    print("=" * 130)
    print("RULE OVERSTAFFING WINDOWS")
    print("=" * 130)

    (
        df
        .filter(
            col("comparison")
            == "RULE_OVERSTAFFS"
        )
        .select(
            "timestamp",
            "severity",
            "rule_additional_staff",
            "model_additional_staff",
            "baseline_predicted_sla",
            "recommended_predicted_sla",
        )
        .orderBy(
            "timestamp"
        )
        .show(
            100,
            truncate=False,
        )
    )

    print()

    # ------------------------------------------------------------
    # Rule understaffing
    # ------------------------------------------------------------

    print("=" * 130)
    print("RULE UNDERSTAFFING WINDOWS")
    print("=" * 130)

    (
        df
        .filter(
            col("comparison")
            == "RULE_UNDERSTAFFS"
        )
        .select(
            "timestamp",
            "severity",
            "rule_additional_staff",
            "model_additional_staff",
            "baseline_predicted_sla",
            "recommended_predicted_sla",
            "recommended_predicted_p95_wait",
        )
        .orderBy(
            "timestamp"
        )
        .show(
            100,
            truncate=False,
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()