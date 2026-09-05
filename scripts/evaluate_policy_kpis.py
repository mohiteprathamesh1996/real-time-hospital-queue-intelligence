from pyspark.sql.functions import (
    col,
    count,
    max as spark_max,
    sum as spark_sum,
)

from config.settings import load_config
from decision_engine.cost_model import (
    StaffingCostModel,
)
from streaming.spark_session import (
    create_spark_session,
)


POLICY_PATH = (
    "./data/gold/policy_comparison_stateful"
)

COUNTERFACTUAL_PATH = (
    "./data/gold/full_timeline_counterfactual"
)

WINDOW_MINUTES = 5


def main():

    spark = create_spark_session(
        "EvaluatePolicyKPIs"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    config = load_config(
        "config/hospital.yaml"
    )

    hourly_cost = float(
        config.staffing
        .incremental_staff_hour_cost
    )

    cost_model = StaffingCostModel(
        hourly_cost_per_staff=(
            hourly_cost
        )
    )

    policy_df = (
        spark.read
        .format("delta")
        .load(POLICY_PATH)
    )

    counterfactual_df = (
        spark.read
        .format("delta")
        .load(COUNTERFACTUAL_PATH)
    )

    # ============================================================
    # RESOURCE METRICS
    # ============================================================

    resource_rows = (
        policy_df
        .groupBy("policy")
        .agg(
            spark_sum(
                "active_additional_staff"
            ).alias(
                "staff_window_units"
            ),

            spark_max(
                "active_additional_staff"
            ).alias(
                "peak_extra_staff"
            ),

            count(
                "*"
            ).alias(
                "windows"
            ),
        )
        .collect()
    )

    resources = {}

    for row in resource_rows:

        staff_window_units = (
            row["staff_window_units"]
            or 0
        )

        staff_minutes = (
            staff_window_units
            * WINDOW_MINUTES
        )

        staff_hours = (
            staff_minutes
            / 60.0
        )

        cost = cost_model.calculate(
            staff_hours=staff_hours
        )

        resources[
            row["policy"]
        ] = {
            "staff_window_units": (
                staff_window_units
            ),
            "staff_minutes": (
                staff_minutes
            ),
            "staff_hours": (
                staff_hours
            ),
            "peak_extra_staff": (
                row["peak_extra_staff"]
            ),
            "cost": (
                cost.total_cost
            ),
        }

    static = resources[
        "STATIC_RULE"
    ]

    counterfactual = resources[
        "COUNTERFACTUAL"
    ]

    # ============================================================
    # ACTION METRICS
    # ============================================================

    action_rows = (
        policy_df
        .filter(
            col("policy_action").isin(
                "DEPLOY",
                "ESCALATE",
                "DEESCALATE",
                "RELEASE",
            )
        )
        .groupBy("policy")
        .count()
        .collect()
    )

    action_counts = {
        row["policy"]: row["count"]
        for row in action_rows
    }

    # ============================================================
    # DECISION QUALITY
    # ============================================================

    decision_distribution = (
        counterfactual_df
        .groupBy(
            "counterfactual_decision"
        )
        .count()
        .collect()
    )

    decisions = {
        row[
            "counterfactual_decision"
        ]: row["count"]
        for row in decision_distribution
    }

    no_action_windows = decisions.get(
        "NO_ACTION_REQUIRED",
        0,
    )

    intervention_windows = decisions.get(
        "STAFFING_INTERVENTION",
        0,
    )

    damage_control_windows = decisions.get(
        "DAMAGE_CONTROL",
        0,
    )

    search_limit_windows = decisions.get(
        "SEARCH_LIMIT_REACHED",
        0,
    )

    # ============================================================
    # SAVINGS
    # ============================================================

    staff_hours_saved = (
        static["staff_hours"]
        - counterfactual[
            "staff_hours"
        ]
    )

    cost_saved = (
        static["cost"]
        - counterfactual["cost"]
    )

    if static["staff_hours"] > 0:
        resource_reduction = (
            staff_hours_saved
            / static["staff_hours"]
            * 100
        )
    else:
        resource_reduction = 0.0

    if static["cost"] > 0:
        cost_reduction = (
            cost_saved
            / static["cost"]
            * 100
        )
    else:
        cost_reduction = 0.0

    # ============================================================
    # OUTPUT
    # ============================================================

    print("=" * 100)
    print(
        "HOSPITAL STAFFING POLICY KPI SUMMARY"
    )
    print("=" * 100)

    print()

    print(
        f"Incremental staff-hour cost: "
        f"${hourly_cost:.2f}"
    )

    print()

    print("=" * 100)
    print("STATIC RULE POLICY")
    print("=" * 100)

    print(
        f"Incremental staff-hours: "
        f"{static['staff_hours']:.2f}"
    )

    print(
        f"Estimated intervention cost: "
        f"${static['cost']:.2f}"
    )

    print(
        f"Peak additional staff: "
        f"{static['peak_extra_staff']}"
    )

    print(
        f"Staffing change actions: "
        f"{action_counts.get('STATIC_RULE', 0)}"
    )

    print()

    print("=" * 100)
    print("COUNTERFACTUAL POLICY")
    print("=" * 100)

    print(
        f"Incremental staff-hours: "
        f"{counterfactual['staff_hours']:.2f}"
    )

    print(
        f"Estimated intervention cost: "
        f"${counterfactual['cost']:.2f}"
    )

    print(
        f"Peak additional staff: "
        f"{counterfactual['peak_extra_staff']}"
    )

    print(
        f"Staffing change actions: "
        f"{action_counts.get('COUNTERFACTUAL', 0)}"
    )

    print()

    print("=" * 100)
    print("RESOURCE EFFICIENCY")
    print("=" * 100)

    print(
        f"Staff-hours avoided: "
        f"{staff_hours_saved:.2f}"
    )

    print(
        f"Staff-hour reduction: "
        f"{resource_reduction:.1f}%"
    )

    print(
        f"Estimated cost avoided: "
        f"${cost_saved:.2f}"
    )

    print(
        f"Estimated cost reduction: "
        f"{cost_reduction:.1f}%"
    )

    print()

    print("=" * 100)
    print("DECISION DISTRIBUTION")
    print("=" * 100)

    print(
        f"No action required:       "
        f"{no_action_windows}"
    )

    print(
        f"SLA restoration:          "
        f"{intervention_windows}"
    )

    print(
        f"Damage-control windows:   "
        f"{damage_control_windows}"
    )

    print(
        f"Search-limit windows:     "
        f"{search_limit_windows}"
    )

    print()

    print("=" * 100)
    print("INTERPRETATION")
    print("=" * 100)

    print(
        "The counterfactual policy uses "
        "simulation to concentrate staffing "
        "capacity during periods where additional "
        "resources materially improve service "
        "performance."
    )

    print()

    print(
        "Compared with the static rule policy, "
        "it avoids unnecessary staffing during "
        "manageable queue conditions while still "
        "allowing stronger temporary escalation "
        "during severe congestion."
    )

    print()

    print(
        "Cost figures represent modeled incremental "
        "staffing cost only. They do not represent "
        "total hospital labor cost or realized "
        "financial savings."
    )

    print("=" * 100)

    spark.stop()


if __name__ == "__main__":
    main()