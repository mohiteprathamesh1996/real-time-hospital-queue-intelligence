from pyspark.sql import Row

from config.settings import load_config
from decision_engine.cost_model import (
    StaffingCostModel,
)
from decision_engine.intervention_policy import (
    StatefulInterventionPolicy,
)
from decision_engine.intervention_state import (
    InterventionState,
)
from streaming.spark_session import (
    create_spark_session,
)


ORACLE_PATH = (
    "./data/gold/full_timeline_counterfactual"
)

FORECAST_PATH = (
    "./data/gold/probabilistic_staffing_timeline"
)

WINDOW_MINUTES = 5

MINIMUM_HOLD_MINUTES = 15


def apply_policy(
    rows,
    recommendation_field,
    policy_name,
):

    policy = StatefulInterventionPolicy(
        minimum_hold_minutes=(
            MINIMUM_HOLD_MINUTES
        )
    )

    state = InterventionState()

    output = []

    for row in rows:

        recommendation = int(
            row[
                recommendation_field
            ]
            or 0
        )

        action = policy.apply(
            timestamp=row["timestamp"],
            recommended_additional_staff=(
                recommendation
            ),
            state=state,
        )

        output.append(
            Row(
                timestamp=row["timestamp"],
                policy=policy_name,
                recommendation=(
                    recommendation
                ),
                policy_action=(
                    action.action
                ),
                active_additional_staff=(
                    state
                    .active_additional_staff
                ),
            )
        )

    return output


def summarize(
    rows,
    hourly_cost,
):

    staff_window_units = sum(
        row.active_additional_staff
        for row in rows
    )

    staff_minutes = (
        staff_window_units
        * WINDOW_MINUTES
    )

    staff_hours = (
        staff_minutes
        / 60.0
    )

    peak_staff = max(
        (
            row.active_additional_staff
            for row in rows
        ),
        default=0,
    )

    change_actions = sum(
        row.policy_action
        in {
            "DEPLOY",
            "ESCALATE",
            "DEESCALATE",
            "RELEASE",
        }
        for row in rows
    )

    cost_model = StaffingCostModel(
        hourly_cost_per_staff=(
            hourly_cost
        )
    )

    cost = cost_model.calculate(
        staff_hours=staff_hours
    )

    return {
        "staff_window_units": (
            staff_window_units
        ),
        "staff_minutes": (
            staff_minutes
        ),
        "staff_hours": (
            staff_hours
        ),
        "peak_staff": (
            peak_staff
        ),
        "change_actions": (
            change_actions
        ),
        "cost": (
            cost.total_cost
        ),
    }


def main():

    spark = create_spark_session(
        "EvaluateThreePolicyResources"
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

    oracle_df = (
        spark.read
        .format("delta")
        .load(
            ORACLE_PATH
        )
        .select(
            "timestamp",
            "rule_additional_staff",
            "model_additional_staff",
        )
    )

    forecast_df = (
        spark.read
        .format("delta")
        .load(
            FORECAST_PATH
        )
        .select(
            "timestamp",
            "forecast_additional_staff",
        )
    )

    timeline = (
        oracle_df
        .join(
            forecast_df,
            on="timestamp",
            how="left",
        )
        .orderBy(
            "timestamp"
        )
        .collect()
    )

    static_rows = apply_policy(
        rows=timeline,
        recommendation_field=(
            "rule_additional_staff"
        ),
        policy_name="STATIC_RULE",
    )

    oracle_rows = apply_policy(
        rows=timeline,
        recommendation_field=(
            "model_additional_staff"
        ),
        policy_name="ORACLE_COUNTERFACTUAL",
    )

    forecast_rows = apply_policy(
        rows=timeline,
        recommendation_field=(
            "forecast_additional_staff"
        ),
        policy_name="FORECAST_PROBABILISTIC",
    )

    summaries = {
        "STATIC_RULE": summarize(
            static_rows,
            hourly_cost,
        ),
        "ORACLE_COUNTERFACTUAL": summarize(
            oracle_rows,
            hourly_cost,
        ),
        "FORECAST_PROBABILISTIC": summarize(
            forecast_rows,
            hourly_cost,
        ),
    }

    print("=" * 120)
    print(
        "THREE-POLICY RESOURCE AND COST COMPARISON"
    )
    print("=" * 120)

    print(
        f"Incremental staff-hour cost: "
        f"${hourly_cost:.2f}"
    )

    print()

    print(
        f"{'Policy':<28}"
        f"{'Staff Hours':>14}"
        f"{'Cost':>14}"
        f"{'Peak +Staff':>14}"
        f"{'Actions':>12}"
    )

    print("-" * 82)

    for policy_name in [
        "STATIC_RULE",
        "ORACLE_COUNTERFACTUAL",
        "FORECAST_PROBABILISTIC",
    ]:

        result = summaries[
            policy_name
        ]

        print(
            f"{policy_name:<28}"
            f"{result['staff_hours']:>14.2f}"
            f"${result['cost']:>13.2f}"
            f"{result['peak_staff']:>14}"
            f"{result['change_actions']:>12}"
        )

    static = summaries[
        "STATIC_RULE"
    ]

    oracle = summaries[
        "ORACLE_COUNTERFACTUAL"
    ]

    forecast = summaries[
        "FORECAST_PROBABILISTIC"
    ]

    print()

    print("=" * 120)
    print(
        "FORECAST POLICY VS STATIC RULE"
    )
    print("=" * 120)

    saved_hours = (
        static["staff_hours"]
        - forecast["staff_hours"]
    )

    saved_cost = (
        static["cost"]
        - forecast["cost"]
    )

    reduction = (
        saved_hours
        / static["staff_hours"]
        * 100
        if static["staff_hours"]
        else 0.0
    )

    print(
        f"Staff-hours avoided: "
        f"{saved_hours:.2f}"
    )

    print(
        f"Resource reduction:  "
        f"{reduction:.1f}%"
    )

    print(
        f"Estimated cost avoided: "
        f"${saved_cost:.2f}"
    )

    print()

    print("=" * 120)
    print(
        "FORECAST POLICY VS ORACLE"
    )
    print("=" * 120)

    oracle_gap = (
        forecast["staff_hours"]
        - oracle["staff_hours"]
    )

    print(
        f"Oracle staff-hours:   "
        f"{oracle['staff_hours']:.2f}"
    )

    print(
        f"Forecast staff-hours: "
        f"{forecast['staff_hours']:.2f}"
    )

    print(
        f"Forecast-oracle gap:  "
        f"{oracle_gap:+.2f} hours"
    )

    print()

    print(
        "Oracle uses actual future demand and represents "
        "a historical upper benchmark. Forecast policy "
        "uses only information available at decision time."
    )

    print("=" * 120)

    spark.stop()


if __name__ == "__main__":
    main()
