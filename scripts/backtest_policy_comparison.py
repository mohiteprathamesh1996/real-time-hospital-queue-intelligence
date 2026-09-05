from pyspark.sql import Row

from decision_engine.intervention_policy import (
    StatefulInterventionPolicy,
)
from decision_engine.intervention_state import (
    InterventionState,
)
from streaming.spark_session import (
    create_spark_session,
)


INPUT_PATH = (
    "./data/gold/full_timeline_counterfactual"
)

OUTPUT_PATH = (
    "./data/gold/policy_comparison_stateful"
)

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
        recommended_staff = int(
            row[recommendation_field]
            or 0
        )

        action = policy.apply(
            timestamp=row["timestamp"],
            recommended_additional_staff=(
                recommended_staff
            ),
            state=state,
        )

        output.append(
            Row(
                timestamp=row["timestamp"],
                policy=policy_name,

                instantaneous_recommendation=(
                    recommended_staff
                ),

                policy_action=(
                    action.action
                ),

                staff_change=(
                    action.delta_staff
                ),

                active_additional_staff=(
                    state.active_additional_staff
                ),

                counterfactual_decision=(
                    row[
                        "counterfactual_decision"
                    ]
                ),

                objective=(
                    row["objective"]
                ),

                baseline_predicted_sla=(
                    row[
                        "baseline_predicted_sla"
                    ]
                ),

                recommended_predicted_sla=(
                    row[
                        "recommended_predicted_sla"
                    ]
                ),

                reason=(
                    action.reason
                ),
            )
        )

    return output


def main():
    spark = create_spark_session(
        "PolicyComparisonBacktest"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    timeline = (
        spark.read
        .format("delta")
        .load(INPUT_PATH)
        .orderBy("timestamp")
        .collect()
    )

    static_rule_rows = apply_policy(
        rows=timeline,
        recommendation_field=(
            "rule_additional_staff"
        ),
        policy_name="STATIC_RULE",
    )

    counterfactual_rows = apply_policy(
        rows=timeline,
        recommendation_field=(
            "model_additional_staff"
        ),
        policy_name="COUNTERFACTUAL",
    )

    output = (
        static_rule_rows
        + counterfactual_rows
    )

    result = spark.createDataFrame(
        output
    )

    (
        result
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .save(
            OUTPUT_PATH
        )
    )

    print("=" * 100)
    print("STATEFUL POLICY COMPARISON CREATED")
    print("=" * 100)

    print(
        f"Timeline windows: "
        f"{len(timeline)}"
    )

    print(
        f"Rows written:     "
        f"{len(output)}"
    )

    print(
        f"Policies:         2"
    )

    print("=" * 100)

    spark.stop()


if __name__ == "__main__":
    main()