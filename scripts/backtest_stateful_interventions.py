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
    "./data/gold/"
    "staffing_decision_backtest"
)

OUTPUT_PATH = (
    "./data/gold/"
    "stateful_intervention_backtest"
)

WINDOW_MINUTES = 5
MINIMUM_HOLD_MINUTES = 15


def main():

    spark = create_spark_session(
        "StatefulInterventionBacktest"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    df = (
        spark.read
        .format("delta")
        .load(INPUT_PATH)
        .orderBy("timestamp")
    )

    rows = df.collect()

    policy = StatefulInterventionPolicy(
        minimum_hold_minutes=(
            MINIMUM_HOLD_MINUTES
        )
    )

    state = InterventionState()

    output = []

    for row in rows:

        recommended_staff = int(
            row[
                "model_additional_staff"
            ]
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

                counterfactual_decision=(
                    row[
                        "counterfactual_decision"
                    ]
                ),

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
                    state
                    .active_additional_staff
                ),

                reason=(
                    action.reason
                ),
            )
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
    print(
        "STATEFUL INTERVENTION "
        "BACKTEST CREATED"
    )
    print("=" * 100)

    print(
        f"Windows evaluated: "
        f"{len(output)}"
    )

    spark.stop()


if __name__ == "__main__":
    main()