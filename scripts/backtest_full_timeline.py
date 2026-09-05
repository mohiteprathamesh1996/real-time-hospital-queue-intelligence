from datetime import timedelta

from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.settings import load_config
from decision_engine.counterfactual_engine import (
    CounterfactualStaffingEngine,
)
from simulator.simpy_engine import (
    InitialService,
    InitialWaitingPatient,
)
from streaming.spark_session import (
    create_spark_session,
)


OPERATIONAL_STATE_PATH = (
    "./data/gold/operational_state_5m"
)

PATIENT_METRICS_PATH = (
    "./data/gold/patient_metrics"
)

STAFFING_DECISIONS_PATH = (
    "./data/gold/staffing_decisions_5m"
)

OUTPUT_PATH = (
    "./data/gold/full_timeline_counterfactual"
)


BASELINE_STAFF = 4

LOOKAHEAD_MINUTES = 30

QUEUE_SLA_MINUTES = 15.0
TARGET_SLA_PERCENTAGE = 95.0

MAX_ADDITIONAL_STAFF = 6

MIN_P95_IMPROVEMENT_MINUTES = 2.0


def build_initial_state(
    patients,
    timestamp,
):
    waiting_rows = (
        patients
        .filter(
            (
                col("queue_entry_time")
                <= timestamp
            )
            & (
                col("service_start_time")
                > timestamp
            )
        )
        .orderBy(
            "queue_entry_time"
        )
        .collect()
    )

    initial_waiting = [
        InitialWaitingPatient(
            patient_type=(
                row["patient_type"]
            ),
            service_duration_minutes=float(
                row["service_minutes"]
            ),
            accrued_wait_minutes=(
                (
                    timestamp
                    - row["queue_entry_time"]
                ).total_seconds()
                / 60.0
            ),
        )
        for row in waiting_rows
    ]

    service_rows = (
        patients
        .filter(
            (
                col("service_start_time")
                <= timestamp
            )
            & (
                col("service_end_time")
                > timestamp
            )
        )
        .orderBy(
            "service_start_time"
        )
        .collect()
    )

    initial_services = [
        InitialService(
            remaining_service_minutes=(
                (
                    row["service_end_time"]
                    - timestamp
                ).total_seconds()
                / 60.0
            )
        )
        for row in service_rows
    ]

    return (
        initial_waiting,
        initial_services,
    )


def build_future_workload(
    patients,
    timestamp,
):
    end_time = (
        timestamp
        + timedelta(
            minutes=LOOKAHEAD_MINUTES
        )
    )

    future_rows = (
        patients
        .filter(
            (
                col("arrival_time")
                >= timestamp
            )
            & (
                col("arrival_time")
                < end_time
            )
        )
        .orderBy(
            "arrival_time"
        )
        .collect()
    )

    arrivals = [
        (
            (
                (
                    row["arrival_time"]
                    - timestamp
                ).total_seconds()
                / 60.0
            ),
            row["patient_type"],
        )
        for row in future_rows
    ]

    service_durations = [
        float(
            row["service_minutes"]
        )
        for row in future_rows
    ]

    return (
        arrivals,
        service_durations,
        len(future_rows),
    )


def main():
    spark = create_spark_session(
        "FullTimelineCounterfactualBacktest"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    config = load_config(
        "config/hospital.yaml"
    )

    patients = (
        spark.read
        .format("delta")
        .load(
            PATIENT_METRICS_PATH
        )
    )

    operational_state = (
        spark.read
        .format("delta")
        .load(
            OPERATIONAL_STATE_PATH
        )
        .orderBy(
            "window_start"
        )
    )

    rule_decisions = (
        spark.read
        .format("delta")
        .load(
            STAFFING_DECISIONS_PATH
        )
        .select(
            col("timestamp").alias(
                "rule_timestamp"
            ),
            col(
                "additional_staff"
            ).alias(
                "rule_additional_staff"
            ),
            col(
                "severity"
            ).alias(
                "rule_severity"
            ),
        )
    )

    timeline = (
        operational_state
        .join(
            rule_decisions,
            operational_state.window_start
            == rule_decisions.rule_timestamp,
            "left",
        )
        .orderBy(
            "window_start"
        )
        .collect()
    )

    engine = CounterfactualStaffingEngine(
        config=config,
        baseline_staff=BASELINE_STAFF,
        queue_sla_minutes=(
            QUEUE_SLA_MINUTES
        ),
        target_sla_percentage=(
            TARGET_SLA_PERCENTAGE
        ),
        min_p95_improvement_minutes=(
            MIN_P95_IMPROVEMENT_MINUTES
        ),
    )

    output_rows = []

    print("=" * 120)
    print(
        "FULL TIMELINE COUNTERFACTUAL BACKTEST"
    )
    print("=" * 120)

    for index, row in enumerate(
        timeline,
        start=1,
    ):
        timestamp = (
            row["window_start"]
        )

        (
            initial_waiting,
            initial_services,
        ) = build_initial_state(
            patients,
            timestamp,
        )

        (
            arrivals,
            service_durations,
            future_arrivals,
        ) = build_future_workload(
            patients,
            timestamp,
        )

        recommendation, scenarios = (
            engine.recommend(
                arrivals=arrivals,
                service_durations=(
                    service_durations
                ),
                initial_waiting=(
                    initial_waiting
                ),
                initial_services=(
                    initial_services
                ),
                max_additional_staff=(
                    MAX_ADDITIONAL_STAFF
                ),
            )
        )

        result = (
            recommendation
            .recommended_result
        )

        if result is None:
            model_additional_staff = 0
            model_total_staff = (
                BASELINE_STAFF
            )

            recommended_sla = None
            recommended_p95 = None

        else:
            model_additional_staff = int(
                result.additional_staff
            )

            model_total_staff = int(
                result.staff_count
            )

            recommended_sla = float(
                result.sla_percentage
            )

            recommended_p95 = float(
                result.p95_wait_minutes
            )

        if scenarios:
            baseline = scenarios[0]

            baseline_sla = float(
                baseline.sla_percentage
            )

            baseline_p95 = float(
                baseline.p95_wait_minutes
            )

        else:
            baseline_sla = None
            baseline_p95 = None

        rule_additional_staff = int(
            row["rule_additional_staff"]
            or 0
        )

        rule_severity = (
            row["rule_severity"]
            or "NORMAL"
        )

        output_rows.append(
            (
                timestamp,

                row["lab_id"],

                rule_severity,

                rule_additional_staff,

                recommendation.decision,

                recommendation.objective,

                model_additional_staff,

                model_total_staff,

                len(initial_waiting),

                len(initial_services),

                future_arrivals,

                baseline_sla,

                baseline_p95,

                recommended_sla,

                recommended_p95,

                float(
                    recommendation
                    .maximum_possible_sla_percentage
                ),

                int(
                    recommendation
                    .already_breached_patients
                ),

                float(
                    recommendation
                    .sla_improvement_percentage_points
                ),

                float(
                    recommendation
                    .p95_improvement_minutes
                ),
            )
        )

        if (
            index % 20 == 0
            or index == len(timeline)
        ):
            print(
                f"Processed "
                f"{index}/{len(timeline)} windows"
            )

    schema = StructType(
        [
            StructField(
                "timestamp",
                TimestampType(),
                False,
            ),

            StructField(
                "lab_id",
                StringType(),
                False,
            ),

            StructField(
                "rule_severity",
                StringType(),
                False,
            ),

            StructField(
                "rule_additional_staff",
                IntegerType(),
                False,
            ),

            StructField(
                "counterfactual_decision",
                StringType(),
                False,
            ),

            StructField(
                "objective",
                StringType(),
                False,
            ),

            StructField(
                "model_additional_staff",
                IntegerType(),
                False,
            ),

            StructField(
                "model_total_staff",
                IntegerType(),
                False,
            ),

            StructField(
                "initial_waiting",
                IntegerType(),
                False,
            ),

            StructField(
                "initial_in_service",
                IntegerType(),
                False,
            ),

            StructField(
                "future_arrivals",
                IntegerType(),
                False,
            ),

            StructField(
                "baseline_predicted_sla",
                DoubleType(),
                True,
            ),

            StructField(
                "baseline_predicted_p95_wait",
                DoubleType(),
                True,
            ),

            StructField(
                "recommended_predicted_sla",
                DoubleType(),
                True,
            ),

            StructField(
                "recommended_predicted_p95_wait",
                DoubleType(),
                True,
            ),

            StructField(
                "maximum_possible_sla",
                DoubleType(),
                False,
            ),

            StructField(
                "already_breached_patients",
                IntegerType(),
                False,
            ),

            StructField(
                "sla_improvement_percentage_points",
                DoubleType(),
                False,
            ),

            StructField(
                "p95_improvement_minutes",
                DoubleType(),
                False,
            ),
        ]
    )

    result_df = spark.createDataFrame(
        output_rows,
        schema=schema,
    )

    (
        result_df
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

    print("=" * 120)

    print(
        f"Full timeline windows written: "
        f"{len(output_rows)}"
    )

    print(
        f"Output: {OUTPUT_PATH}"
    )

    print("=" * 120)

    spark.stop()


if __name__ == "__main__":
    main()