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
from streaming.spark_session import create_spark_session


PATIENT_METRICS_PATH = "./data/gold/patient_metrics"
STAFFING_DECISIONS_PATH = "./data/gold/staffing_decisions_5m"

OUTPUT_PATH = "./data/gold/staffing_decision_backtest"

BASELINE_STAFF = 4
LOOKAHEAD_MINUTES = 30

QUEUE_SLA_MINUTES = 15.0
TARGET_SLA_PERCENTAGE = 95.0

MAX_ADDITIONAL_STAFF = 6


def build_initial_state(
    patients,
    timestamp,
):
    waiting_rows = (
        patients
        .filter(
            (col("queue_entry_time") <= timestamp)
            & (col("service_start_time") > timestamp)
        )
        .orderBy("queue_entry_time")
        .collect()
    )

    initial_waiting = [
        InitialWaitingPatient(
            patient_type=row["patient_type"],
            service_duration_minutes=float(
                row["service_minutes"]
            ),
            accrued_wait_minutes=(
                timestamp
                - row["queue_entry_time"]
            ).total_seconds()
            / 60.0,
        )
        for row in waiting_rows
    ]

    service_rows = (
        patients
        .filter(
            (col("service_start_time") <= timestamp)
            & (col("service_end_time") > timestamp)
        )
        .orderBy("service_start_time")
        .collect()
    )

    initial_services = [
        InitialService(
            remaining_service_minutes=(
                row["service_end_time"]
                - timestamp
            ).total_seconds()
            / 60.0
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
            (col("arrival_time") >= timestamp)
            & (col("arrival_time") < end_time)
        )
        .orderBy("arrival_time")
        .collect()
    )

    arrivals = [
        (
            (
                row["arrival_time"]
                - timestamp
            ).total_seconds()
            / 60.0,
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
        "BacktestStaffingDecisions"
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
        .load(PATIENT_METRICS_PATH)
    )

    decisions = (
        spark.read
        .format("delta")
        .load(STAFFING_DECISIONS_PATH)
        .filter(
            col("intervention_required")
        )
        .orderBy("timestamp")
        .collect()
    )

    engine = CounterfactualStaffingEngine(
        config=config,
        baseline_staff=BASELINE_STAFF,
        queue_sla_minutes=QUEUE_SLA_MINUTES,
        target_sla_percentage=(
            TARGET_SLA_PERCENTAGE
        ),
    )

    output_rows = []

    print("=" * 120)
    print(
        "STAFFING DECISION "
        "COUNTERFACTUAL BACKTEST"
    )
    print("=" * 120)

    for decision_row in decisions:

        timestamp = (
            decision_row["timestamp"]
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

        # --------------------------------------------------------
        # Baseline result
        # --------------------------------------------------------

        if scenarios:
            baseline_result = (
                scenarios[0]
            )

            baseline_predicted_sla = float(
                baseline_result.sla_percentage
            )

            baseline_predicted_p95 = float(
                baseline_result.p95_wait_minutes
            )

        else:
            baseline_predicted_sla = None
            baseline_predicted_p95 = None

        # --------------------------------------------------------
        # Recommended counterfactual result
        # --------------------------------------------------------

        if (
            recommendation.recommended_result
            is not None
        ):
            result = (
                recommendation.recommended_result
            )

            model_additional_staff = int(
                result.additional_staff
            )

            model_total_staff = int(
                result.staff_count
            )

            recommended_predicted_sla = float(
                result.sla_percentage
            )

            recommended_predicted_p95 = float(
                result.p95_wait_minutes
            )

            recommended_predicted_avg_wait = float(
                result.average_wait_minutes
            )

            recommended_predicted_max_wait = float(
                result.max_wait_minutes
            )

        else:
            model_additional_staff = None
            model_total_staff = None

            recommended_predicted_sla = None
            recommended_predicted_p95 = None
            recommended_predicted_avg_wait = None
            recommended_predicted_max_wait = None

        rule_additional_staff = int(
            decision_row[
                "additional_staff"
            ]
        )

        # --------------------------------------------------------
        # Rule vs model comparison
        # --------------------------------------------------------

        if (
            recommendation.decision
            == "DAMAGE_CONTROL"
        ):
            comparison = "DAMAGE_CONTROL"

        elif (
            recommendation.decision
            == "SEARCH_LIMIT_REACHED"
        ):
            comparison = (
                "SEARCH_LIMIT_REACHED"
            )

        elif (
            model_additional_staff
            is None
        ):
            comparison = "UNKNOWN"

        elif (
            rule_additional_staff
            > model_additional_staff
        ):
            comparison = (
                "RULE_OVERSTAFFS"
            )

        elif (
            rule_additional_staff
            < model_additional_staff
        ):
            comparison = (
                "RULE_UNDERSTAFFS"
            )

        else:
            comparison = "ALIGNED"

        output_rows.append(
            (
                # 1
                timestamp,

                # 2
                decision_row["lab_id"],

                # 3
                decision_row["severity"],

                # 4
                rule_additional_staff,

                # 5
                recommendation.decision,

                # 6
                recommendation.objective,

                # 7
                model_additional_staff,

                # 8
                model_total_staff,

                # 9
                len(initial_waiting),

                # 10
                len(initial_services),

                # 11
                future_arrivals,

                # 12
                baseline_predicted_sla,

                # 13
                baseline_predicted_p95,

                # 14
                recommended_predicted_sla,

                # 15
                recommended_predicted_p95,

                # 16
                recommended_predicted_avg_wait,

                # 17
                recommended_predicted_max_wait,

                # 18
                float(
                    recommendation
                    .maximum_possible_sla_percentage
                ),

                # 19
                int(
                    recommendation
                    .already_breached_patients
                ),

                # 20
                float(
                    recommendation
                    .sla_improvement_percentage_points
                ),

                # 21
                float(
                    recommendation
                    .p95_improvement_minutes
                ),

                # 22
                float(
                    recommendation
                    .marginal_p95_threshold_minutes
                ),

                # 23
                comparison,

                # 24
                recommendation.reason,
            )
        )

    schema = StructType(
        [
            # 1
            StructField(
                "timestamp",
                TimestampType(),
                False,
            ),

            # 2
            StructField(
                "lab_id",
                StringType(),
                False,
            ),

            # 3
            StructField(
                "severity",
                StringType(),
                False,
            ),

            # 4
            StructField(
                "rule_additional_staff",
                IntegerType(),
                False,
            ),

            # 5
            StructField(
                "counterfactual_decision",
                StringType(),
                False,
            ),

            # 6
            StructField(
                "objective",
                StringType(),
                False,
            ),

            # 7
            StructField(
                "model_additional_staff",
                IntegerType(),
                True,
            ),

            # 8
            StructField(
                "model_total_staff",
                IntegerType(),
                True,
            ),

            # 9
            StructField(
                "initial_waiting",
                IntegerType(),
                False,
            ),

            # 10
            StructField(
                "initial_in_service",
                IntegerType(),
                False,
            ),

            # 11
            StructField(
                "future_arrivals",
                IntegerType(),
                False,
            ),

            # 12
            StructField(
                "baseline_predicted_sla",
                DoubleType(),
                True,
            ),

            # 13
            StructField(
                "baseline_predicted_p95_wait",
                DoubleType(),
                True,
            ),

            # 14
            StructField(
                "recommended_predicted_sla",
                DoubleType(),
                True,
            ),

            # 15
            StructField(
                "recommended_predicted_p95_wait",
                DoubleType(),
                True,
            ),

            # 16
            StructField(
                "recommended_predicted_avg_wait",
                DoubleType(),
                True,
            ),

            # 17
            StructField(
                "recommended_predicted_max_wait",
                DoubleType(),
                True,
            ),

            # 18
            StructField(
                "maximum_possible_sla",
                DoubleType(),
                False,
            ),

            # 19
            StructField(
                "already_breached_patients",
                IntegerType(),
                False,
            ),

            # 20
            StructField(
                "sla_improvement_percentage_points",
                DoubleType(),
                False,
            ),

            # 21
            StructField(
                "p95_improvement_minutes",
                DoubleType(),
                False,
            ),

            # 22
            StructField(
                "marginal_p95_threshold_minutes",
                DoubleType(),
                False,
            ),

            # 23
            StructField(
                "comparison",
                StringType(),
                False,
            ),

            # 24
            StructField(
                "reason",
                StringType(),
                False,
            ),
        ]
    )

        
    print(
        "Tuple fields:",
        len(output_rows[0]),
    )

    print(
        "Schema fields:",
        len(schema.fields),
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

    print(
        f"Intervention windows evaluated: "
        f"{len(output_rows)}"
    )

    print("=" * 120)

    spark.stop()


if __name__ == "__main__":
    main()