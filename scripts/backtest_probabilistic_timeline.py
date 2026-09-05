from datetime import timedelta

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.settings import load_config
from decision_engine.probabilistic_staffing import (
    ProbabilisticStaffingEngine,
)
from decision_engine.weighted_scenario_forecast import (
    WeightedScenarioForecaster,
)
from simulator.event_models import PatientType
from simulator.simpy_engine import (
    InitialService,
    InitialWaitingPatient,
)
from streaming.spark_session import (
    create_spark_session,
)


PATIENT_METRICS_PATH = (
    "./data/gold/patient_metrics"
)

FULL_TIMELINE_PATH = (
    "./data/gold/full_timeline_counterfactual"
)

OUTPUT_PATH = (
    "./data/gold/probabilistic_staffing_timeline"
)


LOOKBACK_MINUTES = 30
SEGMENT_MINUTES = 15

FORECAST_HORIZON_MINUTES = 30

MONTE_CARLO_SCENARIOS = 200

BASELINE_STAFF = 4
MAX_ADDITIONAL_STAFF = 6

QUEUE_SLA_MINUTES = 15.0
TARGET_SLA_PERCENTAGE = 95.0

TARGET_SUCCESS_PROBABILITY = 0.95

OLDER_WEIGHT = 1.0
RECENT_WEIGHT = 2.0

BASE_SEED = 42000


def to_patient_type(value):

    if isinstance(
        value,
        PatientType,
    ):
        return value

    return PatientType(value)


def build_current_state(
    patients,
    timestamp,
):
    waiting_rows = (
        patients
        .filter(
            (
                patients.queue_entry_time
                <= timestamp
            )
            & (
                patients.service_start_time
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
            patient_type=to_patient_type(
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
                patients.service_start_time
                <= timestamp
            )
            & (
                patients.service_end_time
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


def history_records(
    patients,
    start_time,
    end_time,
):
    rows = (
        patients
        .filter(
            (
                patients.arrival_time
                >= start_time
            )
            & (
                patients.arrival_time
                < end_time
            )
        )
        .orderBy(
            "arrival_time"
        )
        .collect()
    )

    return [
        (
            to_patient_type(
                row["patient_type"]
            ),
            float(
                row["service_minutes"]
            ),
        )
        for row in rows
    ]


def main():

    spark = create_spark_session(
        "ProbabilisticTimelineBacktest"
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

    timeline = (
        spark.read
        .format("delta")
        .load(
            FULL_TIMELINE_PATH
        )
        .orderBy(
            "timestamp"
        )
        .collect()
    )

    first_arrival = (
        patients
        .agg(
            {
                "arrival_time": "min"
            }
        )
        .collect()[0][0]
    )

    forecaster = (
        WeightedScenarioForecaster(
            older_weight=OLDER_WEIGHT,
            recent_weight=RECENT_WEIGHT,
        )
    )

    staffing_engine = (
        ProbabilisticStaffingEngine(
            config=config,
            baseline_staff=(
                BASELINE_STAFF
            ),
            queue_sla_minutes=(
                QUEUE_SLA_MINUTES
            ),
            target_sla_percentage=(
                TARGET_SLA_PERCENTAGE
            ),
            target_success_probability=(
                TARGET_SUCCESS_PROBABILITY
            ),
        )
    )

    output_rows = []

    print("=" * 120)
    print(
        "FULL TIMELINE PROBABILISTIC STAFFING BACKTEST"
    )
    print("=" * 120)

    for index, row in enumerate(
        timeline,
        start=1,
    ):
        timestamp = row["timestamp"]

        lookback_start = (
            timestamp
            - timedelta(
                minutes=LOOKBACK_MINUTES
            )
        )

        midpoint = (
            timestamp
            - timedelta(
                minutes=SEGMENT_MINUTES
            )
        )

        rule_staff = int(
            row["rule_additional_staff"]
            or 0
        )

        if lookback_start < first_arrival:

            output_rows.append(
                (
                    timestamp,
                    False,
                    "RULE_FALLBACK_WARMUP",
                    rule_staff,
                    BASELINE_STAFF
                    + rule_staff,
                    None,
                    None,
                    None,
                    None,
                    None,
                    TARGET_SUCCESS_PROBABILITY,
                )
            )

            continue

        older_records = history_records(
            patients=patients,
            start_time=lookback_start,
            end_time=midpoint,
        )

        recent_records = history_records(
            patients=patients,
            start_time=midpoint,
            end_time=timestamp,
        )

        profile = forecaster.fit(
            older_records=older_records,
            recent_records=recent_records,
            segment_minutes=(
                SEGMENT_MINUTES
            ),
        )

        (
            initial_waiting,
            initial_services,
        ) = build_current_state(
            patients,
            timestamp,
        )

        scenarios = []

        generated_counts = []

        for scenario_index in range(
            MONTE_CARLO_SCENARIOS
        ):
            arrivals, durations = (
                forecaster.generate_scenario(
                    profile=profile,
                    horizon_minutes=(
                        FORECAST_HORIZON_MINUTES
                    ),
                    seed=(
                        BASE_SEED
                        + index * 1000
                        + scenario_index
                    ),
                )
            )

            scenarios.append(
                (
                    arrivals,
                    durations,
                )
            )

            generated_counts.append(
                len(arrivals)
            )

        recommendation, results = (
            staffing_engine.recommend(
                scenarios=scenarios,
                max_additional_staff=(
                    MAX_ADDITIONAL_STAFF
                ),
                initial_waiting=(
                    initial_waiting
                ),
                initial_services=(
                    initial_services
                ),
            )
        )

        baseline_probability = (
            results[0]
            .sla_success_probability
        )

        if recommendation is not None:

            selected = recommendation

            forecast_decision = (
                "NO_ACTION_REQUIRED"
                if selected.additional_staff == 0
                else "PROBABILISTIC_INTERVENTION"
            )

        else:

            selected = max(
                results,
                key=lambda result: (
                    result
                    .sla_success_probability,
                    result
                    .average_sla_percentage,
                    -result
                    .additional_staff,
                ),
            )

            forecast_decision = (
                "BEST_EFFORT_CONFIDENCE_NOT_REACHED"
            )

        expected_arrivals = (
            profile.arrival_rate_per_minute
            * FORECAST_HORIZON_MINUTES
        )

        avg_generated_arrivals = (
            sum(generated_counts)
            / len(generated_counts)
        )

        output_rows.append(
            (
                timestamp,
                True,
                forecast_decision,
                int(
                    selected
                    .additional_staff
                ),
                int(
                    selected.staff_count
                ),
                float(
                    profile
                    .arrival_rate_per_minute
                ),
                float(
                    expected_arrivals
                ),
                float(
                    avg_generated_arrivals
                ),
                float(
                    baseline_probability
                ),
                float(
                    selected
                    .sla_success_probability
                ),
                TARGET_SUCCESS_PROBABILITY,
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
                "forecast_eligible",
                BooleanType(),
                False,
            ),
            StructField(
                "forecast_decision",
                StringType(),
                False,
            ),
            StructField(
                "forecast_additional_staff",
                IntegerType(),
                False,
            ),
            StructField(
                "forecast_total_staff",
                IntegerType(),
                False,
            ),
            StructField(
                "estimated_arrival_rate_per_minute",
                DoubleType(),
                True,
            ),
            StructField(
                "expected_future_arrivals",
                DoubleType(),
                True,
            ),
            StructField(
                "monte_carlo_avg_arrivals",
                DoubleType(),
                True,
            ),
            StructField(
                "baseline_sla_success_probability",
                DoubleType(),
                True,
            ),
            StructField(
                "selected_sla_success_probability",
                DoubleType(),
                True,
            ),
            StructField(
                "target_success_probability",
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
        f"Timeline windows written: "
        f"{len(output_rows)}"
    )

    print(
        f"Output: {OUTPUT_PATH}"
    )

    print("=" * 120)

    spark.stop()


if __name__ == "__main__":
    main()
