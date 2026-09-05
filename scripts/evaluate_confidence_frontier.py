from datetime import timedelta

from config.settings import load_config
from decision_engine.cost_model import (
    StaffingCostModel,
)
from decision_engine.counterfactual_engine import (
    CounterfactualStaffingEngine,
)
from decision_engine.intervention_policy import (
    StatefulInterventionPolicy,
)
from decision_engine.intervention_state import (
    InterventionState,
)
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

ORACLE_PATH = (
    "./data/gold/full_timeline_counterfactual"
)


LOOKBACK_MINUTES = 30
SEGMENT_MINUTES = 15

FORECAST_HORIZON_MINUTES = 30

MONTE_CARLO_SCENARIOS = 200

BASELINE_STAFF = 4
MAX_ADDITIONAL_STAFF = 6

QUEUE_SLA_MINUTES = 15.0
TARGET_SLA_PERCENTAGE = 95.0

OLDER_WEIGHT = 1.0
RECENT_WEIGHT = 2.0

MINIMUM_HOLD_MINUTES = 15
WINDOW_MINUTES = 5

BASE_SEED = 51000


CONFIDENCE_THRESHOLDS = [
    0.80,
    0.85,
    0.90,
    0.95,
    0.975,
    0.99,
]


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


def build_actual_future(
    patients,
    timestamp,
):

    end_time = (
        timestamp
        + timedelta(
            minutes=(
                FORECAST_HORIZON_MINUTES
            )
        )
    )

    rows = (
        patients
        .filter(
            (
                patients.arrival_time
                >= timestamp
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

    arrivals = [
        (
            (
                (
                    row["arrival_time"]
                    - timestamp
                ).total_seconds()
                / 60.0
            ),
            to_patient_type(
                row["patient_type"]
            ),
        )
        for row in rows
    ]

    durations = [
        float(
            row["service_minutes"]
        )
        for row in rows
    ]

    return (
        arrivals,
        durations,
    )


def apply_stateful_policy(
    recommendations,
):

    policy = StatefulInterventionPolicy(
        minimum_hold_minutes=(
            MINIMUM_HOLD_MINUTES
        )
    )

    state = InterventionState()

    active_values = []

    for (
        timestamp,
        recommended_staff,
    ) in recommendations:

        policy.apply(
            timestamp=timestamp,
            recommended_additional_staff=(
                recommended_staff
            ),
            state=state,
        )

        active_values.append(
            state.active_additional_staff
        )

    return active_values


def main():

    spark = create_spark_session(
        "EvaluateConfidenceFrontier"
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

    patients = (
        spark.read
        .format("delta")
        .load(
            PATIENT_METRICS_PATH
        )
    )

    oracle_rows = (
        spark.read
        .format("delta")
        .load(
            ORACLE_PATH
        )
        .select(
            "timestamp",
            "model_additional_staff",
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
            older_weight=(
                OLDER_WEIGHT
            ),
            recent_weight=(
                RECENT_WEIGHT
            ),
        )
    )

    deterministic_engine = (
        CounterfactualStaffingEngine(
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
        )
    )

    eligible_windows = []

    print("=" * 120)
    print(
        "BUILDING COMMON MONTE CARLO SCENARIOS"
    )
    print("=" * 120)

    for index, row in enumerate(
        oracle_rows,
        start=1,
    ):

        timestamp = (
            row["timestamp"]
        )

        lookback_start = (
            timestamp
            - timedelta(
                minutes=(
                    LOOKBACK_MINUTES
                )
            )
        )

        if lookback_start < first_arrival:
            continue

        midpoint = (
            timestamp
            - timedelta(
                minutes=(
                    SEGMENT_MINUTES
                )
            )
        )

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
            older_records=(
                older_records
            ),
            recent_records=(
                recent_records
            ),
            segment_minutes=(
                SEGMENT_MINUTES
            ),
        )

        scenarios = []

        for scenario_index in range(
            MONTE_CARLO_SCENARIOS
        ):

            scenarios.append(
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

        (
            initial_waiting,
            initial_services,
        ) = build_current_state(
            patients,
            timestamp,
        )

        (
            actual_arrivals,
            actual_durations,
        ) = build_actual_future(
            patients,
            timestamp,
        )

        eligible_windows.append(
            {
                "timestamp": (
                    timestamp
                ),
                "oracle_staff": int(
                    row[
                        "model_additional_staff"
                    ]
                    or 0
                ),
                "scenarios": scenarios,
                "initial_waiting": (
                    initial_waiting
                ),
                "initial_services": (
                    initial_services
                ),
                "actual_arrivals": (
                    actual_arrivals
                ),
                "actual_durations": (
                    actual_durations
                ),
            }
        )

        if (
            index % 20 == 0
            or index == len(
                oracle_rows
            )
        ):
            print(
                f"Prepared window "
                f"{index}/{len(oracle_rows)}"
            )

    results = []

    print()
    print("=" * 120)
    print(
        "EVALUATING CONFIDENCE THRESHOLDS"
    )
    print("=" * 120)

    for threshold in (
        CONFIDENCE_THRESHOLDS
    ):

        probabilistic_engine = (
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
                    threshold
                ),
            )
        )

        recommendations = []

        exact = 0
        over = 0
        under = 0

        absolute_errors = []

        actual_sla_passes = 0

        for window in eligible_windows:

            (
                recommendation,
                scenarios_evaluated,
            ) = (
                probabilistic_engine
                .recommend(
                    scenarios=(
                        window["scenarios"]
                    ),
                    max_additional_staff=(
                        MAX_ADDITIONAL_STAFF
                    ),
                    initial_waiting=(
                        window[
                            "initial_waiting"
                        ]
                    ),
                    initial_services=(
                        window[
                            "initial_services"
                        ]
                    ),
                )
            )

            if (
                recommendation
                is None
            ):

                recommendation = max(
                    scenarios_evaluated,
                    key=lambda result: (
                        result
                        .sla_success_probability,
                        result
                        .average_sla_percentage,
                        -result
                        .additional_staff,
                    ),
                )

            forecast_staff = int(
                recommendation
                .additional_staff
            )

            oracle_staff = int(
                window["oracle_staff"]
            )

            difference = (
                forecast_staff
                - oracle_staff
            )

            absolute_errors.append(
                abs(difference)
            )

            if difference == 0:
                exact += 1
            elif difference > 0:
                over += 1
            else:
                under += 1

            recommendations.append(
                (
                    window["timestamp"],
                    forecast_staff,
                )
            )

            actual_result = (
                deterministic_engine
                .evaluate(
                    arrivals=(
                        window[
                            "actual_arrivals"
                        ]
                    ),
                    service_durations=(
                        window[
                            "actual_durations"
                        ]
                    ),
                    additional_staff=(
                        forecast_staff
                    ),
                    initial_waiting=(
                        window[
                            "initial_waiting"
                        ]
                    ),
                    initial_services=(
                        window[
                            "initial_services"
                        ]
                    ),
                )
            )

            if actual_result.meets_sla:
                actual_sla_passes += 1

        active_staff = (
            apply_stateful_policy(
                recommendations
            )
        )

        staff_window_units = sum(
            active_staff
        )

        staff_hours = (
            staff_window_units
            * WINDOW_MINUTES
            / 60.0
        )

        cost = (
            cost_model
            .calculate(
                staff_hours=(
                    staff_hours
                )
            )
            .total_cost
        )

        observation_count = len(
            eligible_windows
        )

        agreement_rate = (
            exact
            / observation_count
            * 100
            if observation_count
            else 0.0
        )

        actual_sla_rate = (
            actual_sla_passes
            / observation_count
            * 100
            if observation_count
            else 0.0
        )

        mae_staff = (
            sum(absolute_errors)
            / len(absolute_errors)
            if absolute_errors
            else 0.0
        )

        peak_extra_staff = max(
            active_staff,
            default=0,
        )

        results.append(
            {
                "threshold": threshold,
                "windows": (
                    observation_count
                ),
                "staff_hours": (
                    staff_hours
                ),
                "cost": cost,
                "actual_sla_rate": (
                    actual_sla_rate
                ),
                "agreement_rate": (
                    agreement_rate
                ),
                "over": over,
                "under": under,
                "staff_mae": (
                    mae_staff
                ),
                "peak_extra_staff": (
                    peak_extra_staff
                ),
            }
        )

        print(
            f"Completed confidence "
            f"{threshold * 100:.1f}%"
        )

    print()

    print("=" * 140)
    print(
        "CONFIDENCE / COST / SLA FRONTIER"
    )
    print("=" * 140)

    print(
        f"{'Confidence':<12}"
        f"{'Staff Hrs':>12}"
        f"{'Cost':>12}"
        f"{'Actual SLA':>14}"
        f"{'Oracle Agree':>15}"
        f"{'Over':>8}"
        f"{'Under':>8}"
        f"{'Staff MAE':>12}"
        f"{'Peak':>8}"
    )

    print("-" * 105)

    for result in results:

        print(
            f"{result['threshold'] * 100:<11.1f}%"
            f"{result['staff_hours']:>12.2f}"
            f"${result['cost']:>11.2f}"
            f"{result['actual_sla_rate']:>13.1f}%"
            f"{result['agreement_rate']:>14.1f}%"
            f"{result['over']:>8}"
            f"{result['under']:>8}"
            f"{result['staff_mae']:>12.2f}"
            f"{result['peak_extra_staff']:>8}"
        )

    eligible_choices = [
        result
        for result in results
        if (
            result[
                "actual_sla_rate"
            ]
            >= TARGET_SLA_PERCENTAGE
        )
    ]

    if eligible_choices:

        selected = min(
            eligible_choices,
            key=lambda result: (
                result[
                    "staff_hours"
                ],
                result["cost"],
                -result[
                    "actual_sla_rate"
                ],
            ),
        )

        print()

        print("=" * 140)
        print(
            "RECOMMENDED OPERATIONAL CONFIDENCE"
        )
        print("=" * 140)

        print(
            f"Confidence threshold: "
            f"{selected['threshold'] * 100:.1f}%"
        )

        print(
            f"Actual SLA success:   "
            f"{selected['actual_sla_rate']:.1f}%"
        )

        print(
            f"Staff-hours:          "
            f"{selected['staff_hours']:.2f}"
        )

        print(
            f"Estimated cost:       "
            f"${selected['cost']:.2f}"
        )

        print(
            f"Oracle agreement:     "
            f"{selected['agreement_rate']:.1f}%"
        )

        print(
            f"Overstaff windows:    "
            f"{selected['over']}"
        )

        print(
            f"Understaff windows:   "
            f"{selected['under']}"
        )

        print()

        print(
            "Selection rule: among thresholds "
            "whose realized historical SLA success "
            "meets or exceeds the 95% service target, "
            "choose the lowest modeled staff-hour "
            "consumption."
        )

    else:

        print()

        print(
            "No tested confidence threshold "
            "achieved the historical 95% SLA "
            "success target."
        )

    print("=" * 140)

    spark.stop()


if __name__ == "__main__":
    main()
