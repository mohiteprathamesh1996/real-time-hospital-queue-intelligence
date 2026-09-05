from datetime import datetime, timedelta

from config.settings import load_config
from decision_engine.arrival_forecast import (
    RecentHistoryForecaster,
)
from decision_engine.probabilistic_staffing import (
    ProbabilisticStaffingEngine,
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

DECISION_TIME = datetime.fromisoformat(
    "2026-09-03T10:10:00"
)

LOOKBACK_MINUTES = 60
FORECAST_HORIZON_MINUTES = 30

BASELINE_STAFF = 4
MAX_ADDITIONAL_STAFF = 6

QUEUE_SLA_MINUTES = 15.0
TARGET_SLA_PERCENTAGE = 95.0

TARGET_SUCCESS_PROBABILITY = 0.95

MONTE_CARLO_SCENARIOS = 200

RANDOM_SEED = 42000


def to_patient_type(value):

    if isinstance(
        value,
        PatientType,
    ):
        return value

    return PatientType(value)


def main():

    spark = create_spark_session(
        "ProbabilisticStaffingExperiment"
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

    # ============================================================
    # CURRENT LIVE STATE
    # ============================================================

    waiting_rows = (
        patients
        .filter(
            (
                patients.queue_entry_time
                <= DECISION_TIME
            )
            & (
                patients.service_start_time
                > DECISION_TIME
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
                    DECISION_TIME
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
                <= DECISION_TIME
            )
            & (
                patients.service_end_time
                > DECISION_TIME
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
                    - DECISION_TIME
                ).total_seconds()
                / 60.0
            )
        )
        for row in service_rows
    ]

    # ============================================================
    # ONLY INFORMATION AVAILABLE BEFORE DECISION TIME
    # ============================================================

    lookback_start = (
        DECISION_TIME
        - timedelta(
            minutes=LOOKBACK_MINUTES
        )
    )

    history_rows = (
        patients
        .filter(
            (
                patients.arrival_time
                >= lookback_start
            )
            & (
                patients.arrival_time
                < DECISION_TIME
            )
        )
        .orderBy(
            "arrival_time"
        )
        .collect()
    )

    historical_patient_types = [
        to_patient_type(
            row["patient_type"]
        )
        for row in history_rows
    ]

    historical_service_durations = [
        float(
            row["service_minutes"]
        )
        for row in history_rows
    ]

    forecaster = (
        RecentHistoryForecaster()
    )

    profile = forecaster.fit(
        patient_types=(
            historical_patient_types
        ),
        service_durations=(
            historical_service_durations
        ),
        lookback_minutes=(
            LOOKBACK_MINUTES
        ),
    )

    # ============================================================
    # MONTE CARLO FUTURE SCENARIOS
    # ============================================================

    scenarios = []

    for scenario_index in range(
        MONTE_CARLO_SCENARIOS
    ):

        scenario = (
            forecaster.generate_scenario(
                profile=profile,
                horizon_minutes=(
                    FORECAST_HORIZON_MINUTES
                ),
                seed=(
                    RANDOM_SEED
                    + scenario_index
                ),
            )
        )

        scenarios.append(
            scenario
        )

    # ============================================================
    # PROBABILISTIC STAFFING DECISION
    # ============================================================

    engine = (
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

    recommendation, results = (
        engine.recommend(
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

    expected_arrivals = (
        profile.arrival_rate_per_minute
        * FORECAST_HORIZON_MINUTES
    )

    actual_scenario_counts = [
        len(arrivals)
        for arrivals, _ in scenarios
    ]

    average_generated_arrivals = (
        sum(actual_scenario_counts)
        / len(actual_scenario_counts)
    )

    # ============================================================
    # REPORT
    # ============================================================

    print("=" * 110)
    print(
        "PROBABILISTIC STAFFING DECISION"
    )
    print("=" * 110)

    print(
        f"Decision time:              "
        f"{DECISION_TIME}"
    )

    print(
        f"Historical lookback:        "
        f"{LOOKBACK_MINUTES} minutes"
    )

    print(
        f"Forecast horizon:           "
        f"{FORECAST_HORIZON_MINUTES} minutes"
    )

    print(
        f"Patients in lookback:       "
        f"{len(history_rows)}"
    )

    print(
        f"Estimated arrivals/minute:  "
        f"{profile.arrival_rate_per_minute:.3f}"
    )

    print(
        f"Expected future arrivals:   "
        f"{expected_arrivals:.1f}"
    )

    print(
        f"Monte Carlo avg arrivals:   "
        f"{average_generated_arrivals:.1f}"
    )

    print(
        f"Monte Carlo scenarios:      "
        f"{MONTE_CARLO_SCENARIOS}"
    )

    print()

    print(
        f"Initially waiting:          "
        f"{len(initial_waiting)}"
    )

    print(
        f"Initially in service:       "
        f"{len(initial_services)}"
    )

    print(
        f"Baseline staff:             "
        f"{BASELINE_STAFF}"
    )

    print(
        f"Required confidence:        "
        f"{TARGET_SUCCESS_PROBABILITY * 100:.1f}%"
    )

    print()

    print("-" * 110)

    print(
        f"{'Staff':<10}"
        f"{'Added':>8}"
        f"{'P(SLA Pass)':>15}"
        f"{'Avg SLA':>15}"
        f"{'Avg P95':>15}"
        f"{'Avg Wait':>15}"
        f"{'Decision':>15}"
    )

    print("-" * 110)

    for result in results:

        status = (
            "PASS"
            if result.meets_confidence_target
            else "FAIL"
        )

        print(
            f"{result.staff_count:<10}"
            f"{result.additional_staff:>8}"
            f"{result.sla_success_probability * 100:>14.1f}%"
            f"{result.average_sla_percentage:>14.1f}%"
            f"{result.average_p95_wait_minutes:>15.2f}"
            f"{result.average_wait_minutes:>15.2f}"
            f"{status:>15}"
        )

    print()

    print("=" * 110)
    print("RECOMMENDATION")
    print("=" * 110)

    if recommendation is None:

        print(
            "No tested staffing level achieved "
            "the required confidence."
        )

    else:

        print(
            f"Deploy +"
            f"{recommendation.additional_staff} "
            f"staff member(s)"
        )

        print(
            f"Total staff:                "
            f"{recommendation.staff_count}"
        )

        print(
            f"Probability of SLA success: "
            f"{recommendation.sla_success_probability * 100:.1f}%"
        )

        print(
            f"Average predicted SLA:      "
            f"{recommendation.average_sla_percentage:.1f}%"
        )

        print(
            f"Average predicted P95:      "
            f"{recommendation.average_p95_wait_minutes:.2f} minutes"
        )

    print()

    print(
        "NOTE: Future arrivals and service times "
        "are simulated exclusively from information "
        "available before the decision timestamp."
    )

    print("=" * 110)

    spark.stop()


if __name__ == "__main__":
    main()
