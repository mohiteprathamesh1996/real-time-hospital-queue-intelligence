from datetime import datetime, timedelta

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

EPISODE_START = datetime.fromisoformat(
    "2026-09-03T10:10:00"
)

LOOKAHEAD_MINUTES = 30
BASELINE_STAFF = 4
MAX_ADDITIONAL_STAFF = 6


def main():
    spark = create_spark_session(
        "InspectCounterfactualEpisode"
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

    episode_end = (
        EPISODE_START
        + timedelta(
            minutes=LOOKAHEAD_MINUTES
        )
    )

    # ------------------------------------------------------------
    # Patients already waiting
    # ------------------------------------------------------------

    waiting_rows = (
        patients
        .filter(
            (patients.queue_entry_time <= EPISODE_START)
            & (
                patients.service_start_time
                > EPISODE_START
            )
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
                EPISODE_START
                - row["queue_entry_time"]
            ).total_seconds()
            / 60.0,
        )
        for row in waiting_rows
    ]

    # ------------------------------------------------------------
    # Patients already in service
    # ------------------------------------------------------------

    service_rows = (
        patients
        .filter(
            (
                patients.service_start_time
                <= EPISODE_START
            )
            & (
                patients.service_end_time
                > EPISODE_START
            )
        )
        .orderBy("service_start_time")
        .collect()
    )

    initial_services = [
        InitialService(
            remaining_service_minutes=(
                row["service_end_time"]
                - EPISODE_START
            ).total_seconds()
            / 60.0
        )
        for row in service_rows
    ]

    # ------------------------------------------------------------
    # Future arrivals
    # ------------------------------------------------------------

    future_rows = (
        patients
        .filter(
            (
                patients.arrival_time
                >= EPISODE_START
            )
            & (
                patients.arrival_time
                < episode_end
            )
        )
        .orderBy("arrival_time")
        .collect()
    )

    arrivals = [
        (
            (
                row["arrival_time"]
                - EPISODE_START
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

    # ------------------------------------------------------------
    # Counterfactual recommendation
    # ------------------------------------------------------------

    engine = CounterfactualStaffingEngine(
        config=config,
        baseline_staff=BASELINE_STAFF,
        queue_sla_minutes=15.0,
        target_sla_percentage=95.0,
    )

    recommendation, scenarios = (
        engine.recommend(
            arrivals=arrivals,
            service_durations=service_durations,
            initial_waiting=initial_waiting,
            initial_services=initial_services,
            max_additional_staff=(
                MAX_ADDITIONAL_STAFF
            ),
        )
    )

    # ------------------------------------------------------------
    # Output
    # ------------------------------------------------------------

    print("=" * 100)
    print("COUNTERFACTUAL STAFFING BACKTEST")
    print("=" * 100)

    print(
        f"Episode start:        "
        f"{EPISODE_START}"
    )

    print(
        f"Lookahead window:     "
        f"{LOOKAHEAD_MINUTES} minutes"
    )

    print(
        f"Initially waiting:    "
        f"{len(initial_waiting)}"
    )

    print(
        f"Initially in service: "
        f"{len(initial_services)}"
    )

    print(
        f"Future arrivals:      "
        f"{len(future_rows)}"
    )

    print(
        f"Baseline staff:       "
        f"{BASELINE_STAFF}"
    )

    print()

    print(
        f"{'Staff':<10}"
        f"{'Added':>10}"
        f"{'Avg Wait':>15}"
        f"{'P95 Wait':>15}"
        f"{'Max Wait':>15}"
        f"{'SLA':>12}"
        f"{'Status':>12}"
    )

    print("-" * 100)

    for scenario in scenarios:

        status = (
            "PASS"
            if scenario.meets_sla
            else "FAIL"
        )

        print(
            f"{scenario.staff_count:<10}"
            f"{scenario.additional_staff:>10}"
            f"{scenario.average_wait_minutes:>15.2f}"
            f"{scenario.p95_wait_minutes:>15.2f}"
            f"{scenario.max_wait_minutes:>15.2f}"
            f"{scenario.sla_percentage:>11.1f}%"
            f"{status:>12}"
        )

    print()
    print("=" * 100)
    print("DECISION")
    print("=" * 100)

    print(
        f"Decision:                 "
        f"{recommendation.decision}"
    )

    print(
        f"Already breached:         "
        f"{recommendation.already_breached_patients}"
    )

    print(
        f"Maximum possible SLA:     "
        f"{recommendation.maximum_possible_sla_percentage:.1f}%"
    )

    print(
        f"Reason:                   "
        f"{recommendation.reason}"
    )

    if (
        recommendation.recommended_result
        is not None
    ):
        result = (
            recommendation.recommended_result
        )

        print()
        print(
            f"Recommended additional:   "
            f"+{result.additional_staff}"
        )

        print(
            f"Recommended total staff:  "
            f"{result.staff_count}"
        )

        print(
            f"Expected SLA:              "
            f"{result.sla_percentage:.1f}%"
        )

        print(
            f"Expected P95 wait:         "
            f"{result.p95_wait_minutes:.2f} minutes"
        )

    spark.stop()


if __name__ == "__main__":
    main()