from datetime import datetime, timedelta

from config.settings import load_config
from decision_engine.counterfactual_engine import (
    CounterfactualStaffingEngine,
)
from simulator.simpy_engine import InitialService
from streaming.spark_session import create_spark_session


PATIENT_METRICS_PATH = "./data/gold/patient_metrics"

EPISODE_START = datetime.fromisoformat(
    "2026-09-03T10:10:00"
)

LOOKAHEAD_MINUTES = 30

BASELINE_STAFF = 4


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
    # 1. Patients already waiting at decision time
    # ------------------------------------------------------------

    waiting_rows = (
        patients
        .filter(
            (patients.queue_entry_time <= EPISODE_START)
            & (patients.service_start_time > EPISODE_START)
        )
        .orderBy("queue_entry_time")
        .collect()
    )

    initial_waiting = [
        (
            row["patient_type"],
            float(row["service_minutes"]),
        )
        for row in waiting_rows
    ]

    # ------------------------------------------------------------
    # 2. Patients already in service at decision time
    # ------------------------------------------------------------

    service_rows = (
        patients
        .filter(
            (patients.service_start_time <= EPISODE_START)
            & (patients.service_end_time > EPISODE_START)
        )
        .orderBy("service_start_time")
        .collect()
    )

    initial_services = [
        InitialService(
            remaining_service_minutes=(
                row["service_end_time"]
                - EPISODE_START
            ).total_seconds() / 60.0
        )
        for row in service_rows
    ]

    # ------------------------------------------------------------
    # 3. Future arrivals during the lookahead window
    # ------------------------------------------------------------

    future_rows = (
        patients
        .filter(
            (patients.arrival_time >= EPISODE_START)
            & (patients.arrival_time < episode_end)
        )
        .orderBy("arrival_time")
        .collect()
    )

    arrivals = [
        (
            (
                row["arrival_time"]
                - EPISODE_START
            ).total_seconds() / 60.0,
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
    # Counterfactual engine
    # ------------------------------------------------------------

    engine = CounterfactualStaffingEngine(
        config=config,
        baseline_staff=BASELINE_STAFF,
        queue_sla_minutes=15,
        target_sla_percentage=95,
    )

    recommendation, scenarios = (
        engine.recommend(
            arrivals=arrivals,
            service_durations=service_durations,
            initial_waiting=initial_waiting,
            initial_services=initial_services,
            max_additional_staff=3,
        )
    )

    # ------------------------------------------------------------
    # Output
    # ------------------------------------------------------------

    print("=" * 100)
    print("COUNTERFACTUAL STAFFING BACKTEST")
    print("=" * 100)

    print(
        f"Episode start:        {EPISODE_START}"
    )

    print(
        f"Lookahead window:     {LOOKAHEAD_MINUTES} minutes"
    )

    print(
        f"Initially waiting:    {len(initial_waiting)}"
    )

    print(
        f"Initially in service: {len(initial_services)}"
    )

    print(
        f"Future arrivals:      {len(future_rows)}"
    )

    print(
        f"Baseline staff:       {BASELINE_STAFF}"
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
    print("RECOMMENDATION")
    print("=" * 100)

    if recommendation is None:
        print(
            "No tested staffing scenario restored the SLA."
        )
    else:
        print(
            f"Deploy +{recommendation.additional_staff} "
            f"staff member(s)"
        )

        print(
            f"Total staff:       "
            f"{recommendation.staff_count}"
        )

        print(
            f"Expected SLA:      "
            f"{recommendation.sla_percentage:.1f}%"
        )

        print(
            f"Expected P95 wait: "
            f"{recommendation.p95_wait_minutes:.2f} minutes"
        )

    spark.stop()


if __name__ == "__main__":
    main()