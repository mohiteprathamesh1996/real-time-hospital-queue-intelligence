from config.settings import load_config
from decision_engine.counterfactual_engine import (
    CounterfactualStaffingEngine,
)
from simulator.event_models import PatientType


def create_engine():
    config = load_config(
        "config/hospital.yaml"
    )

    return CounterfactualStaffingEngine(
        config=config,
        baseline_staff=4,
        queue_sla_minutes=15,
        target_sla_percentage=95,
    )


def test_counterfactual_returns_staff_count():

    engine = create_engine()

    arrivals = [
        (0.0, PatientType.OUTPATIENT),
        (1.0, PatientType.OUTPATIENT),
        (2.0, PatientType.OUTPATIENT),
    ]

    service_durations = [
        5.0,
        5.0,
        5.0,
    ]

    result = engine.evaluate(
        arrivals,
        service_durations,
        additional_staff=1,
    )

    assert result.staff_count == 5
    assert result.additional_staff == 1


def test_extra_capacity_does_not_increase_wait():

    engine = create_engine()

    arrivals = [
        (0.0, PatientType.OUTPATIENT)
        for _ in range(10)
    ]

    service_durations = [
        10.0
        for _ in range(10)
    ]

    baseline = engine.evaluate(
        arrivals,
        service_durations,
        additional_staff=0,
    )

    expanded = engine.evaluate(
        arrivals,
        service_durations,
        additional_staff=2,
    )

    assert (
        expanded.average_wait_minutes
        <= baseline.average_wait_minutes
    )


def test_recommend_returns_first_feasible_scenario():

    engine = create_engine()

    arrivals = [
        (0.0, PatientType.OUTPATIENT)
        for _ in range(8)
    ]

    service_durations = [
        5.0
        for _ in range(8)
    ]

    recommendation, scenarios = (
        engine.recommend(
            arrivals,
            service_durations,
            max_additional_staff=3,
        )
    )

    assert recommendation is not None
    assert recommendation.meets_sla is True

    assert all(
        not scenario.meets_sla
        for scenario in scenarios[:-1]
    )