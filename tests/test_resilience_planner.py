import pytest

from simulator.resilience_planner import (
    ResiliencePlanner,
    ResilienceResult,
)


def test_evaluate_meets_sla():

    planner = ResiliencePlanner(
        target_sla_percentage=95.0
    )

    result = planner.evaluate(
        demand_scenario="Baseline",
        planned_staff=5,
        unavailable_staff=1,
        available_staff=4,
        sla_percentage=98.0,
        average_wait_minutes=2.07,
        p95_wait_minutes=9.32,
    )

    assert result.meets_sla is True
    assert result.available_staff == 4


def test_evaluate_fails_sla():

    planner = ResiliencePlanner(
        target_sla_percentage=95.0
    )

    result = planner.evaluate(
        demand_scenario="Baseline",
        planned_staff=4,
        unavailable_staff=1,
        available_staff=3,
        sla_percentage=38.5,
        average_wait_minutes=21.20,
        p95_wait_minutes=51.36,
    )

    assert result.meets_sla is False


def test_minimum_resilient_staffing():

    planner = ResiliencePlanner(
        target_sla_percentage=95.0
    )

    results = [
        ResilienceResult(
            demand_scenario="Baseline",
            planned_staff=4,
            unavailable_staff=1,
            available_staff=3,
            sla_percentage=38.5,
            average_wait_minutes=21.2,
            p95_wait_minutes=51.36,
            meets_sla=False,
        ),
        ResilienceResult(
            demand_scenario="Baseline",
            planned_staff=5,
            unavailable_staff=1,
            available_staff=4,
            sla_percentage=98.0,
            average_wait_minutes=2.07,
            p95_wait_minutes=9.32,
            meets_sla=True,
        ),
    ]

    minimum = planner.minimum_resilient_staffing(
        results,
        unavailable_staff=1,
    )

    assert minimum == 5


def test_no_feasible_staffing():

    planner = ResiliencePlanner(
        target_sla_percentage=95.0
    )

    results = [
        ResilienceResult(
            demand_scenario="Baseline",
            planned_staff=4,
            unavailable_staff=1,
            available_staff=3,
            sla_percentage=38.5,
            average_wait_minutes=21.2,
            p95_wait_minutes=51.36,
            meets_sla=False,
        ),
    ]

    assert (
        planner.minimum_resilient_staffing(
            results,
            unavailable_staff=1,
        )
        is None
    )