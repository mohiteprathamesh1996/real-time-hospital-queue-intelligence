import pytest

from simulator.staffing_optimizer import (
    StaffingOptimizer,
    StaffingScenario,
)


def create_optimizer():
    return StaffingOptimizer(
        hourly_cost_per_staff=35,
        target_sla_percentage=95,
        queue_sla_minutes=15,
    )


def test_scenario_meets_sla():

    optimizer = create_optimizer()

    scenario = optimizer.evaluate_scenario(
        staff_count=4,
        average_wait_minutes=2.07,
        p95_wait_minutes=9.32,
        max_wait_minutes=17.52,
        sla_percentage=98.0,
        utilization_percentage=56.1,
    )

    assert scenario.meets_sla is True
    assert scenario.hourly_staffing_cost == 140


def test_scenario_fails_sla():

    optimizer = create_optimizer()

    scenario = optimizer.evaluate_scenario(
        staff_count=3,
        average_wait_minutes=21.2,
        p95_wait_minutes=51.36,
        max_wait_minutes=61.39,
        sla_percentage=38.5,
        utilization_percentage=74.8,
    )

    assert scenario.meets_sla is False


def test_minimum_staffing_is_selected():

    optimizer = create_optimizer()

    scenarios = [
        optimizer.evaluate_scenario(
            staff_count=2,
            average_wait_minutes=115.41,
            p95_wait_minutes=205.75,
            max_wait_minutes=218.67,
            sla_percentage=15.6,
            utilization_percentage=112.2,
        ),
        optimizer.evaluate_scenario(
            staff_count=3,
            average_wait_minutes=21.2,
            p95_wait_minutes=51.36,
            max_wait_minutes=61.39,
            sla_percentage=38.5,
            utilization_percentage=74.8,
        ),
        optimizer.evaluate_scenario(
            staff_count=4,
            average_wait_minutes=2.07,
            p95_wait_minutes=9.32,
            max_wait_minutes=17.52,
            sla_percentage=98.0,
            utilization_percentage=56.1,
        ),
        optimizer.evaluate_scenario(
            staff_count=5,
            average_wait_minutes=0.56,
            p95_wait_minutes=3.60,
            max_wait_minutes=7.46,
            sla_percentage=100.0,
            utilization_percentage=44.9,
        ),
    ]

    optimal = optimizer.find_minimum_staffing(
        scenarios
    )

    assert optimal.staff_count == 4
    assert optimal.hourly_staffing_cost == 140


def test_no_feasible_staffing_raises_error():

    optimizer = create_optimizer()

    scenarios = [
        optimizer.evaluate_scenario(
            staff_count=2,
            average_wait_minutes=100,
            p95_wait_minutes=200,
            max_wait_minutes=250,
            sla_percentage=50,
            utilization_percentage=110,
        ),
        optimizer.evaluate_scenario(
            staff_count=3,
            average_wait_minutes=50,
            p95_wait_minutes=100,
            max_wait_minutes=150,
            sla_percentage=80,
            utilization_percentage=80,
        ),
    ]

    with pytest.raises(ValueError):
        optimizer.find_minimum_staffing(
            scenarios
        )