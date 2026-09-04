import pytest

from simulator.capacity_planner import CapacityPlanner


def create_planner():
    return CapacityPlanner(
        queue_sla_minutes=15,
        target_sla_percentage=95,
    )


def test_meets_sla():

    planner = create_planner()

    result = planner.evaluate(
        scenario_name="Baseline",
        demand_multiplier=1.0,
        patient_count=301,
        staff_count=4,
        average_wait_minutes=2.07,
        p95_wait_minutes=9.32,
        sla_percentage=98.0,
        utilization_percentage=56.1,
    )

    assert result.meets_sla is True


def test_fails_sla():

    planner = create_planner()

    result = planner.evaluate(
        scenario_name="High Demand",
        demand_multiplier=1.6,
        patient_count=517,
        staff_count=6,
        average_wait_minutes=3.70,
        p95_wait_minutes=15.11,
        sla_percentage=94.6,
        utilization_percentage=64.7,
    )

    assert result.meets_sla is False


def test_minimum_staffing():

    planner = create_planner()

    results = [
        planner.evaluate(
            "Baseline",
            1.0,
            301,
            3,
            21.20,
            51.36,
            38.5,
            74.8,
        ),
        planner.evaluate(
            "Baseline",
            1.0,
            301,
            4,
            2.07,
            9.32,
            98.0,
            56.1,
        ),
        planner.evaluate(
            "Baseline",
            1.0,
            301,
            5,
            0.56,
            3.60,
            100.0,
            44.9,
        ),
    ]

    assert planner.minimum_staffing(results) == 4


def test_no_feasible_staffing():

    planner = create_planner()

    results = [
        planner.evaluate(
            "High Demand",
            1.6,
            517,
            5,
            17.75,
            45.71,
            48.2,
            77.6,
        ),
        planner.evaluate(
            "High Demand",
            1.6,
            517,
            6,
            3.70,
            15.11,
            94.6,
            64.7,
        ),
    ]

    assert planner.minimum_staffing(results) is None