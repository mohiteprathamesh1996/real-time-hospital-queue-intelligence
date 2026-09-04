from datetime import datetime, timedelta

import pytest

from config.settings import load_config
from simulator.capacity_scenarios import (
    CapacityScenarioGenerator,
)


def create_generator():
    config = load_config(
        "config/hospital.yaml"
    )

    start_time = datetime.fromisoformat(
        config.simulation.start_time
    )

    end_time = (
        start_time
        + timedelta(
            hours=config.simulation.duration_hours
        )
    )

    return CapacityScenarioGenerator(
        config=config,
        start_time=start_time,
        end_time=end_time,
        seed=42,
    )


def test_baseline_scenario_generates_arrivals():

    generator = create_generator()

    scenario = generator.generate(
        name="baseline",
        demand_multiplier=1.0,
    )

    assert scenario.name == "baseline"
    assert scenario.demand_multiplier == 1.0
    assert len(scenario.arrivals) > 0


def test_higher_demand_generates_more_patients():

    generator = create_generator()

    baseline = generator.generate(
        name="baseline",
        demand_multiplier=1.0,
    )

    high_demand = generator.generate(
        name="high_demand",
        demand_multiplier=1.5,
    )

    assert len(high_demand.arrivals) > len(
        baseline.arrivals
    )


def test_invalid_multiplier_raises_error():

    generator = create_generator()

    with pytest.raises(ValueError):
        generator.generate(
            name="invalid",
            demand_multiplier=0,
        )