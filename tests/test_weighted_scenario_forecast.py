import pytest

from decision_engine.weighted_scenario_forecast import (
    WeightedScenarioForecaster,
)
from simulator.event_models import PatientType


def test_weighted_rate_favors_recent_history():

    forecaster = WeightedScenarioForecaster(
        older_weight=1.0,
        recent_weight=2.0,
    )

    older = [
        (
            PatientType.OUTPATIENT,
            5.0,
        )
        for _ in range(5)
    ]

    recent = [
        (
            PatientType.OUTPATIENT,
            5.0,
        )
        for _ in range(10)
    ]

    profile = forecaster.fit(
        older_records=older,
        recent_records=recent,
        segment_minutes=15.0,
    )

    simple_rate = (
        15 / 30.0
    )

    assert (
        profile.arrival_rate_per_minute
        > simple_rate
    )


def test_weighted_scenario_is_reproducible():

    forecaster = WeightedScenarioForecaster()

    profile = forecaster.fit(
        older_records=[
            (
                PatientType.OUTPATIENT,
                5.0,
            ),
        ],
        recent_records=[
            (
                PatientType.INPATIENT,
                8.0,
            ),
        ],
        segment_minutes=15.0,
    )

    first = forecaster.generate_scenario(
        profile=profile,
        horizon_minutes=30.0,
        seed=42,
    )

    second = forecaster.generate_scenario(
        profile=profile,
        horizon_minutes=30.0,
        seed=42,
    )

    assert first == second


def test_empty_history_generates_empty_scenario():

    forecaster = WeightedScenarioForecaster()

    profile = forecaster.fit(
        older_records=[],
        recent_records=[],
        segment_minutes=15.0,
    )

    arrivals, durations = (
        forecaster.generate_scenario(
            profile=profile,
            horizon_minutes=30.0,
            seed=42,
        )
    )

    assert arrivals == []
    assert durations == []
