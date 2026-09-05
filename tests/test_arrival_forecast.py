import pytest

from decision_engine.arrival_forecast import (
    RecentHistoryForecaster,
)
from simulator.event_models import PatientType


def test_arrival_rate_is_estimated_from_lookback():

    forecaster = RecentHistoryForecaster()

    patient_types = [
        PatientType.OUTPATIENT
        for _ in range(6)
    ]

    service_durations = [
        5.0
        for _ in range(6)
    ]

    profile = forecaster.fit(
        patient_types=patient_types,
        service_durations=service_durations,
        lookback_minutes=60.0,
    )

    assert (
        profile.arrival_rate_per_minute
        == pytest.approx(0.1)
    )


def test_forecast_is_reproducible_with_same_seed():

    forecaster = RecentHistoryForecaster()

    profile = forecaster.fit(
        patient_types=[
            PatientType.OUTPATIENT,
            PatientType.INPATIENT,
        ],
        service_durations=[
            5.0,
            8.0,
        ],
        lookback_minutes=10.0,
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


def test_empty_history_generates_no_arrivals():

    forecaster = RecentHistoryForecaster()

    profile = forecaster.fit(
        patient_types=[],
        service_durations=[],
        lookback_minutes=60.0,
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
