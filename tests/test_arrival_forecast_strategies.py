import pytest

from decision_engine.arrival_forecast_strategies import (
    RollingRateForecaster,
    TrendAdjustedRateForecaster,
    WeightedRecentRateForecaster,
)


def test_rolling_rate_forecast():

    forecaster = (
        RollingRateForecaster()
    )

    result = forecaster.forecast(
        arrival_count=30,
        lookback_minutes=30.0,
        horizon_minutes=30.0,
    )

    assert (
        result.predicted_arrivals
        == pytest.approx(30.0)
    )


def test_weighted_forecast_favors_recent_period():

    forecaster = (
        WeightedRecentRateForecaster(
            older_weight=1.0,
            recent_weight=2.0,
        )
    )

    result = forecaster.forecast(
        older_count=5,
        recent_count=10,
        segment_minutes=15.0,
        horizon_minutes=30.0,
    )

    simple_average_prediction = (
        (
            (5 + 10)
            / 30.0
        )
        * 30.0
    )

    assert (
        result.predicted_arrivals
        > simple_average_prediction
    )


def test_trend_adjusted_increases_forecast_when_demand_rises():

    forecaster = (
        TrendAdjustedRateForecaster(
            trend_strength=0.5
        )
    )

    result = forecaster.forecast(
        older_count=5,
        recent_count=10,
        segment_minutes=15.0,
        horizon_minutes=30.0,
    )

    recent_rate_prediction = (
        10 / 15.0 * 30.0
    )

    assert (
        result.predicted_arrivals
        > recent_rate_prediction
    )


def test_trend_adjusted_clips_negative_rate():

    forecaster = (
        TrendAdjustedRateForecaster(
            trend_strength=2.0
        )
    )

    result = forecaster.forecast(
        older_count=20,
        recent_count=2,
        segment_minutes=15.0,
        horizon_minutes=30.0,
    )

    assert (
        result.predicted_arrivals
        >= 0.0
    )
