import pytest

from decision_engine.forecast_metrics import (
    calculate_forecast_metrics,
)


def test_forecast_metrics_are_correct():

    result = calculate_forecast_metrics(
        actual_values=[
            10.0,
            20.0,
        ],
        predicted_values=[
            12.0,
            18.0,
        ],
    )

    assert result.observations == 2

    assert result.mae == pytest.approx(
        2.0
    )

    assert result.bias == pytest.approx(
        0.0
    )

    assert result.rmse == pytest.approx(
        2.0
    )

    assert result.mape == pytest.approx(
        15.0
    )


def test_empty_input_is_supported():

    result = calculate_forecast_metrics(
        actual_values=[],
        predicted_values=[],
    )

    assert result.observations == 0
    assert result.mae == 0.0
    assert result.bias == 0.0
    assert result.rmse == 0.0
    assert result.mape is None


def test_length_mismatch_is_rejected():

    with pytest.raises(ValueError):
        calculate_forecast_metrics(
            actual_values=[1.0],
            predicted_values=[
                1.0,
                2.0,
            ],
        )
