from dataclasses import dataclass


@dataclass(frozen=True)
class ForecastErrorMetrics:
    observations: int
    mae: float
    bias: float
    rmse: float
    mape: float | None


def calculate_forecast_metrics(
    actual_values,
    predicted_values,
) -> ForecastErrorMetrics:
    actual = list(actual_values)
    predicted = list(predicted_values)

    if len(actual) != len(predicted):
        raise ValueError(
            "actual_values and predicted_values "
            "must have the same length"
        )

    if not actual:
        return ForecastErrorMetrics(
            observations=0,
            mae=0.0,
            bias=0.0,
            rmse=0.0,
            mape=None,
        )

    errors = [
        pred - obs
        for obs, pred in zip(
            actual,
            predicted,
        )
    ]

    absolute_errors = [
        abs(error)
        for error in errors
    ]

    squared_errors = [
        error * error
        for error in errors
    ]

    mae = (
        sum(absolute_errors)
        / len(absolute_errors)
    )

    bias = (
        sum(errors)
        / len(errors)
    )

    rmse = (
        sum(squared_errors)
        / len(squared_errors)
    ) ** 0.5

    percentage_errors = [
        abs(pred - obs) / obs
        for obs, pred in zip(
            actual,
            predicted,
        )
        if obs > 0
    ]

    if percentage_errors:
        mape = (
            sum(percentage_errors)
            / len(percentage_errors)
            * 100
        )
    else:
        mape = None

    return ForecastErrorMetrics(
        observations=len(actual),
        mae=mae,
        bias=bias,
        rmse=rmse,
        mape=mape,
    )
