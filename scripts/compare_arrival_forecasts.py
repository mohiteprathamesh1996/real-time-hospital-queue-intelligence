from datetime import timedelta

from pyspark.sql.functions import col

from decision_engine.arrival_forecast_strategies import (
    RollingRateForecaster,
    TrendAdjustedRateForecaster,
    WeightedRecentRateForecaster,
)
from decision_engine.forecast_metrics import (
    calculate_forecast_metrics,
)
from streaming.spark_session import (
    create_spark_session,
)


PATIENT_METRICS_PATH = (
    "./data/gold/patient_metrics"
)

OPERATIONAL_STATE_PATH = (
    "./data/gold/operational_state_5m"
)


LOOKBACK_MINUTES = 30
SEGMENT_MINUTES = 15

FORECAST_HORIZON_MINUTES = 30


def count_arrivals(
    patients,
    start_time,
    end_time,
):
    return (
        patients
        .filter(
            (
                col("arrival_time")
                >= start_time
            )
            & (
                col("arrival_time")
                < end_time
            )
        )
        .count()
    )


def main():

    spark = create_spark_session(
        "CompareArrivalForecastStrategies"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    patients = (
        spark.read
        .format("delta")
        .load(
            PATIENT_METRICS_PATH
        )
    )

    timeline = (
        spark.read
        .format("delta")
        .load(
            OPERATIONAL_STATE_PATH
        )
        .select(
            "window_start"
        )
        .orderBy(
            "window_start"
        )
        .collect()
    )

    first_arrival = (
        patients
        .agg(
            {
                "arrival_time": "min"
            }
        )
        .collect()[0][0]
    )

    last_arrival = (
        patients
        .agg(
            {
                "arrival_time": "max"
            }
        )
        .collect()[0][0]
    )

    rolling = (
        RollingRateForecaster()
    )

    weighted = (
        WeightedRecentRateForecaster(
            older_weight=1.0,
            recent_weight=2.0,
        )
    )

    trend = (
        TrendAdjustedRateForecaster(
            trend_strength=0.5,
        )
    )

    actual_values = []

    rolling_predictions = []
    weighted_predictions = []
    trend_predictions = []

    evaluated_timestamps = []

    for row in timeline:

        timestamp = (
            row["window_start"]
        )

        lookback_start = (
            timestamp
            - timedelta(
                minutes=LOOKBACK_MINUTES
            )
        )

        midpoint = (
            timestamp
            - timedelta(
                minutes=SEGMENT_MINUTES
            )
        )

        forecast_end = (
            timestamp
            + timedelta(
                minutes=(
                    FORECAST_HORIZON_MINUTES
                )
            )
        )

        # Ensure every strategy is evaluated on exactly
        # the same timestamps.
        if lookback_start < first_arrival:
            continue

        if forecast_end > last_arrival:
            continue

        older_count = (
            count_arrivals(
                patients=patients,
                start_time=(
                    lookback_start
                ),
                end_time=midpoint,
            )
        )

        recent_count = (
            count_arrivals(
                patients=patients,
                start_time=midpoint,
                end_time=timestamp,
            )
        )

        total_count = (
            older_count
            + recent_count
        )

        actual_count = (
            count_arrivals(
                patients=patients,
                start_time=timestamp,
                end_time=forecast_end,
            )
        )

        rolling_result = (
            rolling.forecast(
                arrival_count=(
                    total_count
                ),
                lookback_minutes=(
                    LOOKBACK_MINUTES
                ),
                horizon_minutes=(
                    FORECAST_HORIZON_MINUTES
                ),
            )
        )

        weighted_result = (
            weighted.forecast(
                older_count=(
                    older_count
                ),
                recent_count=(
                    recent_count
                ),
                segment_minutes=(
                    SEGMENT_MINUTES
                ),
                horizon_minutes=(
                    FORECAST_HORIZON_MINUTES
                ),
            )
        )

        trend_result = (
            trend.forecast(
                older_count=(
                    older_count
                ),
                recent_count=(
                    recent_count
                ),
                segment_minutes=(
                    SEGMENT_MINUTES
                ),
                horizon_minutes=(
                    FORECAST_HORIZON_MINUTES
                ),
            )
        )

        evaluated_timestamps.append(
            timestamp
        )

        actual_values.append(
            float(actual_count)
        )

        rolling_predictions.append(
            float(
                rolling_result
                .predicted_arrivals
            )
        )

        weighted_predictions.append(
            float(
                weighted_result
                .predicted_arrivals
            )
        )

        trend_predictions.append(
            float(
                trend_result
                .predicted_arrivals
            )
        )

    strategies = [
        (
            "ROLLING_RATE",
            rolling_predictions,
        ),
        (
            "WEIGHTED_RECENT",
            weighted_predictions,
        ),
        (
            "TREND_ADJUSTED",
            trend_predictions,
        ),
    ]

    results = []

    for (
        strategy_name,
        predictions,
    ) in strategies:

        metrics = (
            calculate_forecast_metrics(
                actual_values=(
                    actual_values
                ),
                predicted_values=(
                    predictions
                ),
            )
        )

        results.append(
            (
                strategy_name,
                metrics,
            )
        )

    print("=" * 110)
    print(
        "ARRIVAL FORECAST STRATEGY COMPARISON"
    )
    print("=" * 110)

    print(
        f"Lookback:            "
        f"{LOOKBACK_MINUTES} minutes"
    )

    print(
        f"Historical segments: "
        f"{SEGMENT_MINUTES} + "
        f"{SEGMENT_MINUTES} minutes"
    )

    print(
        f"Forecast horizon:    "
        f"{FORECAST_HORIZON_MINUTES} minutes"
    )

    print(
        f"Common observations: "
        f"{len(actual_values)}"
    )

    print()

    print(
        f"{'Strategy':<22}"
        f"{'MAE':>12}"
        f"{'Bias':>12}"
        f"{'RMSE':>12}"
        f"{'MAPE':>12}"
    )

    print("-" * 70)

    for (
        strategy_name,
        metrics,
    ) in results:

        if metrics.mape is None:
            mape_text = "N/A"
        else:
            mape_text = (
                f"{metrics.mape:.1f}%"
            )

        print(
            f"{strategy_name:<22}"
            f"{metrics.mae:>12.2f}"
            f"{metrics.bias:>12.2f}"
            f"{metrics.rmse:>12.2f}"
            f"{mape_text:>12}"
        )

    winner_name, winner_metrics = min(
        results,
        key=lambda item: (
            item[1].mae,
            abs(item[1].bias),
            item[1].rmse,
        ),
    )

    print()

    print("=" * 110)
    print("BEST BASELINE STRATEGY")
    print("=" * 110)

    print(
        f"Strategy: {winner_name}"
    )

    print(
        f"MAE:      "
        f"{winner_metrics.mae:.2f} patients"
    )

    print(
        f"Bias:     "
        f"{winner_metrics.bias:.2f} patients"
    )

    print(
        f"RMSE:     "
        f"{winner_metrics.rmse:.2f} patients"
    )

    print()

    print(
        "All strategies were evaluated on "
        "the exact same timestamps."
    )

    print("=" * 110)

    spark.stop()


if __name__ == "__main__":
    main()
