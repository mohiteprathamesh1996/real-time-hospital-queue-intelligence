from datetime import timedelta

from pyspark.sql.functions import col

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

FORECAST_HORIZON_MINUTES = 30

LOOKBACK_WINDOWS = [
    30,
    60,
    90,
    120,
]


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
        "EvaluateArrivalForecast"
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

    print("=" * 110)
    print(
        "ARRIVAL FORECAST VALIDATION"
    )
    print("=" * 110)

    print(
        f"Forecast horizon: "
        f"{FORECAST_HORIZON_MINUTES} minutes"
    )

    print(
        f"Timeline windows: "
        f"{len(timeline)}"
    )

    print()

    summaries = []

    for lookback_minutes in LOOKBACK_WINDOWS:

        actual_values = []
        predicted_values = []

        for row in timeline:

            timestamp = (
                row["window_start"]
            )

            lookback_start = (
                timestamp
                - timedelta(
                    minutes=lookback_minutes
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

            # Skip windows without enough historical
            # data or enough future data for fair
            # evaluation.
            if lookback_start < first_arrival:
                continue

            if forecast_end > last_arrival:
                continue

            historical_arrivals = (
                count_arrivals(
                    patients=patients,
                    start_time=(
                        lookback_start
                    ),
                    end_time=timestamp,
                )
            )

            arrival_rate = (
                historical_arrivals
                / lookback_minutes
            )

            predicted_arrivals = (
                arrival_rate
                * FORECAST_HORIZON_MINUTES
            )

            actual_arrivals = (
                count_arrivals(
                    patients=patients,
                    start_time=timestamp,
                    end_time=forecast_end,
                )
            )

            predicted_values.append(
                float(
                    predicted_arrivals
                )
            )

            actual_values.append(
                float(
                    actual_arrivals
                )
            )

        metrics = (
            calculate_forecast_metrics(
                actual_values=actual_values,
                predicted_values=(
                    predicted_values
                ),
            )
        )

        summaries.append(
            (
                lookback_minutes,
                metrics,
            )
        )

    print(
        f"{'Lookback':<12}"
        f"{'Obs':>8}"
        f"{'MAE':>12}"
        f"{'Bias':>12}"
        f"{'RMSE':>12}"
        f"{'MAPE':>12}"
    )

    print("-" * 68)

    for (
        lookback_minutes,
        metrics,
    ) in summaries:

        if metrics.mape is None:
            mape_text = "N/A"
        else:
            mape_text = (
                f"{metrics.mape:.1f}%"
            )

        print(
            f"{lookback_minutes:<12}"
            f"{metrics.observations:>8}"
            f"{metrics.mae:>12.2f}"
            f"{metrics.bias:>12.2f}"
            f"{metrics.rmse:>12.2f}"
            f"{mape_text:>12}"
        )

    valid_summaries = [
        item
        for item in summaries
        if item[1].observations > 0
    ]

    if valid_summaries:

        best_lookback, best_metrics = min(
            valid_summaries,
            key=lambda item: (
                item[1].mae,
                abs(item[1].bias),
                item[1].rmse,
            ),
        )

        print()

        print("=" * 110)
        print(
            "RECOMMENDED BASELINE LOOKBACK"
        )
        print("=" * 110)

        print(
            f"Lookback: "
            f"{best_lookback} minutes"
        )

        print(
            f"MAE:      "
            f"{best_metrics.mae:.2f} patients"
        )

        print(
            f"Bias:     "
            f"{best_metrics.bias:.2f} patients"
        )

        print(
            f"RMSE:     "
            f"{best_metrics.rmse:.2f} patients"
        )

        print()

        print(
            "Selection rule: lowest MAE, "
            "then lowest absolute bias, "
            "then lowest RMSE."
        )

    print("=" * 110)

    spark.stop()


if __name__ == "__main__":
    main()
