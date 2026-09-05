from dataclasses import dataclass


@dataclass(frozen=True)
class ArrivalForecast:
    strategy: str
    predicted_arrivals: float
    estimated_rate_per_minute: float


class RollingRateForecaster:
    """
    Baseline forecast.

    Assumes the arrival rate observed over the complete
    lookback window continues through the forecast horizon.
    """

    def forecast(
        self,
        arrival_count: int,
        lookback_minutes: float,
        horizon_minutes: float,
    ) -> ArrivalForecast:

        if lookback_minutes <= 0:
            raise ValueError(
                "lookback_minutes must be greater than zero"
            )

        if horizon_minutes <= 0:
            raise ValueError(
                "horizon_minutes must be greater than zero"
            )

        rate = (
            arrival_count
            / lookback_minutes
        )

        predicted = (
            rate
            * horizon_minutes
        )

        return ArrivalForecast(
            strategy="ROLLING_RATE",
            predicted_arrivals=predicted,
            estimated_rate_per_minute=rate,
        )


class WeightedRecentRateForecaster:
    """
    Gives more weight to recent arrivals.

    Example with a 30-minute lookback:

        older 15 minutes -> weight 1
        recent 15 minutes -> weight 2

    The weighted rate remains normalized so the units are
    still arrivals per minute.
    """

    def __init__(
        self,
        recent_weight: float = 2.0,
        older_weight: float = 1.0,
    ):
        if recent_weight <= 0:
            raise ValueError(
                "recent_weight must be positive"
            )

        if older_weight <= 0:
            raise ValueError(
                "older_weight must be positive"
            )

        self.recent_weight = (
            recent_weight
        )

        self.older_weight = (
            older_weight
        )

    def forecast(
        self,
        older_count: int,
        recent_count: int,
        segment_minutes: float,
        horizon_minutes: float,
    ) -> ArrivalForecast:

        if segment_minutes <= 0:
            raise ValueError(
                "segment_minutes must be greater than zero"
            )

        if horizon_minutes <= 0:
            raise ValueError(
                "horizon_minutes must be greater than zero"
            )

        older_rate = (
            older_count
            / segment_minutes
        )

        recent_rate = (
            recent_count
            / segment_minutes
        )

        weighted_rate = (
            (
                older_rate
                * self.older_weight
            )
            +
            (
                recent_rate
                * self.recent_weight
            )
        ) / (
            self.older_weight
            + self.recent_weight
        )

        predicted = (
            weighted_rate
            * horizon_minutes
        )

        return ArrivalForecast(
            strategy="WEIGHTED_RECENT",
            predicted_arrivals=predicted,
            estimated_rate_per_minute=(
                weighted_rate
            ),
        )


class TrendAdjustedRateForecaster:
    """
    Uses two adjacent historical segments to estimate
    whether arrival intensity is accelerating or falling.

    Let:

        older_rate = arrivals/min in older half
        recent_rate = arrivals/min in recent half

    The trend-adjusted future rate is:

        recent_rate
        + trend_strength * (recent_rate - older_rate)

    Negative rates are clipped to zero.
    """

    def __init__(
        self,
        trend_strength: float = 0.5,
    ):
        if trend_strength < 0:
            raise ValueError(
                "trend_strength cannot be negative"
            )

        self.trend_strength = (
            trend_strength
        )

    def forecast(
        self,
        older_count: int,
        recent_count: int,
        segment_minutes: float,
        horizon_minutes: float,
    ) -> ArrivalForecast:

        if segment_minutes <= 0:
            raise ValueError(
                "segment_minutes must be greater than zero"
            )

        if horizon_minutes <= 0:
            raise ValueError(
                "horizon_minutes must be greater than zero"
            )

        older_rate = (
            older_count
            / segment_minutes
        )

        recent_rate = (
            recent_count
            / segment_minutes
        )

        trend = (
            recent_rate
            - older_rate
        )

        projected_rate = (
            recent_rate
            + self.trend_strength
            * trend
        )

        projected_rate = max(
            0.0,
            projected_rate,
        )

        predicted = (
            projected_rate
            * horizon_minutes
        )

        return ArrivalForecast(
            strategy="TREND_ADJUSTED",
            predicted_arrivals=predicted,
            estimated_rate_per_minute=(
                projected_rate
            ),
        )
