from dataclasses import dataclass
from math import sqrt

from simulator.queue_comparison import QueueComparison


@dataclass
class QueueValidationSummary:
    """Aggregate validation metrics for queue model comparisons."""

    total_windows: int
    stable_windows: int
    unstable_windows: int

    mean_absolute_error_minutes: float
    root_mean_squared_error_minutes: float
    mean_bias_minutes: float

    theoretical_mean_wait_minutes: float
    observed_mean_wait_minutes: float


class QueueValidationEngine:
    """Evaluate agreement between M/M/c and observed queues."""

    def summarize(
        self,
        comparisons: list[QueueComparison],
    ) -> QueueValidationSummary:

        stable = [
            result
            for result in comparisons
            if (
                result.system_stable
                and result.theoretical_wait_minutes is not None
            )
        ]

        unstable_count = len(comparisons) - len(stable)

        if not stable:
            return QueueValidationSummary(
                total_windows=len(comparisons),
                stable_windows=0,
                unstable_windows=unstable_count,
                mean_absolute_error_minutes=0.0,
                root_mean_squared_error_minutes=0.0,
                mean_bias_minutes=0.0,
                theoretical_mean_wait_minutes=0.0,
                observed_mean_wait_minutes=0.0,
            )

        errors = [
            result.observed_wait_minutes
            - result.theoretical_wait_minutes
            for result in stable
        ]

        absolute_errors = [
            abs(error)
            for error in errors
        ]

        squared_errors = [
            error ** 2
            for error in errors
        ]

        mae = (
            sum(absolute_errors)
            / len(absolute_errors)
        )

        rmse = sqrt(
            sum(squared_errors)
            / len(squared_errors)
        )

        bias = (
            sum(errors)
            / len(errors)
        )

        theoretical_mean = (
            sum(
                result.theoretical_wait_minutes
                for result in stable
            )
            / len(stable)
        )

        observed_mean = (
            sum(
                result.observed_wait_minutes
                for result in stable
            )
            / len(stable)
        )

        return QueueValidationSummary(
            total_windows=len(comparisons),
            stable_windows=len(stable),
            unstable_windows=unstable_count,
            mean_absolute_error_minutes=mae,
            root_mean_squared_error_minutes=rmse,
            mean_bias_minutes=bias,
            theoretical_mean_wait_minutes=theoretical_mean,
            observed_mean_wait_minutes=observed_mean,
        )