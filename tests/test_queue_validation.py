from datetime import datetime, timedelta

import pytest

from simulator.queue_comparison import QueueComparison
from simulator.queue_validation import QueueValidationEngine


def create_comparison(
    theoretical_wait: float | None,
    observed_wait: float,
    stable: bool = True,
):
    start = datetime(2026, 9, 3, 8, 0)
    end = start + timedelta(minutes=30)

    return QueueComparison(
        lab_id="LAB_A",
        window_start=start,
        window_end=end,
        arrival_rate_per_hour=30.0,
        service_rate_per_hour=10.0,
        server_count=4,
        theoretical_utilization=0.75,
        theoretical_wait_minutes=theoretical_wait,
        observed_wait_minutes=observed_wait,
        wait_difference_minutes=(
            observed_wait - theoretical_wait
            if theoretical_wait is not None
            else None
        ),
        relative_error=None,
        system_stable=stable,
    )


def test_validation_summary():

    comparisons = [
        create_comparison(
            theoretical_wait=2.0,
            observed_wait=3.0,
        ),
        create_comparison(
            theoretical_wait=4.0,
            observed_wait=5.0,
        ),
        create_comparison(
            theoretical_wait=None,
            observed_wait=10.0,
            stable=False,
        ),
    ]

    engine = QueueValidationEngine()

    summary = engine.summarize(comparisons)

    assert summary.total_windows == 3
    assert summary.stable_windows == 2
    assert summary.unstable_windows == 1

    assert summary.mean_absolute_error_minutes == pytest.approx(1.0)
    assert summary.root_mean_squared_error_minutes == pytest.approx(1.0)
    assert summary.mean_bias_minutes == pytest.approx(1.0)

    assert summary.theoretical_mean_wait_minutes == pytest.approx(3.0)
    assert summary.observed_mean_wait_minutes == pytest.approx(4.0)