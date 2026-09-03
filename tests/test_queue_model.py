import pytest

from simulator.queue_model import MMcQueueModel


def test_mmc_basic_metrics():

    model = MMcQueueModel(
        arrival_rate_per_hour=30,
        service_rate_per_hour=10,
        server_count=4,
    )

    metrics = model.calculate()

    assert metrics.server_count == 4

    assert metrics.arrival_rate_per_hour == 30
    assert metrics.service_rate_per_hour == 10

    assert metrics.utilization == pytest.approx(0.75)

    assert metrics.probability_wait == pytest.approx(
        0.5094339623,
        rel=1e-5,
    )

    assert metrics.average_queue_length == pytest.approx(
        1.5283018868,
        rel=1e-5,
    )

    assert metrics.average_wait_minutes == pytest.approx(
        3.0566037736,
        rel=1e-5,
    )


def test_mmc_system_time():

    model = MMcQueueModel(
        arrival_rate_per_hour=20,
        service_rate_per_hour=10,
        server_count=3,
    )

    metrics = model.calculate()

    assert metrics.average_system_time_minutes > (
        60 / 10
    )


def test_unstable_system_raises_error():

    model = MMcQueueModel(
        arrival_rate_per_hour=50,
        service_rate_per_hour=10,
        server_count=5,
    )

    with pytest.raises(ValueError, match="unstable"):
        model.calculate()


def test_invalid_arrival_rate():

    with pytest.raises(ValueError):
        MMcQueueModel(
            arrival_rate_per_hour=0,
            service_rate_per_hour=10,
            server_count=4,
        )


def test_invalid_service_rate():

    with pytest.raises(ValueError):
        MMcQueueModel(
            arrival_rate_per_hour=20,
            service_rate_per_hour=0,
            server_count=4,
        )


def test_invalid_server_count():

    with pytest.raises(ValueError):
        MMcQueueModel(
            arrival_rate_per_hour=20,
            service_rate_per_hour=10,
            server_count=0,
        )