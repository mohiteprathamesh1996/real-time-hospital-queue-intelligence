from datetime import datetime, timedelta, timezone

import pytest

from simulator.metrics import PatientMetrics, WindowMetrics
from simulator.queue_comparison import QueueComparisonEngine


def create_patient(
    patient_id: str,
    service_minutes: float = 5.0,
) -> PatientMetrics:

    start = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    return PatientMetrics(
        patient_id=patient_id,
        lab_id="LAB_A",
        arrival_time=start,
        registration_time=start + timedelta(minutes=2),
        queue_entry_time=start + timedelta(minutes=2),
        service_start_time=start + timedelta(minutes=2),
        service_end_time=start + timedelta(
            minutes=2 + service_minutes
        ),
        departure_time=start + timedelta(
            minutes=2 + service_minutes
        ),
        registration_duration_minutes=2.0,
        queue_wait_minutes=0.0,
        service_duration_minutes=service_minutes,
        total_journey_minutes=2.0 + service_minutes,
    )


def create_window(
    arrivals: int,
    average_wait: float,
) -> WindowMetrics:

    start = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    return WindowMetrics(
        lab_id="LAB_A",
        window_start=start,
        window_end=start + timedelta(minutes=30),
        arrivals=arrivals,
        patients_served=arrivals,
        average_queue_wait_minutes=average_wait,
        maximum_queue_wait_minutes=average_wait,
        average_service_time_minutes=5.0,
        utilization=0.0,
        sla_breach_rate=0.0,
    )


def test_service_rate_from_patient_metrics():

    patients = [
        create_patient("PAT_001", 5.0),
        create_patient("PAT_002", 5.0),
        create_patient("PAT_003", 5.0),
    ]

    engine = QueueComparisonEngine(
        server_count=4
    )

    service_rate = engine.calculate_service_rate(
        patients
    )

    assert service_rate == pytest.approx(12.0)


def test_compare_stable_window():

    patients = [
        create_patient(f"PAT_{i:03d}", 5.0)
        for i in range(10)
    ]

    window = create_window(
        arrivals=10,
        average_wait=4.0,
    )

    engine = QueueComparisonEngine(
        server_count=4
    )

    result = engine.compare_windows(
        windows=[window],
        patient_metrics=patients,
    )[0]

    # 10 arrivals / 0.5 hour = 20/hour
    assert result.arrival_rate_per_hour == pytest.approx(20.0)

    # 60 / 5 = 12/hour/staff
    assert result.service_rate_per_hour == pytest.approx(12.0)

    # 20 / (4 * 12) = 0.4167
    assert result.theoretical_utilization == pytest.approx(
        20 / 48
    )

    assert result.system_stable is True

    assert result.theoretical_wait_minutes is not None

    assert result.wait_difference_minutes is not None


def test_compare_unstable_window():

    patients = [
        create_patient(f"PAT_{i:03d}", 5.0)
        for i in range(50)
    ]

    window = create_window(
        arrivals=50,
        average_wait=15.0,
    )

    engine = QueueComparisonEngine(
        server_count=4
    )

    result = engine.compare_windows(
        windows=[window],
        patient_metrics=patients,
    )[0]

    # 50 arrivals / 0.5 hour = 100/hour
    assert result.arrival_rate_per_hour == pytest.approx(
        100.0
    )

    # Capacity = 4 * 12 = 48/hour
    assert result.theoretical_utilization > 1

    assert result.system_stable is False

    assert result.theoretical_wait_minutes is None


def test_zero_arrival_window():

    patients = [
        create_patient("PAT_001", 5.0),
    ]

    window = create_window(
        arrivals=0,
        average_wait=0.0,
    )

    engine = QueueComparisonEngine(
        server_count=4
    )

    result = engine.compare_windows(
        windows=[window],
        patient_metrics=patients,
    )[0]

    assert result.arrival_rate_per_hour == 0.0
    assert result.theoretical_wait_minutes == 0.0
    assert result.system_stable is True


def test_local_service_rate():

    patients = [
        create_patient("PAT_001", 5.0),
        create_patient("PAT_002", 10.0),
    ]

    window = create_window(
        arrivals=2,
        average_wait=2.0,
    )

    engine = QueueComparisonEngine(
        server_count=4
    )

    service_rate = engine.calculate_window_service_rate(
        window=window,
        patient_metrics=patients,
    )

    assert service_rate == pytest.approx(8.0)