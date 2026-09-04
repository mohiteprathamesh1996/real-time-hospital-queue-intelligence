from datetime import datetime, timedelta, timezone
import pytest
from simulator.metrics import PatientMetrics
from simulator.queue_state import QueueStateCalculator


def create_patient(
    patient_id: str,
    queue_entry_minutes: float,
    service_start_minutes: float,
) -> PatientMetrics:

    start = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    arrival_time = start
    registration_time = start + timedelta(minutes=2)
    queue_entry_time = start + timedelta(
        minutes=queue_entry_minutes
    )
    service_start_time = start + timedelta(
        minutes=service_start_minutes
    )
    service_end_time = service_start_time + timedelta(
        minutes=5
    )
    departure_time = service_end_time

    return PatientMetrics(
        patient_id=patient_id,
        lab_id="LAB_A",
        arrival_time=arrival_time,
        registration_time=registration_time,
        queue_entry_time=queue_entry_time,
        service_start_time=service_start_time,
        service_end_time=service_end_time,
        departure_time=departure_time,
        registration_duration_minutes=2.0,
        queue_wait_minutes=(
            service_start_time - queue_entry_time
        ).total_seconds() / 60,
        service_duration_minutes=5.0,
        total_journey_minutes=(
            departure_time - arrival_time
        ).total_seconds() / 60,
    )


def test_queue_state_counts_waiting_patients():

    patients = [
        create_patient(
            "PAT_001",
            queue_entry_minutes=0,
            service_start_minutes=15,
        )
    ]

    calculator = QueueStateCalculator()

    state = calculator.calculate(
        patient_metrics=patients,
        lab_id="LAB_A",
        timestamp=datetime(
            2026,
            9,
            3,
            8,
            10,
            tzinfo=timezone.utc,
        ),
    )

    assert state.patients_waiting == 1


def test_queue_state_does_not_count_patient_after_service_starts():

    patients = [
        create_patient(
            "PAT_001",
            queue_entry_minutes=0,
            service_start_minutes=15,
        )
    ]

    calculator = QueueStateCalculator()

    state = calculator.calculate(
        patient_metrics=patients,
        lab_id="LAB_A",
        timestamp=datetime(
            2026,
            9,
            3,
            8,
            20,
            tzinfo=timezone.utc,
        ),
    )

    assert state.patients_waiting == 0


def test_queue_state_counts_multiple_waiting_patients():

    patients = [
        create_patient(
            "PAT_001",
            queue_entry_minutes=0,
            service_start_minutes=20,
        ),
        create_patient(
            "PAT_002",
            queue_entry_minutes=5,
            service_start_minutes=25,
        ),
        create_patient(
            "PAT_003",
            queue_entry_minutes=10,
            service_start_minutes=15,
        ),
    ]

    calculator = QueueStateCalculator()

    state = calculator.calculate(
        patient_metrics=patients,
        lab_id="LAB_A",
        timestamp=datetime(
            2026,
            9,
            3,
            8,
            12,
            tzinfo=timezone.utc,
        ),
    )

    assert state.patients_waiting == 3


def test_queue_state_returns_zero_for_empty_patient_list():

    calculator = QueueStateCalculator()

    state = calculator.calculate(
        patient_metrics=[],
        lab_id="LAB_A",
        timestamp=datetime(
            2026,
            9,
            3,
            8,
            10,
            tzinfo=timezone.utc,
        ),
    )

    assert state.patients_waiting == 0


def test_queue_state_does_not_count_patient_at_service_start():

    patients = [
        create_patient(
            "PAT_001",
            queue_entry_minutes=0,
            service_start_minutes=15,
        )
    ]

    calculator = QueueStateCalculator()

    state = calculator.calculate(
        patient_metrics=patients,
        lab_id="LAB_A",
        timestamp=datetime(
            2026,
            9,
            3,
            8,
            15,
            tzinfo=timezone.utc,
        ),
    )

    assert state.patients_waiting == 0


def test_queue_state_rejects_invalid_time_range():

    calculator = QueueStateCalculator()

    with pytest.raises(ValueError):
        calculator.calculate_series(
            patient_metrics=[],
            lab_id="LAB_A",
            start_time=datetime(
                2026,
                9,
                3,
                9,
                0,
                tzinfo=timezone.utc,
            ),
            end_time=datetime(
                2026,
                9,
                3,
                8,
                0,
                tzinfo=timezone.utc,
            ),
            window_minutes=30,
        )




