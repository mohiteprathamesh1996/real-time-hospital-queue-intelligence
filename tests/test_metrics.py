from datetime import datetime, timedelta, timezone

from simulator.event_models import EventType, HospitalEvent
from simulator.metrics import MetricsCalculator


def create_event(
    event_type,
    patient_id,
    event_time,
):
    return HospitalEvent(
        event_id=f"{patient_id}_{event_type.value}",
        event_type=event_type,
        patient_id=patient_id,
        lab_id="LAB_A",
        event_time=event_time,
        ingestion_time=event_time,
        patient_type="OUTPATIENT",
        priority="NORMAL",
    )


def test_patient_metrics_calculate_correct_durations():

    start = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    events = [
        create_event(
            EventType.PATIENT_ARRIVAL,
            "PAT_001",
            start,
        ),
        create_event(
            EventType.REGISTRATION_COMPLETED,
            "PAT_001",
            start + timedelta(minutes=2),
        ),
        create_event(
            EventType.QUEUE_ENTERED,
            "PAT_001",
            start + timedelta(minutes=2),
        ),
        create_event(
            EventType.SERVICE_STARTED,
            "PAT_001",
            start + timedelta(minutes=12),
        ),
        create_event(
            EventType.SERVICE_COMPLETED,
            "PAT_001",
            start + timedelta(minutes=17),
        ),
        create_event(
            EventType.PATIENT_DEPARTED,
            "PAT_001",
            start + timedelta(minutes=17),
        ),
    ]

    calculator = MetricsCalculator()

    metrics = calculator.calculate_patient_metrics(events)

    assert len(metrics) == 1

    patient = metrics[0]

    assert patient.registration_duration_minutes == 2
    assert patient.queue_wait_minutes == 10
    assert patient.service_duration_minutes == 5
    assert patient.total_journey_minutes == 17


def test_sla_breach_is_calculated():

    start = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    events = []

    events.extend([
        create_event(
            EventType.PATIENT_ARRIVAL,
            "PAT_001",
            start,
        ),
        create_event(
            EventType.REGISTRATION_COMPLETED,
            "PAT_001",
            start + timedelta(minutes=2),
        ),
        create_event(
            EventType.QUEUE_ENTERED,
            "PAT_001",
            start + timedelta(minutes=2),
        ),
        create_event(
            EventType.SERVICE_STARTED,
            "PAT_001",
            start + timedelta(minutes=20),
        ),
        create_event(
            EventType.SERVICE_COMPLETED,
            "PAT_001",
            start + timedelta(minutes=25),
        ),
        create_event(
            EventType.PATIENT_DEPARTED,
            "PAT_001",
            start + timedelta(minutes=25),
        ),
    ])

    calculator = MetricsCalculator(
        queue_sla_minutes=15,
    )

    patient_metrics = calculator.calculate_patient_metrics(events)

    sla = calculator.calculate_sla_metrics(
        patient_metrics,
        lab_id="LAB_A",
    )

    assert sla.total_patients == 1
    assert sla.patients_within_sla == 0
    assert sla.patients_breaching_sla == 1
    assert sla.sla_breach_rate == 1.0


def test_window_metrics_calculate_hourly_metrics():

    start = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    events = [
        create_event(
            EventType.PATIENT_ARRIVAL,
            "PAT_001",
            start + timedelta(minutes=5),
        ),
        create_event(
            EventType.REGISTRATION_COMPLETED,
            "PAT_001",
            start + timedelta(minutes=7),
        ),
        create_event(
            EventType.QUEUE_ENTERED,
            "PAT_001",
            start + timedelta(minutes=7),
        ),
        create_event(
            EventType.SERVICE_STARTED,
            "PAT_001",
            start + timedelta(minutes=17),
        ),
        create_event(
            EventType.SERVICE_COMPLETED,
            "PAT_001",
            start + timedelta(minutes=22),
        ),
        create_event(
            EventType.PATIENT_DEPARTED,
            "PAT_001",
            start + timedelta(minutes=22),
        ),

        create_event(
            EventType.PATIENT_ARRIVAL,
            "PAT_002",
            start + timedelta(minutes=30),
        ),
        create_event(
            EventType.REGISTRATION_COMPLETED,
            "PAT_002",
            start + timedelta(minutes=32),
        ),
        create_event(
            EventType.QUEUE_ENTERED,
            "PAT_002",
            start + timedelta(minutes=32),
        ),
        create_event(
            EventType.SERVICE_STARTED,
            "PAT_002",
            start + timedelta(minutes=35),
        ),
        create_event(
            EventType.SERVICE_COMPLETED,
            "PAT_002",
            start + timedelta(minutes=40),
        ),
        create_event(
            EventType.PATIENT_DEPARTED,
            "PAT_002",
            start + timedelta(minutes=40),
        ),
    ]

    calculator = MetricsCalculator(
        queue_sla_minutes=15,
    )

    patient_metrics = calculator.calculate_patient_metrics(events)

    windows = calculator.calculate_window_metrics(
        patient_metrics=patient_metrics,
        lab_id="LAB_A",
        start_time=start,
        end_time=start + timedelta(hours=1),
        window_minutes=60,
        station_count=4,
    )

    assert len(windows) == 1

    window = windows[0]

    assert window.arrivals == 2
    assert window.patients_served == 2

    # PAT_001 waits 10 minutes.
    # PAT_002 waits 3 minutes.
    assert window.average_queue_wait_minutes == 6.5

    assert window.maximum_queue_wait_minutes == 10

    # Both patients have 5-minute services.
    assert window.average_service_time_minutes == 5

    # 10 total service minutes /
    # (60 minutes * 4 stations)
    assert window.utilization == 10 / 240

    # Neither patient exceeds 15 minutes.
    assert window.sla_breach_rate == 0

def test_window_metrics_split_service_across_windows():

    start = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    events = [
        create_event(
            EventType.PATIENT_ARRIVAL,
            "PAT_001",
            start + timedelta(minutes=55),
        ),
        create_event(
            EventType.REGISTRATION_COMPLETED,
            "PAT_001",
            start + timedelta(minutes=57),
        ),
        create_event(
            EventType.QUEUE_ENTERED,
            "PAT_001",
            start + timedelta(minutes=57),
        ),
        create_event(
            EventType.SERVICE_STARTED,
            "PAT_001",
            start + timedelta(minutes=55),
        ),
        create_event(
            EventType.SERVICE_COMPLETED,
            "PAT_001",
            start + timedelta(minutes=65),
        ),
        create_event(
            EventType.PATIENT_DEPARTED,
            "PAT_001",
            start + timedelta(minutes=65),
        ),
    ]

    calculator = MetricsCalculator()

    patient_metrics = calculator.calculate_patient_metrics(events)

    windows = calculator.calculate_window_metrics(
        patient_metrics=patient_metrics,
        lab_id="LAB_A",
        start_time=start,
        end_time=start + timedelta(hours=2),
        window_minutes=60,
        station_count=1,
    )

    assert len(windows) == 2

    first_window = windows[0]
    second_window = windows[1]

    # Service runs from 08:55 to 09:05.
    # Therefore:
    # 08:00-09:00 = 5 minutes
    # 09:00-10:00 = 5 minutes

    assert first_window.utilization == 5 / 60
    assert second_window.utilization == 5 / 60

