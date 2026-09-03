from datetime import datetime, timezone
from simulator.event_models import PatientType, Priority, EventType
from simulator.queue_engine import QueueEngine, QueuePatient


def test_all_patients_start_immediately_when_stations_are_available():
    queue_time = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    patients = [
        QueuePatient(
            patient_id=f"PAT_{i}",
            queue_time=queue_time,
            service_duration_minutes=10,
            lab_id="LAB_A",
            patient_type=PatientType.OUTPATIENT,
            priority=Priority.NORMAL,
        )
        for i in range(4)
    ]

    engine = QueueEngine(station_count=4)

    assignments = engine.assign_patients(patients)

    assert len(assignments) == 4

    for assignment in assignments:
        assert assignment.service_start == queue_time

def test_fifth_patient_waits_when_four_stations_are_busy():
    queue_time = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    patients = [
        QueuePatient(
            patient_id=f"PAT_{i}",
            queue_time=queue_time,
            service_duration_minutes=10,
            lab_id="LAB_A",
            patient_type=PatientType.OUTPATIENT,
            priority=Priority.NORMAL,
        )
        for i in range(5)
    ]

    engine = QueueEngine(station_count=4)

    assignments = engine.assign_patients(patients)

    assert len(assignments) == 5

    fifth_patient = next(
        assignment
        for assignment in assignments
        if assignment.patient_id == "PAT_4"
    )

    expected_start = datetime(
        2026,
        9,
        3,
        8,
        10,
        tzinfo=timezone.utc,
    )

    assert fifth_patient.service_start == expected_start


    wait_time = (
        fifth_patient.service_start
        - fifth_patient.queue_time
    )

    assert wait_time.total_seconds() == 10 * 60

def test_generate_service_events():
    queue_time = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    patient = QueuePatient(
        patient_id="PAT_001",
        queue_time=queue_time,
        service_duration_minutes=10,
        lab_id="LAB_A",
        patient_type=PatientType.OUTPATIENT,
        priority=Priority.NORMAL,
    )

    engine = QueueEngine(station_count=4)

    assignments = engine.assign_patients([patient])
    assignment = assignments[0]

    events = engine.generate_service_events(assignment)

    assert len(events) == 4

    assert events[0].event_type == EventType.STAFF_ASSIGNED
    assert events[1].event_type == EventType.SERVICE_STARTED
    assert events[2].event_type == EventType.SERVICE_COMPLETED
    assert events[3].event_type == EventType.PATIENT_DEPARTED

    assert all(
        event.patient_id == "PAT_001"
        for event in events
    )

    assert events[0].staff_id == assignment.station_id
    assert events[1].staff_id == assignment.station_id

    assert events[0].event_time == assignment.service_start
    assert events[2].event_time == assignment.service_end
    assert events[3].event_time == assignment.service_end