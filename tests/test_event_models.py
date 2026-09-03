from datetime import datetime, timezone

from simulator.event_models import (
    EventType,
    HospitalEvent,
    PatientType,
    Priority,
)


def test_valid_hospital_event():
    event = HospitalEvent(
        event_id="evt_000001",
        event_type=EventType.PATIENT_ARRIVAL,
        patient_id="PAT_000001",
        lab_id="LAB_A",
        event_time=datetime.now(timezone.utc),
        ingestion_time=datetime.now(timezone.utc),
        patient_type=PatientType.OUTPATIENT,
        priority=Priority.NORMAL,
    )

    assert event.event_id == "evt_000001"
    assert event.event_type == EventType.PATIENT_ARRIVAL
    assert event.lab_id == "LAB_A"


def test_event_serialization():
    event = HospitalEvent(
        event_id="evt_000002",
        event_type=EventType.QUEUE_ENTERED,
        patient_id="PAT_000002",
        lab_id="LAB_B",
        event_time=datetime.now(timezone.utc),
        ingestion_time=datetime.now(timezone.utc),
        patient_type=PatientType.WALK_IN,
        priority=Priority.URGENT,
    )

    payload = event.model_dump_json()

    assert '"event_id":"evt_000002"' in payload
    assert '"event_type":"QUEUE_ENTERED"' in payload