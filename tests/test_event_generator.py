from datetime import datetime, timezone

from config.settings import load_config
from simulator.event_generator import HospitalEventGenerator
from simulator.event_models import (
    EventType,
    PatientType,
    Priority,
)


def test_generate_patient_arrival():
    config = load_config("config/hospital.yaml")

    generator = HospitalEventGenerator(config, seed=42)

    event = generator.generate_patient_arrival(
        patient_type=PatientType.OUTPATIENT,
        priority=Priority.NORMAL,
        lab_id="LAB_A",
        event_time=datetime(
            2026,
            9,
            3,
            8,
            0,
            tzinfo=timezone.utc,
        ),
    )

    assert event.patient_id.startswith("PAT_")
    assert event.lab_id == "LAB_A"
    assert event.patient_type == PatientType.OUTPATIENT
    assert event.priority == Priority.NORMAL


def test_generate_patient_lifecycle():
    config = load_config("config/hospital.yaml")

    generator = HospitalEventGenerator(config, seed=42)

    arrival_time = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    events = generator.generate_patient_lifecycle(
        patient_type=PatientType.OUTPATIENT,
        priority=Priority.NORMAL,
        lab_id="LAB_A",
        arrival_time=arrival_time,
    )

    assert len(events) == 3

    assert events[0].event_type == EventType.PATIENT_ARRIVAL
    assert events[1].event_type == EventType.REGISTRATION_COMPLETED
    assert events[2].event_type == EventType.QUEUE_ENTERED

    assert events[0].patient_id == events[1].patient_id
    assert events[1].patient_id == events[2].patient_id

    assert events[0].event_time < events[1].event_time
    assert events[1].event_time == events[2].event_time