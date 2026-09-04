from datetime import datetime

import pytest

from simulator.event_models import (
    EventType,
    HospitalEvent,
    PatientType,
    Priority,
)
from simulator.fault_injector import EventFaultInjector


def create_event(event_id: str) -> HospitalEvent:
    timestamp = datetime.fromisoformat(
        "2026-09-03T08:00:00+04:00"
    )

    return HospitalEvent(
        event_id=event_id,
        event_type=EventType.PATIENT_ARRIVAL,
        patient_id="PATIENT_001",
        lab_id="LAB_A",
        event_time=timestamp,
        ingestion_time=timestamp,
        patient_type=PatientType.OUTPATIENT,
        priority=Priority.NORMAL,
    )


def test_duplicate_injection():

    events = [
        create_event("EVENT_001"),
    ]

    injector = EventFaultInjector(
        duplicate_rate=1.0,
        late_event_rate=0.0,
        out_of_order_rate=0.0,
        seed=42,
    )

    result = injector.inject(events)

    assert len(result) == 2
    assert result[0].event_id == result[1].event_id


def test_late_event_injection():

    events = [
        create_event("EVENT_001"),
    ]

    injector = EventFaultInjector(
        duplicate_rate=0.0,
        late_event_rate=1.0,
        out_of_order_rate=0.0,
        seed=42,
    )

    result = injector.inject(events)

    assert len(result) == 2
    assert result[1].event_time < result[0].event_time


def test_no_faults():

    events = [
        create_event("EVENT_001"),
        create_event("EVENT_002"),
    ]

    injector = EventFaultInjector(
        duplicate_rate=0.0,
        late_event_rate=0.0,
        out_of_order_rate=0.0,
        seed=42,
    )

    result = injector.inject(events)

    assert len(result) == len(events)


def test_invalid_duplicate_rate():

    with pytest.raises(ValueError):

        EventFaultInjector(
            duplicate_rate=1.5,
            late_event_rate=0.0,
            out_of_order_rate=0.0,
        )