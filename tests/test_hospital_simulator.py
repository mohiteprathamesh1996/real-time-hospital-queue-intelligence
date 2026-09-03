from datetime import datetime

from config.settings import load_config
from simulator.event_models import PatientType, EventType
from simulator.hospital_simulator import HospitalSimulator


def test_simulator_loads_configuration():
    config = load_config("config/hospital.yaml")

    simulator = HospitalSimulator(
        config,
        seed=42,
    )

    assert simulator.config.hospital.name == "Demo General Hospital"


def test_simulator_returns_correct_time_window():
    config = load_config("config/hospital.yaml")

    simulator = HospitalSimulator(
        config,
        seed=42,
    )

    start_time, end_time = simulator.get_simulation_window()

    assert start_time == datetime.fromisoformat(
        "2026-09-03T06:00:00+04:00"
    )

    assert end_time == datetime.fromisoformat(
        "2026-09-03T18:00:00+04:00"
    )


def test_simulator_returns_patient_type_weights():
    config = load_config("config/hospital.yaml")

    simulator = HospitalSimulator(
        config,
        seed=42,
    )

    weights = simulator.get_patient_type_weights()

    assert weights[PatientType.OUTPATIENT] == 35
    assert weights[PatientType.INPATIENT] == 15
    assert weights[PatientType.WALK_IN] == 10
    assert weights[PatientType.FOLLOW_UP] == 8

def test_simulator_generates_events():
    config = load_config("config/hospital.yaml")

    simulator = HospitalSimulator(
        config,
        seed=42,
    )

    events = simulator.run(
        lab_id="LAB_A",
    )

    assert len(events) > 0

def test_simulator_generates_patient_lifecycle_events():
    config = load_config("config/hospital.yaml")

    simulator = HospitalSimulator(
        config,
        seed=42,
    )

    events = simulator.run(
        lab_id="LAB_A",
    )

    event_types = {
        event.event_type
        for event in events
    }

    assert EventType.PATIENT_ARRIVAL in event_types
    assert EventType.STAFF_ASSIGNED in event_types
    assert EventType.SERVICE_STARTED in event_types
    assert EventType.SERVICE_COMPLETED in event_types
    assert EventType.PATIENT_DEPARTED in event_types


def test_simulator_is_reproducible():
    config = load_config("config/hospital.yaml")

    simulator_1 = HospitalSimulator(
        config,
        seed=42,
    )

    simulator_2 = HospitalSimulator(
        config,
        seed=42,
    )

    events_1 = simulator_1.run(
        lab_id="LAB_A",
    )

    events_2 = simulator_2.run(
        lab_id="LAB_A",
    )

    assert len(events_1) == len(events_2)

    assert [
        (
            event.event_type,
            event.event_time,
            event.patient_type,
            event.priority,
        )
        for event in events_1
    ] == [
        (
            event.event_type,
            event.event_time,
            event.patient_type,
            event.priority,
        )
        for event in events_2
    ]


    