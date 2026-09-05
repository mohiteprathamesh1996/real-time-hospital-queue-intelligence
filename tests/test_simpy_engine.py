import pytest
from config.settings import load_config
from simulator.event_models import PatientType
from simulator.service_process import ServiceTimeGenerator
from simulator.simpy_engine import (
    InitialService,
    InitialWaitingPatient,
    SimPyQueueEngine,
)


class FixedServiceTimeGenerator:
    """Deterministic service generator for predictable tests."""

    def __init__(self, service_minutes: float):
        self.service_minutes = service_minutes

    def generate_service_time(self, patient_type):
        return self.service_minutes


def test_single_patient_has_zero_queue_wait():

    generator = FixedServiceTimeGenerator(
        service_minutes=5.0
    )

    engine = SimPyQueueEngine(
        station_count=4,
        service_time_generator=generator,
    )

    arrivals = [
        (0.0, PatientType.OUTPATIENT),
    ]

    results = engine.run(arrivals)

    assert len(results) == 1

    patient = results[0]

    assert patient.arrival_time == 0.0
    assert patient.service_start_time == 0.0
    assert patient.service_end_time == 5.0
    assert patient.departure_time == 5.0
    assert patient.queue_wait_minutes == 0.0
    assert patient.service_duration_minutes == 5.0

def test_patients_wait_when_all_stations_are_busy():

    generator = FixedServiceTimeGenerator(
        service_minutes=10.0
    )

    engine = SimPyQueueEngine(
        station_count=2,
        service_time_generator=generator,
    )

    arrivals = [
        (0.0, PatientType.OUTPATIENT),
        (0.0, PatientType.OUTPATIENT),
        (0.0, PatientType.OUTPATIENT),
    ]

    results = engine.run(arrivals)

    assert len(results) == 3

    waits = [
        result.queue_wait_minutes
        for result in results
    ]

    assert waits == [0.0, 0.0, 10.0]

def test_increasing_staff_reduces_waiting_time():

    generator = FixedServiceTimeGenerator(
        service_minutes=10.0
    )

    arrivals = [
        (0.0, PatientType.OUTPATIENT),
        (0.0, PatientType.OUTPATIENT),
        (0.0, PatientType.OUTPATIENT),
        (0.0, PatientType.OUTPATIENT),
    ]

    engine_two_staff = SimPyQueueEngine(
        station_count=2,
        service_time_generator=generator,
    )

    engine_four_staff = SimPyQueueEngine(
        station_count=4,
        service_time_generator=generator,
    )

    results_two = engine_two_staff.run(arrivals)
    results_four = engine_four_staff.run(arrivals)

    total_wait_two = sum(
        result.queue_wait_minutes
        for result in results_two
    )

    total_wait_four = sum(
        result.queue_wait_minutes
        for result in results_four
    )

    assert total_wait_two > total_wait_four
    assert total_wait_four == 0.0

def test_service_end_occurs_after_service_start():

    generator = FixedServiceTimeGenerator(
        service_minutes=7.5
    )

    engine = SimPyQueueEngine(
        station_count=1,
        service_time_generator=generator,
    )

    arrivals = [
        (0.0, PatientType.OUTPATIENT),
        (2.0, PatientType.OUTPATIENT),
    ]

    results = engine.run(arrivals)

    for result in results:
        assert (
            result.service_end_time
            > result.service_start_time
        )

        assert (
            result.departure_time
            == result.service_end_time
        )

def test_simpy_results_are_sorted_by_arrival_time():

    generator = FixedServiceTimeGenerator(
        service_minutes=5.0
    )

    engine = SimPyQueueEngine(
        station_count=2,
        service_time_generator=generator,
    )

    arrivals = [
        (10.0, PatientType.OUTPATIENT),
        (2.0, PatientType.INPATIENT),
        (5.0, PatientType.WALK_IN),
    ]

    results = engine.run(arrivals)

    arrival_times = [
        result.arrival_time
        for result in results
    ]

    assert arrival_times == [2.0, 5.0, 10.0]

def test_service_times_are_consistent_across_staffing_levels():
    """The same predetermined service durations must be used
    regardless of staffing level.
    """

    config = load_config("config/hospital.yaml")

    arrivals = [
        (0.0, PatientType.OUTPATIENT),
        (0.0, PatientType.INPATIENT),
        (0.0, PatientType.WALK_IN),
        (5.0, PatientType.FOLLOW_UP),
        (10.0, PatientType.OUTPATIENT),
    ]

    service_durations = [
        5.0,
        7.0,
        6.0,
        4.0,
        5.5,
    ]

    engine_2 = SimPyQueueEngine(
        station_count=2,
        service_time_generator=ServiceTimeGenerator(
            config=config,
            seed=43,
        ),
    )

    engine_4 = SimPyQueueEngine(
        station_count=4,
        service_time_generator=ServiceTimeGenerator(
            config=config,
            seed=43,
        ),
    )

    results_2 = engine_2.run(
        arrivals,
        service_durations=service_durations,
    )

    results_4 = engine_4.run(
        arrivals,
        service_durations=service_durations,
    )

    service_times_2 = {
        result.patient_id: result.service_duration_minutes
        for result in results_2
    }

    service_times_4 = {
        result.patient_id: result.service_duration_minutes
        for result in results_4
    }

    expected_service_times = {
        f"SIM_PAT_{index:06d}": duration
        for index, duration in enumerate(
            service_durations,
            start=1,
        )
    }

    assert service_times_2 == expected_service_times
    assert service_times_4 == expected_service_times
    assert service_times_2 == service_times_4


def test_initial_waiting_patient_preserves_accrued_wait():
    config = load_config(
        "config/hospital.yaml"
    )

    engine = SimPyQueueEngine(
        station_count=1,
        service_time_generator=ServiceTimeGenerator(
            config=config,
            seed=42,
        ),
    )

    initial_services = [
        InitialService(
            remaining_service_minutes=5.0,
        )
    ]

    initial_waiting = [
        InitialWaitingPatient(
            patient_type=PatientType.OUTPATIENT,
            service_duration_minutes=5.0,
            accrued_wait_minutes=12.0,
        )
    ]

    results = engine.run(
        arrivals=[],
        service_durations=[],
        initial_waiting=initial_waiting,
        initial_services=initial_services,
    )

    assert len(results) == 1

    assert (
        results[0].queue_wait_minutes
        == pytest.approx(
            17.0,
            abs=0.01,
        )
    )
