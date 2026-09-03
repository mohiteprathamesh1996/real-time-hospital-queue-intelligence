from datetime import datetime, timedelta, timezone
from simulator.arrival_process import PoissonArrivalProcess
from simulator.event_models import PatientType
from config.settings import load_config


def test_arrival_process_is_reproducible():
    start_time = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    process_1 = PoissonArrivalProcess(rate_per_hour=30, seed=42)
    process_2 = PoissonArrivalProcess(rate_per_hour=30, seed=42)

    arrival_1 = process_1.next_arrival_time(start_time)
    arrival_2 = process_2.next_arrival_time(start_time)

    assert arrival_1 == arrival_2
    assert arrival_1 > start_time

def test_arrival_process_produces_expected_average_rate():
    start_time = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    process = PoissonArrivalProcess(
        rate_per_hour=30,
        seed=42,
    )

    current_time = start_time
    arrivals = 0

    simulation_hours = 100

    end_time = end_time = start_time + timedelta(hours=simulation_hours)

    while current_time < end_time:
        current_time = process.next_arrival_time(current_time)

        if current_time <= end_time:
            arrivals += 1

    observed_rate = arrivals / simulation_hours

    assert 27 <= observed_rate <= 33

def test_arrival_rate_changes_by_time_of_day():
    process = PoissonArrivalProcess(
        rate_per_hour=30,
        seed=42,
        rate_schedule=[
            (6, 8, 20),
            (8, 11, 45),
            (11, 14, 30),
            (14, 18, 15),
        ],
    )

    assert process.get_rate_per_hour(
        datetime(2026, 9, 3, 7, 0, tzinfo=timezone.utc)
    ) == 20

    assert process.get_rate_per_hour(
        datetime(2026, 9, 3, 9, 0, tzinfo=timezone.utc)
    ) == 45

    assert process.get_rate_per_hour(
        datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)
    ) == 30

    assert process.get_rate_per_hour(
        datetime(2026, 9, 3, 16, 0, tzinfo=timezone.utc)
    ) == 15

def test_arrival_process_can_be_created_from_config():
    config = load_config("config/hospital.yaml")

    process = PoissonArrivalProcess.from_config(
        config,
        seed=42,
    )

    assert process.get_rate_per_hour(
        datetime(2026, 9, 3, 7, 0, tzinfo=timezone.utc)
    ) == 20

    assert process.get_rate_per_hour(
        datetime(2026, 9, 3, 9, 0, tzinfo=timezone.utc)
    ) == 45

def test_arrival_process_uses_default_rate_outside_schedule():
    config = load_config("config/hospital.yaml")

    process = PoissonArrivalProcess.from_config(
        config,
        seed=42,
    )

    assert process.get_rate_per_hour(
        datetime(2026, 9, 3, 5, 0, tzinfo=timezone.utc)
    ) == 10

    assert process.get_rate_per_hour(
        datetime(2026, 9, 3, 19, 0, tzinfo=timezone.utc)
    ) == 10

def test_generate_arrivals_within_time_window():
    config = load_config("config/hospital.yaml")

    process = PoissonArrivalProcess.from_config(
        config,
        seed=42,
    )

    start_time = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    end_time = start_time + timedelta(hours=1)

    arrivals = process.generate_arrivals(
        start_time=start_time,
        end_time=end_time,
    )

    assert len(arrivals) > 0

    assert all(
        start_time < arrival <= end_time
        for arrival in arrivals
    )

    assert arrivals == sorted(arrivals)

def test_generate_arrivals_can_cross_rate_boundary():
    config = load_config("config/hospital.yaml")

    process = PoissonArrivalProcess.from_config(
        config,
        seed=42,
    )

    start_time = datetime(
        2026,
        9,
        3,
        7,
        55,
        tzinfo=timezone.utc,
    )

    end_time = datetime(
        2026,
        9,
        3,
        8,
        5,
        tzinfo=timezone.utc,
    )

    arrivals = process.generate_arrivals(
        start_time=start_time,
        end_time=end_time,
    )

    assert len(arrivals) > 0

    assert all(
        start_time < arrival <= end_time
        for arrival in arrivals
    )

    assert arrivals == sorted(arrivals)

def test_arrivals_are_assigned_patient_types():
    config = load_config("config/hospital.yaml")

    process = PoissonArrivalProcess.from_config(
        config,
        seed=42,
    )

    start_time = datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

    end_time = start_time + timedelta(hours=1)

    arrivals = process.generate_patient_arrivals(
        start_time=start_time,
        end_time=end_time,
        patient_type_rates={
            PatientType.OUTPATIENT: 35,
            PatientType.INPATIENT: 15,
            PatientType.WALK_IN: 10,
            PatientType.FOLLOW_UP: 8,
        },
    )

    assert len(arrivals) > 0

    assert all(
        patient_type in PatientType
        for _, patient_type in arrivals
    )

def test_next_rate_boundary():
    process = PoissonArrivalProcess(
        rate_per_hour=10,
        seed=42,
        rate_schedule=[
            (6, 8, 20),
            (8, 11, 45),
            (11, 14, 30),
            (14, 18, 15),
        ],
    )

    current_time = datetime(
        2026,
        9,
        3,
        7,
        55,
        tzinfo=timezone.utc,
    )

    boundary = process.get_next_rate_boundary(current_time)

    assert boundary == datetime(
        2026,
        9,
        3,
        8,
        0,
        tzinfo=timezone.utc,
    )

