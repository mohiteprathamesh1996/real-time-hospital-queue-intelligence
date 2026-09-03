from config.settings import load_config


def test_hospital_config_loads():
    config = load_config("config/hospital.yaml")

    assert config.hospital.name == "Demo General Hospital"

    assert len(config.labs) == 2

    assert config.labs[0].lab_id == "LAB_A"
    assert config.labs[0].stations == 4

    assert config.patient_profiles["outpatient"].arrival_rate_per_hour == 35


def test_fault_injection_config():
    config = load_config("config/hospital.yaml")

    fault_config = config.simulation.fault_injection

    assert fault_config.duplicate_rate == 0.01
    assert fault_config.late_event_rate == 0.03

def test_arrival_rate_schedule():
    config = load_config("config/hospital.yaml")

    schedule = config.simulation.arrival_rate_schedule

    assert len(schedule) == 4

    assert schedule[0].start_hour == 6
    assert schedule[0].end_hour == 8
    assert schedule[0].rate_per_hour == 20

    assert schedule[1].start_hour == 8
    assert schedule[1].end_hour == 11
    assert schedule[1].rate_per_hour == 45