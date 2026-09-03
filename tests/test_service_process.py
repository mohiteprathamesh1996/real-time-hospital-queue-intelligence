from config.settings import load_config
from simulator.event_models import PatientType
from simulator.service_process import ServiceTimeGenerator
from simulator.event_models import Priority


def test_service_time_is_positive():
    config = load_config("config/hospital.yaml")

    generator = ServiceTimeGenerator(
        config,
        seed=42,
    )

    duration = generator.generate_service_time(
        PatientType.OUTPATIENT
    )

    assert duration > 0


def test_service_time_generation_is_reproducible():
    config = load_config("config/hospital.yaml")

    generator_1 = ServiceTimeGenerator(
        config,
        seed=42,
    )

    generator_2 = ServiceTimeGenerator(
        config,
        seed=42,
    )

    duration_1 = generator_1.generate_service_time(
        PatientType.OUTPATIENT
    )

    duration_2 = generator_2.generate_service_time(
        PatientType.OUTPATIENT
    )

    assert duration_1 == duration_2


def test_service_time_differs_by_patient_type():
    config = load_config("config/hospital.yaml")

    generator = ServiceTimeGenerator(
        config,
        seed=42,
    )

    outpatient_duration = generator.generate_service_time(
        PatientType.OUTPATIENT
    )

    inpatient_duration = generator.generate_service_time(
        PatientType.INPATIENT
    )

    assert outpatient_duration != inpatient_duration


def test_priority_generation_returns_valid_priority():
    config = load_config("config/hospital.yaml")

    generator = ServiceTimeGenerator(
        config,
        seed=42,
    )

    priority = generator.generate_priority()

    assert priority in Priority

    