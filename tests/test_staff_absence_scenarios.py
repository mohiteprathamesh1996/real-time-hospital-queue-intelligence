import pytest

from simulator.staff_absence_scenarios import (
    StaffAbsenceScenarioGenerator,
)


def test_staff_absence_scenario():
    generator = StaffAbsenceScenarioGenerator()

    scenario = generator.generate(
        planned_staff=4,
        unavailable_staff=1,
    )

    assert scenario.planned_staff == 4
    assert scenario.unavailable_staff == 1
    assert scenario.available_staff == 3
    assert scenario.name == "1 staff absent"


def test_no_staff_absent():
    generator = StaffAbsenceScenarioGenerator()

    scenario = generator.generate(
        planned_staff=4,
        unavailable_staff=0,
    )

    assert scenario.available_staff == 4


def test_invalid_absence():
    generator = StaffAbsenceScenarioGenerator()

    with pytest.raises(ValueError):
        generator.generate(
            planned_staff=4,
            unavailable_staff=4,
        )


def test_negative_absence():
    generator = StaffAbsenceScenarioGenerator()

    with pytest.raises(ValueError):
        generator.generate(
            planned_staff=4,
            unavailable_staff=-1,
        )