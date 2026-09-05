import pytest

from decision_engine.cost_model import (
    StaffingCostModel,
)


def test_staffing_cost_is_calculated_correctly():

    model = StaffingCostModel(
        hourly_cost_per_staff=35.0
    )

    result = model.calculate(
        staff_hours=2.25
    )

    assert (
        result.total_cost
        == pytest.approx(
            78.75
        )
    )


def test_zero_staff_hours_has_zero_cost():

    model = StaffingCostModel(
        hourly_cost_per_staff=35.0
    )

    result = model.calculate(
        staff_hours=0.0
    )

    assert result.total_cost == 0.0


def test_negative_staff_hours_are_rejected():

    model = StaffingCostModel(
        hourly_cost_per_staff=35.0
    )

    with pytest.raises(ValueError):
        model.calculate(
            staff_hours=-1.0
        )


def test_negative_hourly_cost_is_rejected():

    with pytest.raises(ValueError):
        StaffingCostModel(
            hourly_cost_per_staff=-10.0
        )
