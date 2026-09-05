from dataclasses import dataclass


@dataclass(frozen=True)
class StaffingCostResult:
    staff_hours: float
    hourly_cost_per_staff: float
    total_cost: float


class StaffingCostModel:

    def __init__(
        self,
        hourly_cost_per_staff: float,
    ):
        if hourly_cost_per_staff < 0:
            raise ValueError(
                "hourly_cost_per_staff cannot be negative"
            )

        self.hourly_cost_per_staff = (
            hourly_cost_per_staff
        )

    def calculate(
        self,
        staff_hours: float,
    ) -> StaffingCostResult:

        if staff_hours < 0:
            raise ValueError(
                "staff_hours cannot be negative"
            )

        total_cost = (
            staff_hours
            * self.hourly_cost_per_staff
        )

        return StaffingCostResult(
            staff_hours=staff_hours,
            hourly_cost_per_staff=(
                self.hourly_cost_per_staff
            ),
            total_cost=total_cost,
        )