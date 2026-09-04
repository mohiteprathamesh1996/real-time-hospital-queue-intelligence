from dataclasses import dataclass


@dataclass
class StaffingScenario:
    staff_count: int
    average_wait_minutes: float
    p95_wait_minutes: float
    max_wait_minutes: float
    sla_percentage: float
    utilization_percentage: float
    hourly_staffing_cost: float
    meets_sla: bool


class StaffingOptimizer:
    def __init__(
        self,
        hourly_cost_per_staff: float,
        target_sla_percentage: float,
        queue_sla_minutes: float,
    ):
        if hourly_cost_per_staff < 0:
            raise ValueError(
                "hourly_cost_per_staff cannot be negative"
            )

        if not 0 < target_sla_percentage <= 100:
            raise ValueError(
                "target_sla_percentage must be between 0 and 100"
            )

        if queue_sla_minutes <= 0:
            raise ValueError(
                "queue_sla_minutes must be greater than 0"
            )

        self.hourly_cost_per_staff = hourly_cost_per_staff
        self.target_sla_percentage = target_sla_percentage
        self.queue_sla_minutes = queue_sla_minutes

    def evaluate_scenario(
        self,
        staff_count: int,
        average_wait_minutes: float,
        p95_wait_minutes: float,
        max_wait_minutes: float,
        sla_percentage: float,
        utilization_percentage: float,
    ) -> StaffingScenario:

        if staff_count < 1:
            raise ValueError(
                "staff_count must be at least 1"
            )

        meets_sla = (
            sla_percentage
            >= self.target_sla_percentage
        )

        return StaffingScenario(
            staff_count=staff_count,
            average_wait_minutes=average_wait_minutes,
            p95_wait_minutes=p95_wait_minutes,
            max_wait_minutes=max_wait_minutes,
            sla_percentage=sla_percentage,
            utilization_percentage=utilization_percentage,
            hourly_staffing_cost=(
                staff_count
                * self.hourly_cost_per_staff
            ),
            meets_sla=meets_sla,
        )

    def find_minimum_staffing(
        self,
        scenarios: list[StaffingScenario],
    ) -> StaffingScenario:

        feasible = [
            scenario
            for scenario in scenarios
            if scenario.meets_sla
        ]

        if not feasible:
            raise ValueError(
                "No staffing scenario satisfies the SLA"
            )

        return min(
            feasible,
            key=lambda scenario: (
                scenario.hourly_staffing_cost,
                scenario.staff_count,
            ),
        )