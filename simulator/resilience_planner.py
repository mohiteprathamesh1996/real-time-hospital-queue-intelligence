from dataclasses import dataclass


@dataclass
class ResilienceResult:
    demand_scenario: str
    planned_staff: int
    unavailable_staff: int
    available_staff: int
    sla_percentage: float
    average_wait_minutes: float
    p95_wait_minutes: float
    meets_sla: bool


class ResiliencePlanner:

    def __init__(
        self,
        target_sla_percentage: float,
    ):
        if not 0 < target_sla_percentage <= 100:
            raise ValueError(
                "target_sla_percentage must be between 0 and 100"
            )

        self.target_sla_percentage = target_sla_percentage

    def evaluate(
        self,
        demand_scenario: str,
        planned_staff: int,
        unavailable_staff: int,
        available_staff: int,
        sla_percentage: float,
        average_wait_minutes: float,
        p95_wait_minutes: float,
    ) -> ResilienceResult:

        meets_sla = (
            sla_percentage >= self.target_sla_percentage
        )

        return ResilienceResult(
            demand_scenario=demand_scenario,
            planned_staff=planned_staff,
            unavailable_staff=unavailable_staff,
            available_staff=available_staff,
            sla_percentage=sla_percentage,
            average_wait_minutes=average_wait_minutes,
            p95_wait_minutes=p95_wait_minutes,
            meets_sla=meets_sla,
        )

    def minimum_resilient_staffing(
        self,
        results: list[ResilienceResult],
        unavailable_staff: int,
    ) -> int | None:

        feasible = [
            result
            for result in results
            if (
                result.unavailable_staff == unavailable_staff
                and result.meets_sla
            )
        ]

        if not feasible:
            return None

        return min(
            result.planned_staff
            for result in feasible
        )