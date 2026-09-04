from dataclasses import dataclass


@dataclass
class CapacityResult:
    scenario_name: str
    demand_multiplier: float
    patient_count: int
    staff_count: int
    average_wait_minutes: float
    p95_wait_minutes: float
    sla_percentage: float
    utilization_percentage: float
    meets_sla: bool


class CapacityPlanner:

    def __init__(
        self,
        queue_sla_minutes: float,
        target_sla_percentage: float,
    ):
        if queue_sla_minutes <= 0:
            raise ValueError(
                "queue_sla_minutes must be greater than 0"
            )

        if not 0 < target_sla_percentage <= 100:
            raise ValueError(
                "target_sla_percentage must be between 0 and 100"
            )

        self.queue_sla_minutes = queue_sla_minutes
        self.target_sla_percentage = target_sla_percentage

    def evaluate(
        self,
        scenario_name: str,
        demand_multiplier: float,
        patient_count: int,
        staff_count: int,
        average_wait_minutes: float,
        p95_wait_minutes: float,
        sla_percentage: float,
        utilization_percentage: float,
    ) -> CapacityResult:

        return CapacityResult(
            scenario_name=scenario_name,
            demand_multiplier=demand_multiplier,
            patient_count=patient_count,
            staff_count=staff_count,
            average_wait_minutes=average_wait_minutes,
            p95_wait_minutes=p95_wait_minutes,
            sla_percentage=sla_percentage,
            utilization_percentage=utilization_percentage,
            meets_sla=(
                sla_percentage
                >= self.target_sla_percentage
            ),
        )

    def minimum_staffing(
        self,
        results: list[CapacityResult],
    ) -> int | None:

        feasible = [
            result
            for result in results
            if result.meets_sla
        ]

        if not feasible:
            return None

        return min(
            result.staff_count
            for result in feasible
        )