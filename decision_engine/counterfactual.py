from dataclasses import dataclass


@dataclass(frozen=True)
class CounterfactualResult:
    staff_count: int
    additional_staff: int

    average_wait_minutes: float
    p95_wait_minutes: float
    max_wait_minutes: float

    sla_percentage: float

    meets_sla: bool