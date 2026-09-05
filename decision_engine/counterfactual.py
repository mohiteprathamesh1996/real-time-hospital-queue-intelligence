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


@dataclass(frozen=True)
class CounterfactualRecommendation:
    decision: str

    recommended_result: CounterfactualResult | None

    maximum_possible_sla_percentage: float
    already_breached_patients: int

    reason: str
    objective: str

    baseline_result: CounterfactualResult | None

    sla_improvement_percentage_points: float
    p95_improvement_minutes: float

    marginal_p95_threshold_minutes: float