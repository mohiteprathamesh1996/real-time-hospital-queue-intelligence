from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class OperationalState:
    timestamp: datetime
    lab_id: str

    patients_arrived: int
    patients_waiting: int
    patients_in_service: int

    utilization_percentage: float

    average_wait_minutes: float
    p95_wait_minutes: float

    sla_compliance_rate: float

    queue_pressure: str
    operational_status: str


@dataclass(frozen=True)
class InterventionRecommendation:
    timestamp: datetime
    lab_id: str

    intervention_required: bool
    severity: str

    additional_staff: int

    reason: str