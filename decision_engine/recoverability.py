from dataclasses import dataclass


@dataclass(frozen=True)
class RecoverabilityResult:
    total_patients: int
    already_breached_patients: int
    maximum_possible_sla_percentage: float
    is_recoverable: bool


class SLARecoverabilityAnalyzer:
    """Determine whether an SLA can still be mathematically recovered.

    Patients whose accrued waiting time already exceeds the queue SLA
    cannot have that historical waiting time undone by adding capacity.
    """

    def __init__(
        self,
        queue_sla_minutes: float,
        target_sla_percentage: float,
    ):
        self.queue_sla_minutes = queue_sla_minutes
        self.target_sla_percentage = target_sla_percentage

    def analyze(
        self,
        accrued_wait_minutes,
        future_patient_count: int,
    ) -> RecoverabilityResult:

        accrued_waits = list(accrued_wait_minutes)

        already_breached = sum(
            wait > self.queue_sla_minutes
            for wait in accrued_waits
        )

        total_patients = (
            len(accrued_waits)
            + future_patient_count
        )

        if total_patients == 0:
            maximum_possible_sla = 100.0
        else:
            maximum_possible_sla = (
                (
                    total_patients
                    - already_breached
                )
                / total_patients
                * 100
            )

        return RecoverabilityResult(
            total_patients=total_patients,
            already_breached_patients=already_breached,
            maximum_possible_sla_percentage=(
                maximum_possible_sla
            ),
            is_recoverable=(
                maximum_possible_sla
                >= self.target_sla_percentage
            ),
        )