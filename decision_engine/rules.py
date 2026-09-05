from decision_engine.models import (
    InterventionRecommendation,
    OperationalState,
)


def evaluate_state(
    state: OperationalState,
) -> InterventionRecommendation:

    # Severe congestion
    if (
        state.patients_waiting >= 8
        or state.sla_compliance_rate < 80
        or state.p95_wait_minutes >= 20
    ):
        return InterventionRecommendation(
            timestamp=state.timestamp,
            lab_id=state.lab_id,
            intervention_required=True,
            severity="CRITICAL",
            additional_staff=2,
            reason=(
                "Severe queue congestion detected. "
                "Deploy two additional staff."
            ),
        )

    # Developing congestion
    if (
        state.patients_waiting >= 4
        or state.p95_wait_minutes >= 10
    ):
        return InterventionRecommendation(
            timestamp=state.timestamp,
            lab_id=state.lab_id,
            intervention_required=True,
            severity="HIGH",
            additional_staff=1,
            reason=(
                "Queue pressure is increasing. "
                "Deploy one additional staff member."
            ),
        )

    # Capacity warning
    if (
        state.utilization_percentage >= 100
        and state.patients_waiting > 0
    ):
        return InterventionRecommendation(
            timestamp=state.timestamp,
            lab_id=state.lab_id,
            intervention_required=True,
            severity="WATCH",
            additional_staff=1,
            reason=(
                "All service capacity is occupied "
                "while patients are waiting."
            ),
        )

    return InterventionRecommendation(
        timestamp=state.timestamp,
        lab_id=state.lab_id,
        intervention_required=False,
        severity="NORMAL",
        additional_staff=0,
        reason="No staffing intervention required.",
    )