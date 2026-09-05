from decision_engine.models import (
    InterventionRecommendation,
    OperationalState,
)
from decision_engine.rules import evaluate_state


class StaffingDecisionEngine:

    def evaluate(
        self,
        state: OperationalState,
    ) -> InterventionRecommendation:

        return evaluate_state(state)