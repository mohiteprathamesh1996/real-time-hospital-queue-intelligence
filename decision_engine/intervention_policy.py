from dataclasses import dataclass
from datetime import datetime, timedelta

from decision_engine.intervention_state import (
    InterventionState,
)


@dataclass(frozen=True)
class InterventionAction:
    timestamp: datetime

    previous_staff: int
    recommended_staff: int

    delta_staff: int

    action: str

    reason: str


class StatefulInterventionPolicy:

    def __init__(
        self,
        minimum_hold_minutes: int = 15,
    ):
        self.minimum_hold_minutes = (
            minimum_hold_minutes
        )

    def apply(
        self,
        timestamp: datetime,
        recommended_additional_staff: int,
        state: InterventionState,
    ):
        current = (
            state.active_additional_staff
        )

        recommended = (
            recommended_additional_staff
        )

        # --------------------------------------------------------
        # No current intervention
        # --------------------------------------------------------

        if current == 0:

            if recommended == 0:
                return InterventionAction(
                    timestamp=timestamp,
                    previous_staff=0,
                    recommended_staff=0,
                    delta_staff=0,
                    action="NO_ACTION",
                    reason=(
                        "No additional staffing required."
                    ),
                )

            state.active_additional_staff = (
                recommended
            )

            state.deployed_at = timestamp

            state.last_updated_at = (
                timestamp
            )

            state.hold_until = (
                timestamp
                + timedelta(
                    minutes=self.minimum_hold_minutes
                )
            )

            return InterventionAction(
                timestamp=timestamp,
                previous_staff=0,
                recommended_staff=recommended,
                delta_staff=recommended,
                action="DEPLOY",
                reason=(
                    "Additional capacity deployed."
                ),
            )

        # --------------------------------------------------------
        # Escalation
        # --------------------------------------------------------

        if recommended > current:

            delta = (
                recommended
                - current
            )

            state.active_additional_staff = (
                recommended
            )

            state.last_updated_at = timestamp

            state.hold_until = (
                timestamp
                + timedelta(
                    minutes=self.minimum_hold_minutes
                )
            )

            return InterventionAction(
                timestamp=timestamp,
                previous_staff=current,
                recommended_staff=recommended,
                delta_staff=delta,
                action="ESCALATE",
                reason=(
                    "Additional capacity increased "
                    "because the required staffing "
                    "level rose."
                ),
            )

        # --------------------------------------------------------
        # Recommendation unchanged
        # --------------------------------------------------------

        if recommended == current:

            state.last_updated_at = (
                timestamp
            )

            return InterventionAction(
                timestamp=timestamp,
                previous_staff=current,
                recommended_staff=current,
                delta_staff=0,
                action="HOLD",
                reason=(
                    "Current staffing intervention "
                    "remains appropriate."
                ),
            )

        # --------------------------------------------------------
        # Recommendation has fallen
        # --------------------------------------------------------

        if (
            state.hold_until is not None
            and timestamp < state.hold_until
        ):
            state.last_updated_at = (
                timestamp
            )

            return InterventionAction(
                timestamp=timestamp,
                previous_staff=current,
                recommended_staff=current,
                delta_staff=0,
                action="HOLD",
                reason=(
                    "Minimum intervention hold period "
                    "has not yet elapsed."
                ),
            )

        # --------------------------------------------------------
        # De-escalation / release
        # --------------------------------------------------------

        delta = (
            recommended
            - current
        )

        state.active_additional_staff = (
            recommended
        )

        state.last_updated_at = timestamp

        if recommended == 0:
            state.deployed_at = None
            state.hold_until = None

            action = "RELEASE"

            reason = (
                "Additional capacity released "
                "after the hold period."
            )

        else:
            state.hold_until = (
                timestamp
                + timedelta(
                    minutes=self.minimum_hold_minutes
                )
            )

            action = "DEESCALATE"

            reason = (
                "Additional capacity reduced "
                "after the hold period."
            )

        return InterventionAction(
            timestamp=timestamp,
            previous_staff=current,
            recommended_staff=recommended,
            delta_staff=delta,
            action=action,
            reason=reason,
        )