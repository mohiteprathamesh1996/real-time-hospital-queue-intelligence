from datetime import datetime, timedelta

from decision_engine.intervention_policy import (
    StatefulInterventionPolicy,
)
from decision_engine.intervention_state import (
    InterventionState,
)


def test_deploys_staff_when_none_active():

    policy = StatefulInterventionPolicy(
        minimum_hold_minutes=15
    )

    state = InterventionState()

    timestamp = datetime(
        2026,
        9,
        3,
        10,
        10,
    )

    action = policy.apply(
        timestamp=timestamp,
        recommended_additional_staff=1,
        state=state,
    )

    assert action.action == "DEPLOY"
    assert action.delta_staff == 1

    assert (
        state.active_additional_staff
        == 1
    )


def test_holds_staff_during_minimum_period():

    policy = StatefulInterventionPolicy(
        minimum_hold_minutes=15
    )

    start = datetime(
        2026,
        9,
        3,
        10,
        10,
    )

    state = InterventionState(
        active_additional_staff=1,
        deployed_at=start,
        last_updated_at=start,
        hold_until=(
            start
            + timedelta(
                minutes=15
            )
        ),
    )

    action = policy.apply(
        timestamp=(
            start
            + timedelta(
                minutes=5
            )
        ),
        recommended_additional_staff=0,
        state=state,
    )

    assert action.action == "HOLD"

    assert (
        state.active_additional_staff
        == 1
    )


def test_escalation_happens_immediately():

    policy = StatefulInterventionPolicy(
        minimum_hold_minutes=15
    )

    start = datetime(
        2026,
        9,
        3,
        10,
        10,
    )

    state = InterventionState(
        active_additional_staff=1,
        deployed_at=start,
        last_updated_at=start,
        hold_until=(
            start
            + timedelta(
                minutes=15
            )
        ),
    )

    action = policy.apply(
        timestamp=(
            start
            + timedelta(
                minutes=5
            )
        ),
        recommended_additional_staff=3,
        state=state,
    )

    assert action.action == "ESCALATE"
    assert action.delta_staff == 2

    assert (
        state.active_additional_staff
        == 3
    )


def test_release_after_hold_period():

    policy = StatefulInterventionPolicy(
        minimum_hold_minutes=15
    )

    start = datetime(
        2026,
        9,
        3,
        10,
        10,
    )

    state = InterventionState(
        active_additional_staff=1,
        deployed_at=start,
        last_updated_at=start,
        hold_until=(
            start
            + timedelta(
                minutes=15
            )
        ),
    )

    action = policy.apply(
        timestamp=(
            start
            + timedelta(
                minutes=15
            )
        ),
        recommended_additional_staff=0,
        state=state,
    )

    assert action.action == "RELEASE"

    assert (
        state.active_additional_staff
        == 0
    )