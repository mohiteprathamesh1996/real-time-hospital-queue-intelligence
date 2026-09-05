from datetime import datetime

from decision_engine.engine import StaffingDecisionEngine
from decision_engine.models import OperationalState


def make_state(
    patients_waiting=0,
    utilization_percentage=50.0,
    p95_wait_minutes=0.0,
    sla_compliance_rate=100.0,
):
    return OperationalState(
        timestamp=datetime(2026, 9, 3, 10, 0),
        lab_id="LAB_A",
        patients_arrived=5,
        patients_waiting=patients_waiting,
        patients_in_service=4,
        utilization_percentage=utilization_percentage,
        average_wait_minutes=0.0,
        p95_wait_minutes=p95_wait_minutes,
        sla_compliance_rate=sla_compliance_rate,
        queue_pressure="NONE",
        operational_status="GREEN",
    )


def test_normal_operation_requires_no_intervention():
    engine = StaffingDecisionEngine()

    recommendation = engine.evaluate(
        make_state()
    )

    assert recommendation.intervention_required is False
    assert recommendation.additional_staff == 0
    assert recommendation.severity == "NORMAL"


def test_full_utilization_without_queue_does_not_trigger_staffing():
    engine = StaffingDecisionEngine()

    recommendation = engine.evaluate(
        make_state(
            patients_waiting=0,
            utilization_percentage=100.0,
        )
    )

    assert recommendation.intervention_required is False
    assert recommendation.additional_staff == 0


def test_waiting_patients_at_full_capacity_trigger_intervention():
    engine = StaffingDecisionEngine()

    recommendation = engine.evaluate(
        make_state(
            patients_waiting=2,
            utilization_percentage=100.0,
        )
    )

    assert recommendation.intervention_required is True
    assert recommendation.additional_staff == 1
    assert recommendation.severity == "WATCH"


def test_developing_queue_requires_one_extra_staff():
    engine = StaffingDecisionEngine()

    recommendation = engine.evaluate(
        make_state(
            patients_waiting=5,
            utilization_percentage=100.0,
        )
    )

    assert recommendation.intervention_required is True
    assert recommendation.additional_staff == 1
    assert recommendation.severity == "HIGH"


def test_severe_queue_requires_two_extra_staff():
    engine = StaffingDecisionEngine()

    recommendation = engine.evaluate(
        make_state(
            patients_waiting=10,
            utilization_percentage=100.0,
        )
    )

    assert recommendation.intervention_required is True
    assert recommendation.additional_staff == 2
    assert recommendation.severity == "CRITICAL"


def test_sla_collapse_triggers_critical_intervention():
    engine = StaffingDecisionEngine()

    recommendation = engine.evaluate(
        make_state(
            patients_waiting=3,
            utilization_percentage=100.0,
            sla_compliance_rate=40.0,
        )
    )

    assert recommendation.intervention_required is True
    assert recommendation.additional_staff == 2
    assert recommendation.severity == "CRITICAL"


def test_extreme_p95_triggers_critical_intervention():
    engine = StaffingDecisionEngine()

    recommendation = engine.evaluate(
        make_state(
            patients_waiting=3,
            utilization_percentage=100.0,
            p95_wait_minutes=22.0,
        )
    )

    assert recommendation.intervention_required is True
    assert recommendation.additional_staff == 2
    assert recommendation.severity == "CRITICAL"