import pytest
from config.settings import load_config
from decision_engine.counterfactual_engine import (
    CounterfactualStaffingEngine,
)
from decision_engine.counterfactual import (
    CounterfactualResult,
)
from simulator.event_models import PatientType
from simulator.simpy_engine import InitialWaitingPatient


def test_unrecoverable_sla_uses_damage_control():
    config = load_config(
        "config/hospital.yaml"
    )

    engine = CounterfactualStaffingEngine(
        config=config,
        baseline_staff=4,
        queue_sla_minutes=15.0,
        target_sla_percentage=95.0,
    )

    initial_waiting = [
        InitialWaitingPatient(
            patient_type=PatientType.OUTPATIENT,
            service_duration_minutes=5.0,
            accrued_wait_minutes=20.0,
        ),
        InitialWaitingPatient(
            patient_type=PatientType.OUTPATIENT,
            service_duration_minutes=5.0,
            accrued_wait_minutes=20.0,
        ),
    ]

    arrivals = [
        (
            0.0,
            PatientType.OUTPATIENT,
        )
    ]

    service_durations = [
        5.0,
    ]

    recommendation, scenarios = (
        engine.recommend(
            arrivals=arrivals,
            service_durations=service_durations,
            initial_waiting=initial_waiting,
            max_additional_staff=6,
        )
    )

    assert (
        recommendation.decision
        == "DAMAGE_CONTROL"
    )

    assert (
        recommendation.objective
        == "RESOURCE_AWARE_DAMAGE_CONTROL"
    )

    assert (
        recommendation.recommended_result
        is not None
    )

    assert (
        recommendation.already_breached_patients
        == 2
    )

    assert (
        recommendation.maximum_possible_sla_percentage
        == pytest.approx(
            33.3333333333
        )
    )

    assert len(scenarios) == 7


def test_damage_control_prefers_best_sla_then_lower_staff():
    config = load_config(
        "config/hospital.yaml"
    )

    engine = CounterfactualStaffingEngine(
        config=config,
        baseline_staff=4,
        queue_sla_minutes=15.0,
        target_sla_percentage=95.0,
    )

    initial_waiting = [
        InitialWaitingPatient(
            patient_type=PatientType.OUTPATIENT,
            service_duration_minutes=10.0,
            accrued_wait_minutes=20.0,
        ),
        InitialWaitingPatient(
            patient_type=PatientType.OUTPATIENT,
            service_duration_minutes=10.0,
            accrued_wait_minutes=20.0,
        ),
    ]

    recommendation, scenarios = (
        engine.recommend(
            arrivals=[
                (
                    0.0,
                    PatientType.OUTPATIENT,
                ),
            ],
            service_durations=[10.0],
            initial_waiting=initial_waiting,
            max_additional_staff=3,
        )
    )

    selected = (
        recommendation.recommended_result
    )

    assert selected is not None

    best_sla = max(
        scenario.sla_percentage
        for scenario in scenarios
    )

    assert (
        selected.sla_percentage
        == pytest.approx(best_sla)
    )

def make_counterfactual_result(
    additional_staff,
    sla,
    p95,
):
    return CounterfactualResult(
        staff_count=4 + additional_staff,
        additional_staff=additional_staff,
        average_wait_minutes=p95 / 2,
        p95_wait_minutes=p95,
        max_wait_minutes=p95 + 2,
        sla_percentage=sla,
        meets_sla=sla >= 95.0,
    )


def test_damage_control_rejects_tiny_p95_gain():
    config = load_config(
        "config/hospital.yaml"
    )

    engine = CounterfactualStaffingEngine(
        config=config,
        baseline_staff=4,
        min_p95_improvement_minutes=2.0,
    )

    scenarios = [
        make_counterfactual_result(
            0,
            90.0,
            18.0,
        ),
        make_counterfactual_result(
            1,
            90.0,
            17.4,
        ),
        make_counterfactual_result(
            2,
            90.0,
            17.0,
        ),
    ]

    selected = (
        engine
        ._select_resource_aware_damage_control(
            scenarios
        )
    )

    assert selected.additional_staff == 0


def test_damage_control_accepts_meaningful_p95_gain():
    config = load_config(
        "config/hospital.yaml"
    )

    engine = CounterfactualStaffingEngine(
        config=config,
        baseline_staff=4,
        min_p95_improvement_minutes=2.0,
    )

    scenarios = [
        make_counterfactual_result(
            0,
            90.0,
            18.0,
        ),
        make_counterfactual_result(
            1,
            90.0,
            17.0,
        ),
        make_counterfactual_result(
            2,
            90.0,
            15.5,
        ),
        make_counterfactual_result(
            3,
            90.0,
            15.0,
        ),
    ]

    selected = (
        engine
        ._select_resource_aware_damage_control(
            scenarios
        )
    )

    assert selected.additional_staff == 2

    assert (
        selected.p95_wait_minutes
        == pytest.approx(15.5)
    )


def test_damage_control_prioritizes_sla_gain():
    config = load_config(
        "config/hospital.yaml"
    )

    engine = CounterfactualStaffingEngine(
        config=config,
        baseline_staff=4,
        min_p95_improvement_minutes=2.0,
    )

    scenarios = [
        make_counterfactual_result(
            0,
            80.0,
            18.0,
        ),
        make_counterfactual_result(
            1,
            85.0,
            17.8,
        ),
        make_counterfactual_result(
            2,
            90.0,
            17.7,
        ),
    ]

    selected = (
        engine
        ._select_resource_aware_damage_control(
            scenarios
        )
    )

    # Even though the P95 improvement is small,
    # SLA improvement takes precedence.
    assert selected.additional_staff == 2

    assert (
        selected.sla_percentage
        == pytest.approx(90.0)
    )