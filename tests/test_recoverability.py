import pytest

from decision_engine.recoverability import (
    SLARecoverabilityAnalyzer,
)


def create_analyzer():
    return SLARecoverabilityAnalyzer(
        queue_sla_minutes=15.0,
        target_sla_percentage=95.0,
    )


def test_no_existing_breaches_is_fully_recoverable():
    analyzer = create_analyzer()

    result = analyzer.analyze(
        accrued_wait_minutes=[
            2.0,
            5.0,
            10.0,
        ],
        future_patient_count=7,
    )

    assert result.total_patients == 10
    assert result.already_breached_patients == 0
    assert (
        result.maximum_possible_sla_percentage
        == pytest.approx(100.0)
    )
    assert result.is_recoverable is True


def test_existing_breaches_can_make_sla_unrecoverable():
    analyzer = create_analyzer()

    result = analyzer.analyze(
        accrued_wait_minutes=[
            16.0,
            18.0,
            20.0,
            5.0,
        ],
        future_patient_count=6,
    )

    # 3 of 10 patients have already breached.
    # Even with infinite capacity from this instant onward,
    # only 7/10 can satisfy the SLA.

    assert result.total_patients == 10
    assert result.already_breached_patients == 3

    assert (
        result.maximum_possible_sla_percentage
        == pytest.approx(70.0)
    )

    assert result.is_recoverable is False


def test_wait_exactly_at_sla_threshold_is_not_breached():
    analyzer = create_analyzer()

    result = analyzer.analyze(
        accrued_wait_minutes=[
            15.0,
        ],
        future_patient_count=19,
    )

    assert result.already_breached_patients == 0
    assert result.is_recoverable is True


def test_empty_patient_population_is_recoverable():
    analyzer = create_analyzer()

    result = analyzer.analyze(
        accrued_wait_minutes=[],
        future_patient_count=0,
    )

    assert result.total_patients == 0

    assert (
        result.maximum_possible_sla_percentage
        == pytest.approx(100.0)
    )

    assert result.is_recoverable is True