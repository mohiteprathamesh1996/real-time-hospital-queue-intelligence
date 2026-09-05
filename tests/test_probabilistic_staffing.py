import pytest

from config.settings import load_config
from decision_engine.probabilistic_staffing import (
    ProbabilisticStaffingEngine,
)
from simulator.event_models import PatientType


def test_single_patient_scenarios_need_no_extra_staff():

    config = load_config(
        "config/hospital.yaml"
    )

    engine = ProbabilisticStaffingEngine(
        config=config,
        baseline_staff=4,
        queue_sla_minutes=15.0,
        target_sla_percentage=95.0,
        target_success_probability=0.95,
    )

    scenarios = [
        (
            [
                (
                    0.0,
                    PatientType.OUTPATIENT,
                )
            ],
            [5.0],
        )
        for _ in range(10)
    ]

    recommendation, results = (
        engine.recommend(
            scenarios=scenarios,
            max_additional_staff=3,
        )
    )

    assert recommendation is not None

    assert (
        recommendation.additional_staff
        == 0
    )

    assert (
        recommendation.sla_success_probability
        == pytest.approx(1.0)
    )

    assert len(results) == 1


def test_invalid_probability_is_rejected():

    config = load_config(
        "config/hospital.yaml"
    )

    with pytest.raises(ValueError):
        ProbabilisticStaffingEngine(
            config=config,
            baseline_staff=4,
            target_success_probability=1.5,
        )
