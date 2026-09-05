from dataclasses import dataclass
from statistics import mean

from decision_engine.counterfactual_engine import (
    CounterfactualStaffingEngine,
)


@dataclass(frozen=True)
class ProbabilisticStaffingResult:
    staff_count: int
    additional_staff: int

    scenario_count: int

    sla_success_probability: float

    average_sla_percentage: float
    average_p95_wait_minutes: float
    average_wait_minutes: float

    meets_confidence_target: bool


class ProbabilisticStaffingEngine:
    """
    Evaluates staffing levels across many possible futures.

    Instead of saying:

        +1 staff => SLA = 100%

    the engine answers:

        +1 staff => 82% probability of meeting SLA

    The recommended staffing level is the minimum capacity
    whose probability of meeting the SLA exceeds the
    configured confidence threshold.
    """

    def __init__(
        self,
        config,
        baseline_staff: int,
        queue_sla_minutes: float = 15.0,
        target_sla_percentage: float = 95.0,
        target_success_probability: float = 0.95,
    ):
        if not (
            0.0
            <= target_success_probability
            <= 1.0
        ):
            raise ValueError(
                "target_success_probability "
                "must be between 0 and 1"
            )

        self.config = config
        self.baseline_staff = baseline_staff

        self.queue_sla_minutes = (
            queue_sla_minutes
        )

        self.target_sla_percentage = (
            target_sla_percentage
        )

        self.target_success_probability = (
            target_success_probability
        )

    def evaluate(
        self,
        scenarios,
        additional_staff: int,
        initial_waiting=None,
        initial_services=None,
    ) -> ProbabilisticStaffingResult:

        if not scenarios:
            raise ValueError(
                "At least one future scenario is required"
            )

        deterministic_engine = (
            CounterfactualStaffingEngine(
                config=self.config,
                baseline_staff=(
                    self.baseline_staff
                ),
                queue_sla_minutes=(
                    self.queue_sla_minutes
                ),
                target_sla_percentage=(
                    self.target_sla_percentage
                ),
            )
        )

        simulation_results = []

        for (
            arrivals,
            service_durations,
        ) in scenarios:

            result = (
                deterministic_engine.evaluate(
                    arrivals=arrivals,
                    service_durations=(
                        service_durations
                    ),
                    additional_staff=(
                        additional_staff
                    ),
                    initial_waiting=(
                        initial_waiting
                    ),
                    initial_services=(
                        initial_services
                    ),
                )
            )

            simulation_results.append(
                result
            )

        successes = sum(
            result.meets_sla
            for result in simulation_results
        )

        success_probability = (
            successes
            / len(simulation_results)
        )

        return ProbabilisticStaffingResult(
            staff_count=(
                self.baseline_staff
                + additional_staff
            ),
            additional_staff=(
                additional_staff
            ),
            scenario_count=(
                len(simulation_results)
            ),
            sla_success_probability=(
                success_probability
            ),
            average_sla_percentage=mean(
                result.sla_percentage
                for result in simulation_results
            ),
            average_p95_wait_minutes=mean(
                result.p95_wait_minutes
                for result in simulation_results
            ),
            average_wait_minutes=mean(
                result.average_wait_minutes
                for result in simulation_results
            ),
            meets_confidence_target=(
                success_probability
                >= self.target_success_probability
            ),
        )

    def recommend(
        self,
        scenarios,
        max_additional_staff: int,
        initial_waiting=None,
        initial_services=None,
    ):
        evaluated = []

        for additional_staff in range(
            max_additional_staff + 1
        ):

            result = self.evaluate(
                scenarios=scenarios,
                additional_staff=(
                    additional_staff
                ),
                initial_waiting=(
                    initial_waiting
                ),
                initial_services=(
                    initial_services
                ),
            )

            evaluated.append(
                result
            )

            if result.meets_confidence_target:
                return (
                    result,
                    evaluated,
                )

        return (
            None,
            evaluated,
        )
