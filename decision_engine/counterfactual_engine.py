from statistics import mean

from decision_engine.counterfactual import (
    CounterfactualRecommendation,
    CounterfactualResult,
)
from decision_engine.recoverability import (
    SLARecoverabilityAnalyzer,
)
from simulator.service_process import (
    ServiceTimeGenerator,
)
from simulator.simpy_engine import (
    SimPyQueueEngine,
)


class CounterfactualStaffingEngine:

    def __init__(
        self,
        config,
        baseline_staff: int,
        queue_sla_minutes: float = 15.0,
        target_sla_percentage: float = 95.0,
        min_p95_improvement_minutes: float = 2.0,
    ):
        self.config = config
        self.baseline_staff = baseline_staff
        self.queue_sla_minutes = (
            queue_sla_minutes
        )
        self.target_sla_percentage = (
            target_sla_percentage
        )

        self.min_p95_improvement_minutes = (
            min_p95_improvement_minutes
        )

        self.recoverability_analyzer = (
            SLARecoverabilityAnalyzer(
                queue_sla_minutes=(
                    queue_sla_minutes
                ),
                target_sla_percentage=(
                    target_sla_percentage
                ),
            )
        )

    @staticmethod
    def calculate_p95(values):
        if not values:
            return 0.0

        values = sorted(values)

        index = int(
            0.95 * (len(values) - 1)
        )

        return values[index]

    def evaluate(
        self,
        arrivals,
        service_durations,
        additional_staff: int,
        initial_waiting=None,
        initial_services=None,
    ) -> CounterfactualResult:

        initial_waiting = (
            initial_waiting or []
        )

        initial_services = (
            initial_services or []
        )

        staff_count = (
            self.baseline_staff
            + additional_staff
        )

        engine = SimPyQueueEngine(
            station_count=staff_count,
            service_time_generator=(
                ServiceTimeGenerator(
                    self.config,
                    seed=43,
                )
            ),
        )

        results = engine.run(
            arrivals,
            service_durations=(
                service_durations
            ),
            initial_waiting=(
                initial_waiting
            ),
            initial_services=(
                initial_services
            ),
        )

        waits = [
            result.queue_wait_minutes
            for result in results
        ]

        if not waits:
            return CounterfactualResult(
                staff_count=staff_count,
                additional_staff=(
                    additional_staff
                ),
                average_wait_minutes=0.0,
                p95_wait_minutes=0.0,
                max_wait_minutes=0.0,
                sla_percentage=100.0,
                meets_sla=True,
            )

        average_wait = mean(waits)

        p95_wait = self.calculate_p95(
            waits
        )

        max_wait = max(waits)

        within_sla = sum(
            wait <= self.queue_sla_minutes
            for wait in waits
        )

        sla_percentage = (
            within_sla
            / len(waits)
            * 100
        )

        return CounterfactualResult(
            staff_count=staff_count,
            additional_staff=(
                additional_staff
            ),
            average_wait_minutes=(
                average_wait
            ),
            p95_wait_minutes=p95_wait,
            max_wait_minutes=max_wait,
            sla_percentage=sla_percentage,
            meets_sla=(
                sla_percentage
                >= self.target_sla_percentage
            ),
        )

    def _run_scenarios(
        self,
        arrivals,
        service_durations,
        initial_waiting,
        initial_services,
        max_additional_staff,
    ):
        scenarios = []

        for additional_staff in range(
            max_additional_staff + 1
        ):
            result = self.evaluate(
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

            scenarios.append(result)

        return scenarios

    def _select_resource_aware_damage_control(
        self,
        scenarios,
    ) -> CounterfactualResult:
        """
        Select a resource-efficient damage-control
        scenario.

        Policy:

        1. Any improvement in SLA is considered
           meaningful and may justify additional
           staffing.

        2. Once SLA no longer improves, additional
           capacity is accepted only if P95 wait
           improves by at least the configured
           marginal threshold.

        3. Otherwise, retain the lower staffing
           scenario.
        """

        if not scenarios:
            raise ValueError(
                "At least one scenario is required"
            )

        selected = scenarios[0]

        epsilon = 1e-9

        for candidate in scenarios[1:]:

            sla_improvement = (
                candidate.sla_percentage
                - selected.sla_percentage
            )

            p95_improvement = (
                selected.p95_wait_minutes
                - candidate.p95_wait_minutes
            )

            # SLA improvement takes priority.
            if sla_improvement > epsilon:
                selected = candidate
                continue

            # Same SLA plateau:
            # only spend more capacity when
            # the tail-wait improvement is meaningful.
            if (
                abs(sla_improvement)
                <= epsilon
                and p95_improvement
                >= self.min_p95_improvement_minutes
            ):
                selected = candidate

        return selected

    def recommend(
        self,
        arrivals,
        service_durations,
        max_additional_staff: int = 3,
        initial_waiting=None,
        initial_services=None,
    ):

        initial_waiting = (
            initial_waiting or []
        )

        initial_services = (
            initial_services or []
        )

        accrued_waits = [
            patient.accrued_wait_minutes
            for patient in initial_waiting
        ]

        recoverability = (
            self.recoverability_analyzer
            .analyze(
                accrued_wait_minutes=(
                    accrued_waits
                ),
                future_patient_count=(
                    len(arrivals)
                ),
            )
        )

        scenarios = self._run_scenarios(
            arrivals=arrivals,
            service_durations=(
                service_durations
            ),
            initial_waiting=(
                initial_waiting
            ),
            initial_services=(
                initial_services
            ),
            max_additional_staff=(
                max_additional_staff
            ),
        )

        baseline_result = scenarios[0]

        # ========================================================
        # MODE 1:
        # SLA is mathematically unrecoverable.
        # ========================================================

        if not recoverability.is_recoverable:

            selected = (
                self
                ._select_resource_aware_damage_control(
                    scenarios
                )
            )

            sla_improvement = (
                selected.sla_percentage
                - baseline_result.sla_percentage
            )

            p95_improvement = (
                baseline_result.p95_wait_minutes
                - selected.p95_wait_minutes
            )

            return (
                CounterfactualRecommendation(
                    decision="DAMAGE_CONTROL",
                    recommended_result=selected,

                    maximum_possible_sla_percentage=(
                        recoverability
                        .maximum_possible_sla_percentage
                    ),

                    already_breached_patients=(
                        recoverability
                        .already_breached_patients
                    ),

                    reason=(
                        "The SLA target is no longer "
                        "mathematically recoverable. "
                        "The recommendation maximizes "
                        "achievable SLA and adds further "
                        "capacity only when the reduction "
                        "in P95 waiting time is operationally "
                        "meaningful."
                    ),

                    objective=(
                        "RESOURCE_AWARE_DAMAGE_CONTROL"
                    ),

                    baseline_result=(
                        baseline_result
                    ),

                    sla_improvement_percentage_points=(
                        sla_improvement
                    ),

                    p95_improvement_minutes=(
                        p95_improvement
                    ),

                    marginal_p95_threshold_minutes=(
                        self
                        .min_p95_improvement_minutes
                    ),
                ),
                scenarios,
            )

        # ========================================================
        # MODE 2:
        # SLA can still be restored.
        # ========================================================

        for result in scenarios:

            if result.meets_sla:

                if result.additional_staff == 0:
                    decision = (
                        "NO_ACTION_REQUIRED"
                    )
                else:
                    decision = (
                        "STAFFING_INTERVENTION"
                    )

                sla_improvement = (
                    result.sla_percentage
                    - baseline_result.sla_percentage
                )

                p95_improvement = (
                    baseline_result.p95_wait_minutes
                    - result.p95_wait_minutes
                )

                return (
                    CounterfactualRecommendation(
                        decision=decision,
                        recommended_result=result,

                        maximum_possible_sla_percentage=(
                            recoverability
                            .maximum_possible_sla_percentage
                        ),

                        already_breached_patients=(
                            recoverability
                            .already_breached_patients
                        ),

                        reason=(
                            "The minimum feasible staffing "
                            "scenario capable of restoring "
                            "the SLA was identified."
                        ),

                        objective="SLA_RESTORATION",

                        baseline_result=(
                            baseline_result
                        ),

                        sla_improvement_percentage_points=(
                            sla_improvement
                        ),

                        p95_improvement_minutes=(
                            p95_improvement
                        ),

                        marginal_p95_threshold_minutes=(
                            self
                            .min_p95_improvement_minutes
                        ),
                    ),
                    scenarios,
                )

        # ========================================================
        # MODE 3:
        # The SLA is theoretically recoverable,
        # but not within the configured search range.
        # ========================================================

        selected = (
            self
            ._select_resource_aware_damage_control(
                scenarios
            )
        )

        sla_improvement = (
            selected.sla_percentage
            - baseline_result.sla_percentage
        )

        p95_improvement = (
            baseline_result.p95_wait_minutes
            - selected.p95_wait_minutes
        )

        return (
            CounterfactualRecommendation(
                decision="SEARCH_LIMIT_REACHED",
                recommended_result=selected,

                maximum_possible_sla_percentage=(
                    recoverability
                    .maximum_possible_sla_percentage
                ),

                already_breached_patients=(
                    recoverability
                    .already_breached_patients
                ),

                reason=(
                    "The SLA is theoretically "
                    "recoverable, but no tested "
                    "staffing level restored it. "
                    "The best resource-aware scenario "
                    "within the configured search range "
                    "was returned."
                ),

                objective="BEST_EFFORT",

                baseline_result=(
                    baseline_result
                ),

                sla_improvement_percentage_points=(
                    sla_improvement
                ),

                p95_improvement_minutes=(
                    p95_improvement
                ),

                marginal_p95_threshold_minutes=(
                    self.min_p95_improvement_minutes
                ),
            ),
            scenarios,
        )