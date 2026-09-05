from statistics import mean

from decision_engine.counterfactual import (
    CounterfactualResult,
)
from simulator.service_process import ServiceTimeGenerator
from simulator.simpy_engine import SimPyQueueEngine


class CounterfactualStaffingEngine:

    def __init__(
        self,
        config,
        baseline_staff: int,
        queue_sla_minutes: float = 15.0,
        target_sla_percentage: float = 95.0,
    ):
        self.config = config
        self.baseline_staff = baseline_staff
        self.queue_sla_minutes = queue_sla_minutes
        self.target_sla_percentage = (
            target_sla_percentage
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
            service_durations=service_durations,
            initial_waiting=initial_waiting,
            initial_services=initial_services,
        )

        waits = [
            result.queue_wait_minutes
            for result in results
        ]

        if not waits:
            return CounterfactualResult(
                staff_count=staff_count,
                additional_staff=additional_staff,
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
            additional_staff=additional_staff,
            average_wait_minutes=average_wait,
            p95_wait_minutes=p95_wait,
            max_wait_minutes=max_wait,
            sla_percentage=sla_percentage,
            meets_sla=(
                sla_percentage
                >= self.target_sla_percentage
            ),
        )

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

        scenarios = []

        for additional_staff in range(
            max_additional_staff + 1
        ):
            result = self.evaluate(
                arrivals=arrivals,
                service_durations=service_durations,
                additional_staff=additional_staff,
                initial_waiting=initial_waiting,
                initial_services=initial_services,
            )

            scenarios.append(result)

            if result.meets_sla:
                return result, scenarios

        return None, scenarios