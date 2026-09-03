from dataclasses import dataclass
from math import factorial


@dataclass
class MMcMetrics:
    """Results from an M/M/c queueing model."""

    arrival_rate_per_hour: float
    service_rate_per_hour: float
    server_count: int

    utilization: float
    probability_zero_customers: float
    probability_wait: float

    average_queue_length: float
    average_wait_minutes: float

    average_system_length: float
    average_system_time_minutes: float


class MMcQueueModel:
    """
    M/M/c queueing model.

    Assumptions:
    - Poisson arrivals
    - Exponential service times
    - c identical servers
    - Infinite waiting room
    - FCFS discipline
    - Stable system: utilization < 1
    """

    def __init__(
        self,
        arrival_rate_per_hour: float,
        service_rate_per_hour: float,
        server_count: int,
    ):
        if arrival_rate_per_hour <= 0:
            raise ValueError(
                "arrival_rate_per_hour must be greater than 0"
            )

        if service_rate_per_hour <= 0:
            raise ValueError(
                "service_rate_per_hour must be greater than 0"
            )

        if server_count < 1:
            raise ValueError(
                "server_count must be at least 1"
            )

        self.lambda_rate = arrival_rate_per_hour
        self.mu_rate = service_rate_per_hour
        self.server_count = server_count

        self.utilization = (
            self.lambda_rate
            / (self.server_count * self.mu_rate)
        )

    def calculate(self) -> MMcMetrics:
        """Calculate M/M/c queueing metrics."""

        # ---------------------------------------------------------
        # SYSTEM STABILITY
        # ---------------------------------------------------------
        if self.utilization >= 1:
            raise ValueError(
                "M/M/c system is unstable because utilization "
                "must be less than 1."
            )

        c = self.server_count
        rho = self.utilization

        # ---------------------------------------------------------
        # P0
        #
        # P0 = [
        #   sum(n=0 to c-1) ((c*rho)^n / n!)
        #   +
        #   ((c*rho)^c / c!) * 1/(1-rho)
        # ]^-1
        # ---------------------------------------------------------

        first_sum = sum(
            (c * rho) ** n / factorial(n)
            for n in range(c)
        )

        last_term = (
            (c * rho) ** c
            / factorial(c)
        ) * (1 / (1 - rho))

        probability_zero = 1 / (
            first_sum + last_term
        )

        # ---------------------------------------------------------
        # P(WAIT)
        #
        # Erlang C formula
        # ---------------------------------------------------------

        probability_wait = (
            last_term * probability_zero
        )

        # ---------------------------------------------------------
        # Lq
        #
        # Lq = P(wait) * rho / (1-rho)
        #       * c
        # ---------------------------------------------------------

        average_queue_length = (
            probability_wait
            * rho
            / (1 - rho)
        )

        # ---------------------------------------------------------
        # Wq
        # ---------------------------------------------------------

        average_wait_hours = (
            average_queue_length
            / self.lambda_rate
        )

        average_wait_minutes = (
            average_wait_hours * 60
        )

        # ---------------------------------------------------------
        # L
        # ---------------------------------------------------------

        average_system_length = (
            average_queue_length
            + self.lambda_rate / self.mu_rate
        )

        # ---------------------------------------------------------
        # W
        # ---------------------------------------------------------

        average_system_time_hours = (
            average_system_length
            / self.lambda_rate
        )

        average_system_time_minutes = (
            average_system_time_hours * 60
        )

        return MMcMetrics(
            arrival_rate_per_hour=self.lambda_rate,
            service_rate_per_hour=self.mu_rate,
            server_count=c,
            utilization=rho,
            probability_zero_customers=probability_zero,
            probability_wait=probability_wait,
            average_queue_length=average_queue_length,
            average_wait_minutes=average_wait_minutes,
            average_system_length=average_system_length,
            average_system_time_minutes=average_system_time_minutes,
        )