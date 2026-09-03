from dataclasses import dataclass

from simulator.metrics import PatientMetrics, WindowMetrics
from simulator.queue_model import MMcQueueModel


@dataclass
class QueueComparison:
    """Comparison between M/M/c theory and observed simulation."""

    lab_id: str
    window_start: object
    window_end: object

    arrival_rate_per_hour: float
    service_rate_per_hour: float
    server_count: int

    theoretical_utilization: float
    theoretical_wait_minutes: float | None

    observed_wait_minutes: float
    wait_difference_minutes: float | None
    relative_error: float | None

    system_stable: bool


class QueueComparisonEngine:
    """
    Compare observed queue behavior against an M/M/c model.

    The simulation provides observed arrival and service behavior.
    M/M/c provides a theoretical baseline under stationary
    Poisson/exponential assumptions.
    """

    def __init__(self, server_count: int):
        if server_count < 1:
            raise ValueError(
                "server_count must be at least 1"
            )

        self.server_count = server_count

    def calculate_service_rate(
        self,
        patient_metrics: list[PatientMetrics],
    ) -> float:
        """Calculate service rate from observed service times."""

        if not patient_metrics:
            raise ValueError(
                "patient_metrics must not be empty"
            )

        average_service_minutes = (
            sum(
                patient.service_duration_minutes
                for patient in patient_metrics
            )
            / len(patient_metrics)
        )

        if average_service_minutes <= 0:
            raise ValueError(
                "Average service time must be greater than 0"
            )

        return 60 / average_service_minutes

    def calculate_window_service_rate(
        self,
        window: WindowMetrics,
        patient_metrics: list[PatientMetrics],
    ) -> float | None:
        """
        Calculate the observed service rate for a window.

        Service times are attributed to the window in which the
        service completed. Windows with no completed services
        return None.
        """

        patients = [
            patient
            for patient in patient_metrics
            if (
                patient.lab_id == window.lab_id
                and window.window_start
                <= patient.service_end_time
                < window.window_end
            )
        ]

        if not patients:
            return None

        average_service_minutes = (
            sum(
                patient.service_duration_minutes
                for patient in patients
            )
            / len(patients)
        )

        if average_service_minutes <= 0:
            return None

        return 60 / average_service_minutes

    def compare_window(
        self,
        window: WindowMetrics,
        service_rate_per_hour: float,
    ) -> QueueComparison:
        """Compare one operational window against M/M/c theory."""

        window_hours = (
            window.window_end - window.window_start
        ).total_seconds() / 3600

        if window_hours <= 0:
            raise ValueError(
                "Window duration must be greater than 0"
            )

        arrival_rate = (
            window.arrivals / window_hours
        )

        if arrival_rate == 0:
            return QueueComparison(
                lab_id=window.lab_id,
                window_start=window.window_start,
                window_end=window.window_end,
                arrival_rate_per_hour=0.0,
                service_rate_per_hour=service_rate_per_hour,
                server_count=self.server_count,
                theoretical_utilization=0.0,
                theoretical_wait_minutes=0.0,
                observed_wait_minutes=window.average_queue_wait_minutes,
                wait_difference_minutes=(
                    -window.average_queue_wait_minutes
                ),
                relative_error=(
                    0.0
                    if window.average_queue_wait_minutes == 0
                    else None
                ),
                system_stable=True,
            )

        theoretical_utilization = (
            arrival_rate
            / (
                self.server_count
                * service_rate_per_hour
            )
        )

        # M/M/c only has a valid steady-state solution
        # when utilization is strictly below 1.
        if theoretical_utilization >= 1:
            return QueueComparison(
                lab_id=window.lab_id,
                window_start=window.window_start,
                window_end=window.window_end,
                arrival_rate_per_hour=arrival_rate,
                service_rate_per_hour=service_rate_per_hour,
                server_count=self.server_count,
                theoretical_utilization=theoretical_utilization,
                theoretical_wait_minutes=None,
                observed_wait_minutes=window.average_queue_wait_minutes,
                wait_difference_minutes=None,
                relative_error=None,
                system_stable=False,
            )

        model = MMcQueueModel(
            arrival_rate_per_hour=arrival_rate,
            service_rate_per_hour=service_rate_per_hour,
            server_count=self.server_count,
        )

        metrics = model.calculate()

        theoretical_wait = metrics.average_wait_minutes
        observed_wait = window.average_queue_wait_minutes

        difference = (
            observed_wait - theoretical_wait
        )

        relative_error = (
            difference / theoretical_wait
            if theoretical_wait > 0
            else None
        )

        return QueueComparison(
            lab_id=window.lab_id,
            window_start=window.window_start,
            window_end=window.window_end,
            arrival_rate_per_hour=arrival_rate,
            service_rate_per_hour=service_rate_per_hour,
            server_count=self.server_count,
            theoretical_utilization=theoretical_utilization,
            theoretical_wait_minutes=theoretical_wait,
            observed_wait_minutes=observed_wait,
            wait_difference_minutes=difference,
            relative_error=relative_error,
            system_stable=True,
        )


    def compare_window_with_local_service_rate(
        self,
        window: WindowMetrics,
        patient_metrics: list[PatientMetrics],
    ) -> QueueComparison:
        """Compare a window against M/M/c using local service rate."""

        service_rate = self.calculate_window_service_rate(
            window=window,
            patient_metrics=patient_metrics,
        )

        # If no service completed in this window, we cannot
        # estimate a local service rate.
        if service_rate is None:
            return QueueComparison(
                lab_id=window.lab_id,
                window_start=window.window_start,
                window_end=window.window_end,
                arrival_rate_per_hour=0.0,
                service_rate_per_hour=0.0,
                server_count=self.server_count,
                theoretical_utilization=0.0,
                theoretical_wait_minutes=None,
                observed_wait_minutes=window.average_queue_wait_minutes,
                wait_difference_minutes=None,
                relative_error=None,
                system_stable=True,
            )

        window_hours = (
            window.window_end - window.window_start
        ).total_seconds() / 3600

        arrival_rate = (
            window.arrivals / window_hours
        )

        if arrival_rate == 0:
            return QueueComparison(
                lab_id=window.lab_id,
                window_start=window.window_start,
                window_end=window.window_end,
                arrival_rate_per_hour=0.0,
                service_rate_per_hour=service_rate,
                server_count=self.server_count,
                theoretical_utilization=0.0,
                theoretical_wait_minutes=0.0,
                observed_wait_minutes=window.average_queue_wait_minutes,
                wait_difference_minutes=(
                    -window.average_queue_wait_minutes
                ),
                relative_error=0.0,
                system_stable=True,
            )

        theoretical_utilization = (
            arrival_rate
            / (
                self.server_count
                * service_rate
            )
        )

        if theoretical_utilization >= 1:
            return QueueComparison(
                lab_id=window.lab_id,
                window_start=window.window_start,
                window_end=window.window_end,
                arrival_rate_per_hour=arrival_rate,
                service_rate_per_hour=service_rate,
                server_count=self.server_count,
                theoretical_utilization=theoretical_utilization,
                theoretical_wait_minutes=None,
                observed_wait_minutes=window.average_queue_wait_minutes,
                wait_difference_minutes=None,
                relative_error=None,
                system_stable=False,
            )

        model = MMcQueueModel(
            arrival_rate_per_hour=arrival_rate,
            service_rate_per_hour=service_rate,
            server_count=self.server_count,
        )

        metrics = model.calculate()

        theoretical_wait = metrics.average_wait_minutes
        observed_wait = window.average_queue_wait_minutes

        difference = (
            observed_wait - theoretical_wait
        )

        relative_error = (
            difference / theoretical_wait
            if theoretical_wait > 0
            else None
        )

        return QueueComparison(
            lab_id=window.lab_id,
            window_start=window.window_start,
            window_end=window.window_end,
            arrival_rate_per_hour=arrival_rate,
            service_rate_per_hour=service_rate,
            server_count=self.server_count,
            theoretical_utilization=theoretical_utilization,
            theoretical_wait_minutes=theoretical_wait,
            observed_wait_minutes=observed_wait,
            wait_difference_minutes=difference,
            relative_error=relative_error,
            system_stable=True,
        )


    def compare_windows(
        self,
        windows: list[WindowMetrics],
        patient_metrics: list[PatientMetrics],
    ) -> list[QueueComparison]:
        """Compare all operational windows against M/M/c."""

        service_rate = self.calculate_service_rate(
            patient_metrics
        )

        return [
            self.compare_window(
                window=window,
                service_rate_per_hour=service_rate,
            )
            for window in windows
        ]