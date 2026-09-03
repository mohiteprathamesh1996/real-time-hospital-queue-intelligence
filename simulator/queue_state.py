from dataclasses import dataclass
from datetime import datetime, timedelta

from simulator.metrics import PatientMetrics


@dataclass
class QueueState:
    """Queue state at a specific point in time."""

    lab_id: str
    timestamp: datetime
    patients_waiting: int


class QueueStateCalculator:
    """Calculate instantaneous queue length from patient lifecycle data."""

    def calculate(
        self,
        patient_metrics: list[PatientMetrics],
        lab_id: str,
        timestamp: datetime,
    ) -> QueueState:
        """Calculate how many patients are waiting at a given timestamp."""

        waiting_patients = [
            patient
            for patient in patient_metrics
            if (
                patient.lab_id == lab_id
                and patient.queue_entry_time <= timestamp
                and patient.service_start_time > timestamp
            )
        ]

        return QueueState(
            lab_id=lab_id,
            timestamp=timestamp,
            patients_waiting=len(waiting_patients),
        )

    def calculate_series(
        self,
        patient_metrics: list[PatientMetrics],
        lab_id: str,
        start_time: datetime,
        end_time: datetime,
        window_minutes: int = 30,
    ) -> list[QueueState]:
        """Calculate queue state at regular time intervals."""

        if window_minutes <= 0:
            raise ValueError(
                "window_minutes must be greater than 0"
            )

        if end_time <= start_time:
            raise ValueError(
                "end_time must be after start_time"
            )

        results = []

        current_time = start_time

        while current_time < end_time:
            results.append(
                self.calculate(
                    patient_metrics=patient_metrics,
                    lab_id=lab_id,
                    timestamp=current_time,
                )
            )

            current_time += timedelta(
                minutes=window_minutes
            )

        return results