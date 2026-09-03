from dataclasses import dataclass
from datetime import datetime, timedelta

from simulator.event_models import EventType, HospitalEvent


@dataclass
class PatientMetrics:
    patient_id: str
    lab_id: str

    arrival_time: datetime
    registration_time: datetime
    queue_entry_time: datetime
    service_start_time: datetime
    service_end_time: datetime
    departure_time: datetime

    registration_duration_minutes: float
    queue_wait_minutes: float
    service_duration_minutes: float
    total_journey_minutes: float


@dataclass
class LabMetrics:
    lab_id: str
    total_patients: int
    patients_served: int
    total_service_minutes: float
    utilization: float
    patients_per_hour: float


@dataclass
class SLAMetrics:
    lab_id: str
    total_patients: int
    patients_within_sla: int
    patients_breaching_sla: int
    sla_breach_rate: float


@dataclass
class WindowMetrics:
    lab_id: str
    window_start: datetime
    window_end: datetime

    arrivals: int
    patients_served: int

    average_queue_wait_minutes: float
    maximum_queue_wait_minutes: float
    average_service_time_minutes: float

    utilization: float
    sla_breach_rate: float


class MetricsCalculator:
    """Calculate operational metrics from hospital lifecycle events."""

    def __init__(
        self,
        queue_sla_minutes: float = 15.0,
    ):
        self.queue_sla_minutes = queue_sla_minutes

    def calculate_patient_metrics(
        self,
        events: list[HospitalEvent],
    ) -> list[PatientMetrics]:

        events_by_patient: dict[str, list[HospitalEvent]] = {}

        for event in events:
            events_by_patient.setdefault(
                event.patient_id,
                [],
            ).append(event)

        patient_metrics = []

        for patient_id, patient_events in events_by_patient.items():

            event_map = {
                event.event_type: event
                for event in patient_events
            }

            required_events = [
                EventType.PATIENT_ARRIVAL,
                EventType.REGISTRATION_COMPLETED,
                EventType.QUEUE_ENTERED,
                EventType.SERVICE_STARTED,
                EventType.SERVICE_COMPLETED,
                EventType.PATIENT_DEPARTED,
            ]

            if not all(
                event_type in event_map
                for event_type in required_events
            ):
                continue

            arrival_time = event_map[
                EventType.PATIENT_ARRIVAL
            ].event_time

            registration_time = event_map[
                EventType.REGISTRATION_COMPLETED
            ].event_time

            queue_entry_time = event_map[
                EventType.QUEUE_ENTERED
            ].event_time

            service_start_time = event_map[
                EventType.SERVICE_STARTED
            ].event_time

            service_end_time = event_map[
                EventType.SERVICE_COMPLETED
            ].event_time

            departure_time = event_map[
                EventType.PATIENT_DEPARTED
            ].event_time

            registration_duration = (
                registration_time - arrival_time
            ).total_seconds() / 60

            queue_wait = (
                service_start_time - queue_entry_time
            ).total_seconds() / 60

            service_duration = (
                service_end_time - service_start_time
            ).total_seconds() / 60

            total_journey = (
                departure_time - arrival_time
            ).total_seconds() / 60

            lab_id = event_map[
                EventType.PATIENT_ARRIVAL
            ].lab_id

            patient_metrics.append(
                PatientMetrics(
                    patient_id=patient_id,
                    lab_id=lab_id,
                    arrival_time=arrival_time,
                    registration_time=registration_time,
                    queue_entry_time=queue_entry_time,
                    service_start_time=service_start_time,
                    service_end_time=service_end_time,
                    departure_time=departure_time,
                    registration_duration_minutes=registration_duration,
                    queue_wait_minutes=queue_wait,
                    service_duration_minutes=service_duration,
                    total_journey_minutes=total_journey,
                )
            )

        return sorted(
            patient_metrics,
            key=lambda patient: patient.arrival_time,
        )

    def calculate_lab_metrics(
        self,
        patient_metrics: list[PatientMetrics],
        lab_id: str,
        operating_hours: float,
        station_count: int,
    ) -> LabMetrics:

        lab_patients = [
            patient
            for patient in patient_metrics
            if patient.lab_id == lab_id
        ]

        total_service_minutes = sum(
            patient.service_duration_minutes
            for patient in lab_patients
        )

        available_staff_minutes = (
            operating_hours * 60 * station_count
        )

        utilization = (
            total_service_minutes / available_staff_minutes
            if available_staff_minutes > 0
            else 0.0
        )

        patients_per_hour = (
            len(lab_patients) / operating_hours
            if operating_hours > 0
            else 0.0
        )

        return LabMetrics(
            lab_id=lab_id,
            total_patients=len(lab_patients),
            patients_served=len(lab_patients),
            total_service_minutes=total_service_minutes,
            utilization=utilization,
            patients_per_hour=patients_per_hour,
        )

    def calculate_sla_metrics(
        self,
        patient_metrics: list[PatientMetrics],
        lab_id: str,
    ) -> SLAMetrics:

        lab_patients = [
            patient
            for patient in patient_metrics
            if patient.lab_id == lab_id
        ]

        patients_within_sla = sum(
            patient.queue_wait_minutes <= self.queue_sla_minutes
            for patient in lab_patients
        )

        patients_breaching_sla = (
            len(lab_patients) - patients_within_sla
        )

        sla_breach_rate = (
            patients_breaching_sla / len(lab_patients)
            if lab_patients
            else 0.0
        )

        return SLAMetrics(
            lab_id=lab_id,
            total_patients=len(lab_patients),
            patients_within_sla=patients_within_sla,
            patients_breaching_sla=patients_breaching_sla,
            sla_breach_rate=sla_breach_rate,
        )

    def calculate_window_metrics(
        self,
        patient_metrics: list[PatientMetrics],
        lab_id: str,
        start_time: datetime,
        end_time: datetime,
        window_minutes: int = 60,
        station_count: int = 4,
    ) -> list[WindowMetrics]:
        """Calculate operational metrics for fixed time windows."""

        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0")

        if station_count <= 0:
            raise ValueError("station_count must be greater than 0")

        lab_patients = [
            patient
            for patient in patient_metrics
            if patient.lab_id == lab_id
        ]

        results = []

        window_start = start_time

        while window_start < end_time:
            window_end = min(
                window_start + timedelta(minutes=window_minutes),
                end_time,
            )

            # ---------------------------------------------------------
            # ARRIVALS
            # ---------------------------------------------------------
            arrivals = [
                patient
                for patient in lab_patients
                if window_start <= patient.arrival_time < window_end
            ]

            # ---------------------------------------------------------
            # SERVICE COMPLETIONS
            # ---------------------------------------------------------
            served = [
                patient
                for patient in lab_patients
                if window_start <= patient.service_end_time < window_end
            ]

            # ---------------------------------------------------------
            # QUEUE WAIT
            # ---------------------------------------------------------
            if arrivals:
                average_queue_wait = sum(
                    patient.queue_wait_minutes
                    for patient in arrivals
                ) / len(arrivals)

                maximum_queue_wait = max(
                    patient.queue_wait_minutes
                    for patient in arrivals
                )

                sla_breaches = sum(
                    patient.queue_wait_minutes > self.queue_sla_minutes
                    for patient in arrivals
                )

                sla_breach_rate = (
                    sla_breaches / len(arrivals)
                )
            else:
                average_queue_wait = 0.0
                maximum_queue_wait = 0.0
                sla_breach_rate = 0.0

            # ---------------------------------------------------------
            # SERVICE TIME + UTILIZATION
            # ---------------------------------------------------------
            total_service_minutes = 0.0

            for patient in lab_patients:

                service_start = patient.service_start_time
                service_end = patient.service_end_time

                # No overlap with this window.
                if service_end <= window_start:
                    continue

                if service_start >= window_end:
                    continue

                # Clip service interval to window.
                overlap_start = max(
                    service_start,
                    window_start,
                )

                overlap_end = min(
                    service_end,
                    window_end,
                )

                overlap_minutes = (
                    overlap_end - overlap_start
                ).total_seconds() / 60

                total_service_minutes += overlap_minutes

            available_staff_minutes = (
                window_minutes * station_count
            )

            utilization = (
                total_service_minutes / available_staff_minutes
                if available_staff_minutes > 0
                else 0.0
            )

            # ---------------------------------------------------------
            # AVERAGE SERVICE TIME
            # ---------------------------------------------------------
            if served:
                average_service_time = sum(
                    patient.service_duration_minutes
                    for patient in served
                ) / len(served)
            else:
                average_service_time = 0.0

            results.append(
                WindowMetrics(
                    lab_id=lab_id,
                    window_start=window_start,
                    window_end=window_end,
                    arrivals=len(arrivals),
                    patients_served=len(served),
                    average_queue_wait_minutes=average_queue_wait,
                    maximum_queue_wait_minutes=maximum_queue_wait,
                    average_service_time_minutes=average_service_time,
                    utilization=utilization,
                    sla_breach_rate=sla_breach_rate,
                )
            )

            window_start = window_end

        return results
