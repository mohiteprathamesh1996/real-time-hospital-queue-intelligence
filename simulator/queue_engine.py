from dataclasses import dataclass
from datetime import datetime, timedelta
from heapq import heappop, heappush
from simulator.event_models import PatientType, Priority

from uuid import uuid4

from simulator.event_models import (
    EventType,
    HospitalEvent,
    PatientType,
    Priority,
)

@dataclass
class QueuePatient:
    patient_id: str
    queue_time: datetime
    service_duration_minutes: float
    lab_id: str
    patient_type: PatientType
    priority: Priority


@dataclass
class ServiceAssignment:
    patient_id: str
    station_id: str
    service_start: datetime
    service_end: datetime
    queue_time: datetime
    lab_id: str
    patient_type: PatientType
    priority: Priority


class QueueEngine:
    """Simulate a multi-station service queue."""

    def __init__(self, station_count: int):
        if station_count < 1:
            raise ValueError("station_count must be at least 1")

        self.station_count = station_count

    def assign_patients(
            
        self,
        patients: list[QueuePatient],
    ) -> list[ServiceAssignment]:
        """
        Assign patients to the earliest available station.

        Patients are processed in queue arrival order.
        """

        if not patients:
            return []

        initial_time = min(
            patient.queue_time
            for patient in patients
        )

        available_stations = [
            (initial_time, f"STAFF_{i:02d}")
            for i in range(1, self.station_count + 1)
        ]

        assignments: list[ServiceAssignment] = []

        for patient in sorted(patients, key=lambda p: p.queue_time):
            available_time, station_id = heappop(available_stations)

            service_start = max(
                patient.queue_time,
                available_time,
            )

            service_end = service_start + timedelta(
                minutes=patient.service_duration_minutes
            )

            assignment = ServiceAssignment(
                patient_id=patient.patient_id,
                station_id=station_id,
                service_start=service_start,
                service_end=service_end,
                queue_time=patient.queue_time,
                lab_id=patient.lab_id,
                patient_type=patient.patient_type,
                priority=patient.priority,
            )

            assignments.append(assignment)

            heappush(
                available_stations,
                (service_end, station_id),
            )

        return assignments

    def generate_service_events(
        self,
        assignment: ServiceAssignment,
    ) -> list[HospitalEvent]:
        """Convert a service assignment into hospital lifecycle events."""

        events = [
            HospitalEvent(
                event_id=str(uuid4()),
                event_type=EventType.STAFF_ASSIGNED,
                patient_id=assignment.patient_id,
                lab_id=assignment.lab_id,
                event_time=assignment.service_start,
                ingestion_time=assignment.service_start,
                patient_type=assignment.patient_type,
                priority=assignment.priority,
                staff_id=assignment.station_id,
            ),
            HospitalEvent(
                event_id=str(uuid4()),
                event_type=EventType.SERVICE_STARTED,
                patient_id=assignment.patient_id,
                lab_id=assignment.lab_id,
                event_time=assignment.service_start,
                ingestion_time=assignment.service_start,
                patient_type=assignment.patient_type,
                priority=assignment.priority,
                staff_id=assignment.station_id,
            ),
            HospitalEvent(
                event_id=str(uuid4()),
                event_type=EventType.SERVICE_COMPLETED,
                patient_id=assignment.patient_id,
                lab_id=assignment.lab_id,
                event_time=assignment.service_end,
                ingestion_time=assignment.service_end,
                patient_type=assignment.patient_type,
                priority=assignment.priority,
                staff_id=assignment.station_id,
            ),
            HospitalEvent(
                event_id=str(uuid4()),
                event_type=EventType.PATIENT_DEPARTED,
                patient_id=assignment.patient_id,
                lab_id=assignment.lab_id,
                event_time=assignment.service_end,
                ingestion_time=assignment.service_end,
                patient_type=assignment.patient_type,
                priority=assignment.priority,
                staff_id=assignment.station_id,
            ),
        ]

        return events