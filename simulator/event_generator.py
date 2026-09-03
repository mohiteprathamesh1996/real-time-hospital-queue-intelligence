from datetime import datetime
from uuid import uuid4
from datetime import datetime, timedelta
from numpy.random import Generator, default_rng

from config.settings import AppConfig
from simulator.event_models import (
    EventType,
    HospitalEvent,
    PatientType,
    Priority,
)


class HospitalEventGenerator:
    """Generate synthetic hospital operational events."""

    def __init__(self, config: AppConfig, seed: int = 42):
        self.config = config
        self.rng: Generator = default_rng(seed)

    def generate_patient_arrival(
        self,
        patient_type: PatientType,
        priority: Priority,
        lab_id: str,
        event_time: datetime,
    ) -> HospitalEvent:
        """Generate a single patient-arrival event."""

        return HospitalEvent(
            event_id=str(uuid4()),
            event_type=EventType.PATIENT_ARRIVAL,
            patient_id=f"PAT_{uuid4().hex[:8]}",
            lab_id=lab_id,
            event_time=event_time,
            ingestion_time=event_time,
            patient_type=patient_type,
            priority=priority,
        )

    def generate_patient_lifecycle(
        self,
        patient_type: PatientType,
        priority: Priority,
        lab_id: str,
        arrival_time: datetime,
    ) -> list[HospitalEvent]:
        """Generate the initial lifecycle events for one patient."""

        patient_id = f"PAT_{uuid4().hex[:8]}"
        appointment_id = f"APT_{uuid4().hex[:8]}"

        registration_duration = self.rng.uniform(1, 3)

        registration_time = arrival_time.replace(
            microsecond=0
        )

        queue_time = registration_time

        events = [
            HospitalEvent(
                event_id=str(uuid4()),
                event_type=EventType.PATIENT_ARRIVAL,
                patient_id=patient_id,
                lab_id=lab_id,
                event_time=arrival_time,
                ingestion_time=arrival_time,
                patient_type=patient_type,
                priority=priority,
                appointment_id=appointment_id,
            ),
            HospitalEvent(
                event_id=str(uuid4()),
                event_type=EventType.REGISTRATION_COMPLETED,
                patient_id=patient_id,
                lab_id=lab_id,
                event_time=registration_time
                + timedelta(minutes=registration_duration),
                ingestion_time=registration_time
                + timedelta(minutes=registration_duration),
                patient_type=patient_type,
                priority=priority,
                appointment_id=appointment_id,
            ),
            HospitalEvent(
                event_id=str(uuid4()),
                event_type=EventType.QUEUE_ENTERED,
                patient_id=patient_id,
                lab_id=lab_id,
                event_time=queue_time
                + timedelta(minutes=registration_duration),
                ingestion_time=queue_time
                + timedelta(minutes=registration_duration),
                patient_type=patient_type,
                priority=priority,
                appointment_id=appointment_id,
            ),
        ]

        return events

