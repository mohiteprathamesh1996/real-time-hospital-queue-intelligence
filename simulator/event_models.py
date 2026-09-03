from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class EventType(str, Enum):
    APPOINTMENT_CREATED = "APPOINTMENT_CREATED"
    PATIENT_ARRIVAL = "PATIENT_ARRIVAL"
    REGISTRATION_COMPLETED = "REGISTRATION_COMPLETED"
    QUEUE_ENTERED = "QUEUE_ENTERED"
    STAFF_ASSIGNED = "STAFF_ASSIGNED"
    SERVICE_STARTED = "SERVICE_STARTED"
    SERVICE_COMPLETED = "SERVICE_COMPLETED"
    PATIENT_DEPARTED = "PATIENT_DEPARTED"
    APPOINTMENT_CANCELLED = "APPOINTMENT_CANCELLED"
    PATIENT_NO_SHOW = "PATIENT_NO_SHOW"


class PatientType(str, Enum):
    OUTPATIENT = "OUTPATIENT"
    INPATIENT = "INPATIENT"
    WALK_IN = "WALK_IN"
    FOLLOW_UP = "FOLLOW_UP"


class Priority(str, Enum):
    NORMAL = "NORMAL"
    URGENT = "URGENT"
    PEDIATRIC = "PEDIATRIC"
    ELDERLY = "ELDERLY"


class HospitalEvent(BaseModel):
    """
    Canonical event contract for hospital operational events.
    """

    event_id: str = Field(min_length=1)
    event_type: EventType

    patient_id: str = Field(min_length=1)
    lab_id: str = Field(min_length=1)

    event_time: datetime
    ingestion_time: datetime

    source: str = "hospital_simulator"
    schema_version: int = 1

    patient_type: PatientType
    priority: Priority

    appointment_id: str | None = None
    staff_id: str | None = None