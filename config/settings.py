from pathlib import Path

import yaml
from pydantic import BaseModel


class OperatingHours(BaseModel):
    start: str
    end: str


class LabConfig(BaseModel):
    lab_id: str
    name: str
    stations: int
    operating_hours: OperatingHours


class PatientProfile(BaseModel):
    arrival_weight: float
    service_time_mean_minutes: float
    service_time_std_minutes: float
    no_show_rate: float
    cancellation_rate: float


class PriorityConfig(BaseModel):
    probability: float


class FaultInjectionConfig(BaseModel):
    duplicate_rate: float
    late_event_rate: float
    malformed_event_rate: float
    out_of_order_rate: float

class ArrivalRateConfig(BaseModel):
    start_hour: int
    end_hour: int
    rate_per_hour: float

class SimulationConfig(BaseModel):
    start_time: str
    duration_hours: int
    arrival_rate_schedule: list[ArrivalRateConfig]
    fault_injection: FaultInjectionConfig
    default_arrival_rate_per_hour: float


class HospitalConfig(BaseModel):
    name: str
    timezone: str

class StaffingConfig(BaseModel):
    incremental_staff_hour_cost: float = 35.0

class AppConfig(BaseModel):
    hospital: HospitalConfig
    labs: list[LabConfig]
    patient_profiles: dict[str, PatientProfile]
    priorities: dict[str, PriorityConfig]
    simulation: SimulationConfig
    staffing: StaffingConfig


def load_config(path: str | Path) -> AppConfig:
    """Load and validate hospital configuration."""

    config_path = Path(path)

    with config_path.open("r", encoding="utf-8") as file:
        raw_config = yaml.safe_load(file)

    return AppConfig.model_validate(raw_config)