from dataclasses import dataclass
from datetime import datetime

from simulator.arrival_process import PoissonArrivalProcess
from simulator.event_models import PatientType


@dataclass
class CapacityScenario:
    name: str
    demand_multiplier: float
    arrivals: list[tuple[float, PatientType]]


class CapacityScenarioGenerator:

    def __init__(
        self,
        config,
        start_time: datetime,
        end_time: datetime,
        seed: int = 42,
    ):
        self.config = config
        self.start_time = start_time
        self.end_time = end_time
        self.seed = seed

    def generate(
        self,
        name: str,
        demand_multiplier: float,
    ) -> CapacityScenario:

        if demand_multiplier <= 0:
            raise ValueError(
                "demand_multiplier must be greater than 0"
            )

        base_process = PoissonArrivalProcess.from_config(
            self.config,
            seed=self.seed,
        )

        scaled_schedule = [
            (
                start_hour,
                end_hour,
                rate * demand_multiplier,
            )
            for start_hour, end_hour, rate
            in base_process.rate_schedule
        ]

        scaled_process = PoissonArrivalProcess(
            rate_per_hour=(
                base_process.rate_per_hour
                * demand_multiplier
            ),
            seed=self.seed,
            rate_schedule=scaled_schedule,
        )

        patient_type_weights = {
            PatientType.OUTPATIENT:
                self.config.patient_profiles[
                    "outpatient"
                ].arrival_weight,

            PatientType.INPATIENT:
                self.config.patient_profiles[
                    "inpatient"
                ].arrival_weight,

            PatientType.WALK_IN:
                self.config.patient_profiles[
                    "walk_in"
                ].arrival_weight,

            PatientType.FOLLOW_UP:
                self.config.patient_profiles[
                    "follow_up"
                ].arrival_weight,
        }

        arrivals_datetime = (
            scaled_process.generate_patient_arrivals(
                start_time=self.start_time,
                end_time=self.end_time,
                patient_type_rates=patient_type_weights,
            )
        )

        arrivals = [
            (
                (
                    arrival_time - self.start_time
                ).total_seconds() / 60,
                patient_type,
            )
            for arrival_time, patient_type
            in arrivals_datetime
        ]

        return CapacityScenario(
            name=name,
            demand_multiplier=demand_multiplier,
            arrivals=arrivals,
        )