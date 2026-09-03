from datetime import datetime, timedelta
from config.settings import AppConfig
from numpy.random import Generator, default_rng
from simulator.event_models import PatientType


class PoissonArrivalProcess:
    """Generate arrivals according to a time-varying Poisson process."""

    def __init__(
        self,
        rate_per_hour: float,
        seed: int | None = None,
        rate_schedule: list[tuple[int, int, float]] | None = None,
    ):
        if rate_per_hour <= 0:
            raise ValueError("rate_per_hour must be greater than 0")

        self.rate_per_hour = rate_per_hour
        self.rng: Generator = default_rng(seed)
        self.rate_schedule = rate_schedule or []

    @classmethod
    def from_config(
        cls,
        config: AppConfig,
        seed: int | None = None,
    ) -> "PoissonArrivalProcess":
        """Create an arrival process from validated hospital configuration."""

        schedule = [
            (
                item.start_hour,
                item.end_hour,
                item.rate_per_hour,
            )
            for item in config.simulation.arrival_rate_schedule
        ]

        return cls(
            rate_per_hour=config.simulation.default_arrival_rate_per_hour,
            seed=seed,
            rate_schedule=schedule,
        )

    def get_rate_per_hour(self, current_time: datetime) -> float:
        """Return the configured arrival rate for the current hour."""

        hour = current_time.hour

        for start_hour, end_hour, rate in self.rate_schedule:
            if start_hour <= hour < end_hour:
                return rate

        return self.rate_per_hour

    def get_next_rate_boundary(
        self,
        current_time: datetime,
    ) -> datetime | None:
        """Return the next configured arrival-rate boundary."""

        boundaries = []

        for start_hour, end_hour, _ in self.rate_schedule:
            for hour in (start_hour, end_hour):
                boundary = current_time.replace(
                    hour=hour,
                    minute=0,
                    second=0,
                    microsecond=0,
                )

                if boundary <= current_time:
                    boundary += timedelta(days=1)

                boundaries.append(boundary)

        return min(boundaries) if boundaries else None

    def next_arrival_time(self, current_time: datetime) -> datetime:
        """Generate the next arrival using the time-varying arrival rate."""

        while True:
            rate = self.get_rate_per_hour(current_time)

            interarrival_hours = self.rng.exponential(
                scale=1 / rate
            )

            candidate_time = current_time + timedelta(
                hours=interarrival_hours
            )

            next_boundary = self.get_next_rate_boundary(current_time)

            if next_boundary is None:
                return candidate_time

            if candidate_time < next_boundary:
                return candidate_time

            current_time = next_boundary

    def generate_arrivals(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[datetime]:
        """Generate arrival timestamps within a simulation window."""

        if end_time <= start_time:
            raise ValueError("end_time must be after start_time")

        arrivals: list[datetime] = []
        current_time = start_time

        while True:
            current_time = self.next_arrival_time(current_time)

            if current_time > end_time:
                break

            arrivals.append(current_time)

        return arrivals

    def generate_patient_arrivals(
        self,
        start_time: datetime,
        end_time: datetime,
        patient_type_rates: dict[PatientType, float],
    ) -> list[tuple[datetime, PatientType]]:
        """Generate arrivals using time-varying total demand and patient mix."""

        if not patient_type_rates:
            raise ValueError("patient_type_rates must not be empty")

        total_mix_weight = sum(patient_type_rates.values())

        if total_mix_weight <= 0:
            raise ValueError(
                "Total patient-type weight must be greater than 0"
            )

        patient_types = list(patient_type_rates.keys())

        probabilities = [
            patient_type_rates[patient_type] / total_mix_weight
            for patient_type in patient_types
        ]

        arrivals: list[tuple[datetime, PatientType]] = []

        current_time = start_time

        while current_time < end_time:
            rate = self.get_rate_per_hour(current_time)

            interarrival_hours = self.rng.exponential(
                scale=1 / rate
            )

            candidate_time = current_time + timedelta(
                hours=interarrival_hours
            )

            next_boundary = self.get_next_rate_boundary(
                current_time
            )

            if next_boundary is not None and candidate_time >= next_boundary:
                current_time = next_boundary
                continue

            if candidate_time > end_time:
                break

            current_time = candidate_time

            selected_index = self.rng.choice(
                len(patient_types),
                p=probabilities,
            )

            patient_type = patient_types[selected_index]

            arrivals.append(
                (current_time, patient_type)
            )

        return arrivals

    def get_patient_type_weights(
        self,
        config: AppConfig,
    ) -> dict[PatientType, float]:
        """Return patient-type weights from hospital configuration."""

        return {
            PatientType.OUTPATIENT: config.patient_profiles["outpatient"].arrival_weight,
            PatientType.INPATIENT: config.patient_profiles["inpatient"].arrival_weight,
            PatientType.WALK_IN: config.patient_profiles["walk_in"].arrival_weight,
            PatientType.FOLLOW_UP: config.patient_profiles["follow_up"].arrival_weight,
        }

