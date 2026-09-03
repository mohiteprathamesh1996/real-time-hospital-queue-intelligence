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

    def next_arrival_time(self, current_time: datetime) -> datetime:
        """Generate the timestamp of the next arrival."""

        rate = self.get_rate_per_hour(current_time)

        interarrival_hours = self.rng.exponential(
            scale=1 / rate
        )

        return current_time + timedelta(
            hours=interarrival_hours
        )

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
        """Generate arrivals and assign each one a patient type."""

        if not patient_type_rates:
            raise ValueError("patient_type_rates must not be empty")

        total_rate = sum(patient_type_rates.values())

        if total_rate <= 0:
            raise ValueError("Total patient arrival rate must be greater than 0")

        arrivals: list[tuple[datetime, PatientType]] = []

        current_time = start_time

        while True:
            interarrival_hours = self.rng.exponential(
                scale=1 / total_rate
            )

            current_time = current_time + timedelta(
                hours=interarrival_hours
            )

            if current_time > end_time:
                break

            patient_types = list(patient_type_rates.keys())
            rates = list(patient_type_rates.values())

            selected_index = self.rng.choice(
                len(patient_types),
                p=[rate / total_rate for rate in rates],
            )

            patient_type = patient_types[selected_index]

            arrivals.append(
                (current_time, patient_type)
            )

        return arrivals


