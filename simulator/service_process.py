from numpy.random import Generator, default_rng

from config.settings import AppConfig
from simulator.event_models import PatientType
from simulator.event_models import PatientType, Priority


class ServiceTimeGenerator:
    """Generate patient service durations from configured distributions."""

    def __init__(
        self,
        config: AppConfig,
        seed: int | None = None,
    ):
        self.config = config
        self.rng: Generator = default_rng(seed)

    def generate_service_time(
        self,
        patient_type: PatientType,
    ) -> float:
        """Generate a service duration in minutes."""

        profile = self.config.patient_profiles[patient_type.value.lower()]

        duration = self.rng.normal(
            loc=profile.service_time_mean_minutes,
            scale=profile.service_time_std_minutes,
        )

        # Service duration cannot be zero or negative.
        return max(0.5, float(duration))

    def generate_priority(self) -> Priority:
        """Generate a patient priority using configured probabilities."""

        priorities = list(Priority)

        probabilities = [
            self.config.priorities[
                priority.value.lower()
            ].probability
            for priority in priorities
        ]

        selected_index = self.rng.choice(
            len(priorities),
            p=probabilities,
        )

        return priorities[selected_index]

    