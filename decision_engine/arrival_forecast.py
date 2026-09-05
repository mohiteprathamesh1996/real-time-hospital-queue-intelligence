from dataclasses import dataclass
import random


@dataclass(frozen=True)
class DemandProfile:
    arrival_rate_per_minute: float
    patient_types: tuple
    service_durations: tuple


class RecentHistoryForecaster:
    """
    Simple forecasting baseline.

    Estimates near-term arrival intensity from a recent
    historical lookback window and generates stochastic
    future scenarios using exponential interarrival times.

    This is deliberately simple and interpretable.
    It establishes a baseline before introducing ML.
    """

    def fit(
        self,
        patient_types,
        service_durations,
        lookback_minutes: float,
    ) -> DemandProfile:

        if lookback_minutes <= 0:
            raise ValueError(
                "lookback_minutes must be greater than zero"
            )

        if len(patient_types) != len(service_durations):
            raise ValueError(
                "patient_types and service_durations "
                "must have the same length"
            )

        arrival_rate = (
            len(patient_types)
            / lookback_minutes
        )

        return DemandProfile(
            arrival_rate_per_minute=arrival_rate,
            patient_types=tuple(patient_types),
            service_durations=tuple(
                float(value)
                for value in service_durations
            ),
        )

    def generate_scenario(
        self,
        profile: DemandProfile,
        horizon_minutes: float,
        seed: int,
    ):
        if horizon_minutes <= 0:
            raise ValueError(
                "horizon_minutes must be greater than zero"
            )

        rng = random.Random(seed)

        if (
            profile.arrival_rate_per_minute <= 0
            or not profile.patient_types
        ):
            return [], []

        arrivals = []
        service_durations = []

        current_time = 0.0

        while True:
            interarrival = rng.expovariate(
                profile.arrival_rate_per_minute
            )

            current_time += interarrival

            if current_time >= horizon_minutes:
                break

            patient_type = rng.choice(
                profile.patient_types
            )

            if profile.service_durations:
                service_duration = rng.choice(
                    profile.service_durations
                )
            else:
                service_duration = 5.0

            arrivals.append(
                (
                    current_time,
                    patient_type,
                )
            )

            service_durations.append(
                float(service_duration)
            )

        return (
            arrivals,
            service_durations,
        )
