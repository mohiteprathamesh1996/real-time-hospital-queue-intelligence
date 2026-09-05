from dataclasses import dataclass
import random


@dataclass(frozen=True)
class WeightedDemandProfile:
    arrival_rate_per_minute: float
    patient_records: tuple
    record_weights: tuple

    older_count: int
    recent_count: int


class WeightedScenarioForecaster:
    """
    Generates stochastic future patient arrivals from the
    weighted-recent forecasting strategy.

    Recent history receives more weight than older history,
    while patient type and service duration are sampled
    together to preserve their empirical relationship.
    """

    def __init__(
        self,
        older_weight: float = 1.0,
        recent_weight: float = 2.0,
    ):
        if older_weight <= 0:
            raise ValueError(
                "older_weight must be positive"
            )

        if recent_weight <= 0:
            raise ValueError(
                "recent_weight must be positive"
            )

        self.older_weight = older_weight
        self.recent_weight = recent_weight

    def fit(
        self,
        older_records,
        recent_records,
        segment_minutes: float,
    ) -> WeightedDemandProfile:

        if segment_minutes <= 0:
            raise ValueError(
                "segment_minutes must be greater than zero"
            )

        older_records = list(older_records)
        recent_records = list(recent_records)

        older_rate = (
            len(older_records)
            / segment_minutes
        )

        recent_rate = (
            len(recent_records)
            / segment_minutes
        )

        weighted_rate = (
            (
                older_rate
                * self.older_weight
            )
            +
            (
                recent_rate
                * self.recent_weight
            )
        ) / (
            self.older_weight
            + self.recent_weight
        )

        records = (
            older_records
            + recent_records
        )

        weights = (
            [
                self.older_weight
                for _ in older_records
            ]
            +
            [
                self.recent_weight
                for _ in recent_records
            ]
        )

        return WeightedDemandProfile(
            arrival_rate_per_minute=weighted_rate,
            patient_records=tuple(records),
            record_weights=tuple(weights),
            older_count=len(older_records),
            recent_count=len(recent_records),
        )

    def generate_scenario(
        self,
        profile: WeightedDemandProfile,
        horizon_minutes: float,
        seed: int,
    ):
        if horizon_minutes <= 0:
            raise ValueError(
                "horizon_minutes must be greater than zero"
            )

        if (
            profile.arrival_rate_per_minute <= 0
            or not profile.patient_records
        ):
            return [], []

        rng = random.Random(seed)

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

            record = rng.choices(
                population=profile.patient_records,
                weights=profile.record_weights,
                k=1,
            )[0]

            patient_type, service_duration = (
                record
            )

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
