from dataclasses import dataclass

import simpy

from simulator.event_models import PatientType


@dataclass
class SimPyPatientResult:
    """Result of one patient's journey through the SimPy system."""

    patient_id: str
    patient_type: PatientType

    arrival_time: float
    service_start_time: float
    service_end_time: float
    departure_time: float

    queue_wait_minutes: float
    service_duration_minutes: float


class SimPyQueueEngine:
    """
    Discrete-event hospital queue simulation using SimPy.

    The simulation clock is measured in minutes from the beginning
    of the simulation.
    """

    def __init__(
        self,
        station_count: int,
        service_time_generator,
    ):
        if station_count < 1:
            raise ValueError(
                "station_count must be at least 1"
            )

        self.station_count = station_count
        self.service_time_generator = service_time_generator

    def run_patient(
        self,
        env,
        resource,
        patient_id,
        patient_type,
        arrival_time,
        service_duration=None,
    ):
        yield env.timeout(
            max(0, arrival_time - env.now)
        )

        actual_arrival_time = env.now

        if service_duration is None:
            service_duration = (
                self.service_time_generator
                .generate_service_time(patient_type)
            )

        with resource.request() as request:
            yield request

            service_start_time = env.now

            yield env.timeout(service_duration)

            service_end_time = env.now

        departure_time = service_end_time

        result = SimPyPatientResult(
            patient_id=patient_id,
            patient_type=patient_type,
            arrival_time=actual_arrival_time,
            service_start_time=service_start_time,
            service_end_time=service_end_time,
            departure_time=departure_time,
            queue_wait_minutes=(
                service_start_time - actual_arrival_time
            ),
            service_duration_minutes=service_duration,
        )

        self.results.append(result)

    def run(
        self,
        arrivals: list[tuple[float, PatientType]],
        service_durations: list[float] | None = None,
    ):
        self.results = []

        if service_durations is not None:
            if len(service_durations) != len(arrivals):
                raise ValueError(
                    "service_durations must have the same length as arrivals"
                )

        env = simpy.Environment()
        resource = simpy.Resource(
            env,
            capacity=self.station_count,
        )

        for index, (arrival_time, patient_type) in enumerate(
            arrivals,
            start=1,
        ):
            service_duration = (
                service_durations[index - 1]
                if service_durations is not None
                else None
            )

            env.process(
                self.run_patient(
                    env,
                    resource,
                    f"SIM_PAT_{index:06d}",
                    patient_type,
                    arrival_time,
                    service_duration,
                )
            )

        env.run()

        return sorted(
            self.results,
            key=lambda result: result.arrival_time,
        )
    