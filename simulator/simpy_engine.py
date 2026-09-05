from dataclasses import dataclass

import simpy

from simulator.event_models import PatientType


@dataclass
class InitialService:
    remaining_service_minutes: float


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

            yield env.timeout(
                service_duration
            )

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

    def occupy_station(
        self,
        env,
        resource,
        remaining_service_minutes,
    ):
        """
        Occupy one station for a patient who was already
        in service when the counterfactual simulation began.
        """

        with resource.request() as request:
            yield request

            yield env.timeout(
                remaining_service_minutes
            )

    def run(
        self,
        arrivals: list[tuple[float, PatientType]],
        service_durations: list[float] | None = None,
        initial_waiting: list[
            tuple[PatientType, float]
        ] | None = None,
        initial_services: list[InitialService] | None = None,
    ):
        self.results = []

        initial_waiting = (
            initial_waiting or []
        )

        initial_services = (
            initial_services or []
        )

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

        # ----------------------------------------------------------
        # Patients already in service at t = 0
        # ----------------------------------------------------------

        for initial_service in initial_services:
            env.process(
                self.occupy_station(
                    env=env,
                    resource=resource,
                    remaining_service_minutes=(
                        initial_service.remaining_service_minutes
                    ),
                )
            )

        # ----------------------------------------------------------
        # Patients already waiting at t = 0
        # ----------------------------------------------------------

        for index, (
            patient_type,
            service_duration,
        ) in enumerate(
            initial_waiting,
            start=1,
        ):
            env.process(
                self.run_patient(
                    env=env,
                    resource=resource,
                    patient_id=(
                        f"INITIAL_WAIT_{index:06d}"
                    ),
                    patient_type=patient_type,
                    arrival_time=0.0,
                    service_duration=service_duration,
                )
            )

        # ----------------------------------------------------------
        # Future arrivals
        # ----------------------------------------------------------

        for index, (
            arrival_time,
            patient_type,
        ) in enumerate(
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
                    env=env,
                    resource=resource,
                    patient_id=(
                        f"SIM_PAT_{index:06d}"
                    ),
                    patient_type=patient_type,
                    arrival_time=arrival_time,
                    service_duration=service_duration,
                )
            )

        env.run()

        return sorted(
            self.results,
            key=lambda result: (
                result.arrival_time,
                result.patient_id,
            ),
        )