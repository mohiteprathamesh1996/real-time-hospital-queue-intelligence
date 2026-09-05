from dataclasses import dataclass

import simpy

from simulator.event_models import PatientType


@dataclass(frozen=True)
class InitialService:
    """Represents a patient already in service at simulation start."""

    remaining_service_minutes: float


@dataclass(frozen=True)
class InitialWaitingPatient:
    """Represents a patient already waiting when simulation starts."""

    patient_type: PatientType
    service_duration_minutes: float
    accrued_wait_minutes: float


@dataclass
class SimPyPatientResult:
    """Result for one simulated patient."""

    patient_id: str
    patient_type: PatientType

    arrival_time: float
    service_start_time: float
    service_end_time: float
    departure_time: float

    queue_wait_minutes: float
    service_duration_minutes: float


class SimPyQueueEngine:
    """SimPy-based multi-server queue simulation."""

    def __init__(
        self,
        station_count: int,
        service_time_generator,
    ):
        if station_count <= 0:
            raise ValueError(
                "station_count must be greater than zero"
            )

        self.station_count = station_count
        self.service_time_generator = service_time_generator

    def _occupy_station(
        self,
        env: simpy.Environment,
        resource: simpy.Resource,
        remaining_service_minutes: float,
    ):
        """Occupy a station for a patient already in service."""

        with resource.request() as request:
            yield request

            yield env.timeout(
                remaining_service_minutes
            )

    def _run_patient(
        self,
        env: simpy.Environment,
        resource: simpy.Resource,
        results: list[SimPyPatientResult],
        patient_id: str,
        patient_type: PatientType,
        arrival_time: float,
        service_duration: float,
        accrued_wait_minutes: float = 0.0,
    ):
        """Run one patient through the queue and service process."""

        # Wait until this patient's arrival time.
        if arrival_time > env.now:
            yield env.timeout(
                arrival_time - env.now
            )

        queue_entry_time = env.now

        # Request a service station.
        with resource.request() as request:
            yield request

            service_start_time = env.now

            # Waiting incurred during this counterfactual simulation.
            new_wait_minutes = (
                service_start_time
                - queue_entry_time
            )

            # For patients already waiting before t=0, preserve
            # the waiting time they had already accumulated.
            queue_wait_minutes = (
                accrued_wait_minutes
                + new_wait_minutes
            )

            yield env.timeout(
                service_duration
            )

            service_end_time = env.now
            departure_time = service_end_time

            results.append(
                SimPyPatientResult(
                    patient_id=patient_id,
                    patient_type=patient_type,
                    arrival_time=arrival_time,
                    service_start_time=service_start_time,
                    service_end_time=service_end_time,
                    departure_time=departure_time,
                    queue_wait_minutes=queue_wait_minutes,
                    service_duration_minutes=service_duration,
                )
            )

    def run(
        self,
        arrivals,
        service_durations=None,
        initial_waiting=None,
        initial_services=None,
    ) -> list[SimPyPatientResult]:
        """Run the queue simulation.

        Parameters
        ----------
        arrivals:
            Sequence of tuples:

                (arrival_time_minutes, patient_type)

        service_durations:
            Optional predetermined service durations corresponding to
            arrivals. Supplying these keeps staffing experiments
            deterministic.

        initial_waiting:
            Optional list of InitialWaitingPatient objects representing
            patients already in the queue when the simulation begins.

        initial_services:
            Optional list of InitialService objects representing patients
            already occupying stations when the simulation begins.
        """

        initial_waiting = (
            initial_waiting
            if initial_waiting is not None
            else []
        )

        initial_services = (
            initial_services
            if initial_services is not None
            else []
        )

        if len(initial_services) > self.station_count:
            raise ValueError(
                "initial_services cannot exceed station_count"
            )

        if (
            service_durations is not None
            and len(service_durations) != len(arrivals)
        ):
            raise ValueError(
                "service_durations must have the same length as arrivals"
            )

        env = simpy.Environment()

        resource = simpy.Resource(
            env,
            capacity=self.station_count,
        )

        results: list[SimPyPatientResult] = []

        # ------------------------------------------------------------
        # Patients already occupying service stations
        # ------------------------------------------------------------

        for initial_service in initial_services:
            env.process(
                self._occupy_station(
                    env=env,
                    resource=resource,
                    remaining_service_minutes=(
                        initial_service.remaining_service_minutes
                    ),
                )
            )

        # ------------------------------------------------------------
        # Patients already waiting at simulation start
        # ------------------------------------------------------------

        for index, patient in enumerate(
            initial_waiting,
            start=1,
        ):
            env.process(
                self._run_patient(
                    env=env,
                    resource=resource,
                    results=results,
                    patient_id=(
                        f"INITIAL_WAIT_{index:06d}"
                    ),
                    patient_type=patient.patient_type,
                    arrival_time=0.0,
                    service_duration=(
                        patient.service_duration_minutes
                    ),
                    accrued_wait_minutes=(
                        patient.accrued_wait_minutes
                    ),
                )
            )

        # ------------------------------------------------------------
        # Future arrivals
        # ------------------------------------------------------------

        arrival_records = []

        for index, (
            arrival_time,
            patient_type,
        ) in enumerate(
            arrivals,
            start=1,
        ):
            if service_durations is None:
                service_duration = (
                    self.service_time_generator
                    .generate_service_time(
                        patient_type
                    )
                )
            else:
                service_duration = (
                    service_durations[
                        index - 1
                    ]
                )

            arrival_records.append(
                (
                    index,
                    float(arrival_time),
                    patient_type,
                    float(service_duration),
                )
            )

        # Schedule chronologically while preserving the original
        # patient IDs and predetermined service-duration mapping.
        arrival_records.sort(
            key=lambda row: row[1]
        )

        for (
            original_index,
            arrival_time,
            patient_type,
            service_duration,
        ) in arrival_records:

            env.process(
                self._run_patient(
                    env=env,
                    resource=resource,
                    results=results,
                    patient_id=(
                        f"SIM_PAT_{original_index:06d}"
                    ),
                    patient_type=patient_type,
                    arrival_time=arrival_time,
                    service_duration=service_duration,
                    accrued_wait_minutes=0.0,
                )
            )

        # ------------------------------------------------------------
        # Run simulation
        # ------------------------------------------------------------

        env.run()

        # Keep output deterministic and chronological.
        results.sort(
            key=lambda result: (
                result.arrival_time,
                result.patient_id,
            )
        )

        return results