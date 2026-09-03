from datetime import datetime, timedelta

from config.settings import AppConfig
from simulator.arrival_process import PoissonArrivalProcess
from simulator.event_generator import HospitalEventGenerator
from simulator.event_models import HospitalEvent, PatientType
from simulator.queue_engine import QueueEngine
from simulator.service_process import ServiceTimeGenerator

from simulator.event_models import (
    EventType,
    HospitalEvent,
    PatientType,
    Priority,
)
from simulator.queue_engine import QueuePatient, QueueEngine

class HospitalSimulator:
    """End-to-end hospital operational event simulator."""

    def __init__(
        self,
        config: AppConfig,
        seed: int = 42,
    ):
        self.config = config
        self.seed = seed

        self.arrival_process = PoissonArrivalProcess.from_config(
            config,
            seed=seed,
        )

        self.service_generator = ServiceTimeGenerator(
            config,
            seed=seed + 1,
        )

        self.event_generator = HospitalEventGenerator(
            config,
            seed=seed + 2,
        )

    def get_simulation_window(self) -> tuple[datetime, datetime]:
        """Return the configured simulation start and end timestamps."""

        start_time = datetime.fromisoformat(
            self.config.simulation.start_time
        )

        end_time = start_time + timedelta(
            hours=self.config.simulation.duration_hours
        )

        return start_time, end_time

    def get_patient_type_weights(self) -> dict[PatientType, float]:
        """Return patient-type weights from configuration."""

        return {
            PatientType.OUTPATIENT:
                self.config.patient_profiles["outpatient"].arrival_weight,

            PatientType.INPATIENT:
                self.config.patient_profiles["inpatient"].arrival_weight,

            PatientType.WALK_IN:
                self.config.patient_profiles["walk_in"].arrival_weight,

            PatientType.FOLLOW_UP:
                self.config.patient_profiles["follow_up"].arrival_weight,
        }

    def create_queue_patients(
        self,
        arrivals: list[tuple[datetime, PatientType]],
        lab_id: str,
    ) -> list[QueuePatient]:
        """Convert generated arrivals into queue patients."""

        queue_engine = QueueEngine(
            station_count=next(
                lab.stations
                for lab in self.config.labs
                if lab.lab_id == lab_id
            ),
            service_time_generator=self.service_generator,
        )

        patients: list[QueuePatient] = []

        for index, (arrival_time, patient_type) in enumerate(arrivals):
            priority = self.service_generator.generate_priority()
            patient_id = f"PAT_{index + 1:06d}"

            registration_duration = 2.0

            queue_entry_time = arrival_time + timedelta(
                minutes=registration_duration
            )

            patient = queue_engine.create_queue_patient(
                patient_id=patient_id,
                arrival_time=arrival_time,
                queue_time=queue_entry_time,
                lab_id=lab_id,
                patient_type=patient_type,
                priority=priority,
            )

            patients.append(patient)

        return patients

    def run(self, lab_id: str = "LAB_A") -> list[HospitalEvent]:
        """Run the hospital simulation and return lifecycle events."""

        start_time, end_time = self.get_simulation_window()

        patient_type_weights = self.get_patient_type_weights()

        arrivals = self.arrival_process.generate_patient_arrivals(
            start_time=start_time,
            end_time=end_time,
            patient_type_rates=patient_type_weights,
        )

        queue_patients = self.create_queue_patients(
            arrivals=arrivals,
            lab_id=lab_id,
        )

        station_count = next(
            lab.stations
            for lab in self.config.labs
            if lab.lab_id == lab_id
        )

        queue_engine = QueueEngine(
            station_count=station_count,
        )

        assignments = queue_engine.assign_patients(queue_patients)

        events: list[HospitalEvent] = []

        for patient, assignment in zip(queue_patients, assignments):
            arrival_time = patient.arrival_time
            queue_entry_time = patient.queue_time
            registration_time = queue_entry_time

            registration_duration = 2.0

            registration_time = queue_entry_time - timedelta(
                minutes=registration_duration
            )

            arrival_time = registration_time - timedelta(
                minutes=registration_duration
            )

            arrival_event = HospitalEvent(
                event_id=f"{patient.patient_id}_ARRIVAL",
                event_type=EventType.PATIENT_ARRIVAL,
                patient_id=patient.patient_id,
                lab_id=patient.lab_id,
                event_time=arrival_time,
                ingestion_time=arrival_time,
                patient_type=patient.patient_type,
                priority=patient.priority,
            )

            registration_event = HospitalEvent(
                event_id=f"{patient.patient_id}_REGISTRATION",
                event_type=EventType.REGISTRATION_COMPLETED,
                patient_id=patient.patient_id,
                lab_id=patient.lab_id,
                event_time=registration_time,
                ingestion_time=registration_time,
                patient_type=patient.patient_type,
                priority=patient.priority,
            )

            queue_event = HospitalEvent(
                event_id=f"{patient.patient_id}_QUEUE",
                event_type=EventType.QUEUE_ENTERED,
                patient_id=patient.patient_id,
                lab_id=patient.lab_id,
                event_time=queue_entry_time,
                ingestion_time=queue_entry_time,
                patient_type=patient.patient_type,
                priority=patient.priority,
            )

            events.append(arrival_event)
            events.append(registration_event)
            events.append(queue_event)

            events.extend(
                queue_engine.generate_service_events(
                    assignment
                )
            )

            # departure_event = HospitalEvent(
            #     event_id=f"{patient.patient_id}_DEPARTURE",
            #     event_type=EventType.PATIENT_DEPARTED,
            #     patient_id=patient.patient_id,
            #     lab_id=patient.lab_id,
            #     event_time=assignment.service_end,
            #     ingestion_time=assignment.service_end,
            #     patient_type=patient.patient_type,
            #     priority=patient.priority,
            #     staff_id=assignment.station_id,
            # )

            # events.append(departure_event)

        return sorted(
            events,
            key=lambda event: event.event_time,
        )