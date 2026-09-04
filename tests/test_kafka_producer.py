from datetime import datetime

from config.settings import load_config
from simulator.event_models import (
    EventType,
    HospitalEvent,
    PatientType,
    Priority,
)
from streaming.producer import HospitalEventProducer


def main():

    config = load_config(
        "config/hospital.yaml"
    )

    producer = HospitalEventProducer()

    event = HospitalEvent(
        event_id="TEST_EVENT_000001",
        event_type=EventType.PATIENT_ARRIVAL,
        patient_id="TEST_PATIENT_000001",
        lab_id="LAB_A",
        event_time=datetime.fromisoformat(
            config.simulation.start_time
        ),
        ingestion_time=datetime.now().astimezone(),
        patient_type=PatientType.OUTPATIENT,
        priority=Priority.NORMAL,
    )

    producer.publish(event)

    producer.flush()

    print("Event published successfully.")


if __name__ == "__main__":
    main()