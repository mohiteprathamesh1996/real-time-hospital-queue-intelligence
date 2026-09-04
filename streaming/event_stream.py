import time

from config.settings import load_config
from simulator.hospital_simulator import HospitalSimulator
from streaming.producer import HospitalEventProducer


CONFIG_PATH = "config/hospital.yaml"
LAB_ID = "LAB_A"

# How quickly events are published.
# 0.05 means approximately 20 events/second.
EVENT_DELAY_SECONDS = 0.05


def main():
    # ---------------------------------------------------------------
    # Load configuration
    # ---------------------------------------------------------------

    config = load_config(CONFIG_PATH)

    # ---------------------------------------------------------------
    # Create the existing hospital simulator
    # ---------------------------------------------------------------

    simulator = HospitalSimulator(
        config,
        seed=42,
    )

    # ---------------------------------------------------------------
    # Generate hospital events
    # ---------------------------------------------------------------

    events = simulator.run(
        lab_id=LAB_ID
    )

    print("=" * 80)
    print("HOSPITAL EVENT STREAM PRODUCER")
    print("=" * 80)

    print(f"Lab:             {LAB_ID}")
    print(f"Events generated: {len(events)}")
    print()

    # ---------------------------------------------------------------
    # Create Kafka producer
    # ---------------------------------------------------------------

    producer = HospitalEventProducer()

    # ---------------------------------------------------------------
    # Publish events
    # ---------------------------------------------------------------

    published_count = 0

    for event in events:

        producer.publish(event)

        published_count += 1

        print(
            f"[{published_count:04d}/{len(events):04d}] "
            f"{event.event_type.value:<25} "
            f"patient={event.patient_id:<20} "
            f"event_time={event.event_time.isoformat()}"
        )

        time.sleep(EVENT_DELAY_SECONDS)

    # ---------------------------------------------------------------
    # Flush buffered Kafka messages
    # ---------------------------------------------------------------

    producer.flush()

    print()
    print("=" * 80)
    print("STREAM COMPLETE")
    print("=" * 80)

    print(
        f"Successfully published: {published_count} events"
    )


if __name__ == "__main__":
    main()