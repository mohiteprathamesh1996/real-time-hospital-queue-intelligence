from collections import Counter
from statistics import mean

from config.settings import load_config
from simulator.event_models import EventType
from simulator.hospital_simulator import HospitalSimulator
from simulator.metrics import MetricsCalculator


def main() -> None:
    config = load_config("config/hospital.yaml")

    simulator = HospitalSimulator(
        config,
        seed=42,
    )

    events = simulator.run(
        lab_id="LAB_A",
    )

    calculator = MetricsCalculator(queue_sla_minutes=15)

    patient_metrics = calculator.calculate_patient_metrics(events)

    start_time, end_time = simulator.get_simulation_window()

    window_metrics = calculator.calculate_window_metrics(
        patient_metrics=patient_metrics,
        lab_id="LAB_A",
        start_time=start_time,
        end_time=end_time,
        window_minutes=5,
        station_count=4,
    )

    lab_metrics = calculator.calculate_lab_metrics(
        patient_metrics=patient_metrics,
        lab_id="LAB_A",
        operating_hours=12,
        station_count=4,
    )

    sla_metrics = calculator.calculate_sla_metrics(
        patient_metrics=patient_metrics,
        lab_id="LAB_A",
    )

    print("=" * 60)
    print("HOSPITAL SIMULATION INSPECTION")
    print("=" * 60)

    print(f"Total events: {len(events):,}")

    patient_ids = {
        event.patient_id
        for event in events
    }

    print(f"Unique patients: {len(patient_ids):,}")

    print("\nEvent counts:")
    event_counts = Counter(event.event_type for event in events)

    for event_type, count in event_counts.items():
        print(f"  {event_type.value:<25} {count:,}")

    print("\nPatient types:")

    patient_types = Counter(
        event.patient_type
        for event in events
        if event.event_type == EventType.PATIENT_ARRIVAL
    )

    total_patients = sum(patient_types.values())

    for patient_type, count in patient_types.items():
        percentage = count / total_patients * 100
        print(
            f"  {patient_type.value:<15}"
            f"{count:>6,}"
            f" ({percentage:5.1f}%)"
        )

    print("\nPriorities:")

    priorities = Counter(
        event.priority
        for event in events
        if event.event_type == EventType.PATIENT_ARRIVAL
    )

    for priority, count in priorities.items():
        percentage = count / total_patients * 100
        print(
            f"  {priority.value:<15}"
            f"{count:>6,}"
            f" ({percentage:5.1f}%)"
        )

    arrival_times = [
        event.event_time
        for event in events
        if event.event_type == EventType.PATIENT_ARRIVAL
    ]

    print("\nArrivals by hour:")

    hourly_arrivals = Counter(
        arrival_time.hour
        for arrival_time in arrival_times
    )

    for hour in sorted(hourly_arrivals):
        print(
            f"  {hour:02d}:00"
            f"  {hourly_arrivals[hour]:>4,}"
        )

    service_starts = {
        event.patient_id: event.event_time
        for event in events
        if event.event_type == EventType.SERVICE_STARTED
    }

    queue_entries = {
        event.patient_id: event.event_time
        for event in events
        if event.event_type == EventType.QUEUE_ENTERED
    }

    waiting_times = []

    for patient_id, service_start in service_starts.items():
        arrival_time = queue_entries.get(patient_id)

        if arrival_time is not None:
            wait_minutes = (
                service_start - arrival_time
            ).total_seconds() / 60

            waiting_times.append(wait_minutes)

    if waiting_times:
        print("\nWaiting time:")
        print(f"  Average: {mean(waiting_times):.2f} minutes")
        print(f"  Maximum: {max(waiting_times):.2f} minutes")
        print(
            f"  Patients who waited: "
            f"{sum(wait > 0 for wait in waiting_times):,}"
        )

    print()
    print("=" * 60)
    print("OPERATIONAL METRICS")
    print("=" * 60)

    if patient_metrics:
        average_registration = sum(
            patient.registration_duration_minutes
            for patient in patient_metrics
        ) / len(patient_metrics)

        average_queue_wait = sum(
            patient.queue_wait_minutes
            for patient in patient_metrics
        ) / len(patient_metrics)

        maximum_queue_wait = max(
            patient.queue_wait_minutes
            for patient in patient_metrics
        )

        average_service_time = sum(
            patient.service_duration_minutes
            for patient in patient_metrics
        ) / len(patient_metrics)

        average_journey_time = sum(
            patient.total_journey_minutes
            for patient in patient_metrics
        ) / len(patient_metrics)

        print()
        print("Patient Metrics")
        print(
            f"  Average registration time: {average_registration:.2f} minutes"
        )
        print(
            f"  Average queue wait:        {average_queue_wait:.2f} minutes"
        )
        print(
            f"  Maximum queue wait:        {maximum_queue_wait:.2f} minutes"
        )
        print(
            f"  Average service time:      {average_service_time:.2f} minutes"
        )
        print(
            f"  Average total journey:     {average_journey_time:.2f} minutes"
        )

    print()
    print("Lab Metrics")
    print(f"  Patients served:           {lab_metrics.patients_served}")
    print(f"  Patients per hour:         {lab_metrics.patients_per_hour:.2f}")
    print(
        f"  Total service minutes:     "
        f"{lab_metrics.total_service_minutes:.2f}"
    )
    print(f"  Staff utilization:         {lab_metrics.utilization:.2%}")

    print()
    print("SLA Metrics")
    print("  SLA target:                15.00 minutes")
    print(f"  Patients within SLA:       {sla_metrics.patients_within_sla}")
    print(
        f"  Patients breaching SLA:    "
        f"{sla_metrics.patients_breaching_sla}"
    )
    print(
        f"  SLA breach rate:           "
        f"{sla_metrics.sla_breach_rate:.2%}"
    )
    print()
    print("=" * 60)
    print("5-MINUTE OPERATIONAL METRICS")
    print("=" * 60)

    print()
    print(
        f"{'Window':<16}"
        f"{'Arrivals':>10}"
        f"{'Served':>10}"
        f"{'Avg Wait':>12}"
        f"{'Max Wait':>12}"
        f"{'Utilization':>14}"
        f"{'SLA Breach':>14}"
    )

    for window in window_metrics:
        window_label = (
            f"{window.window_start:%H:%M}"
            f"-"
            f"{window.window_end:%H:%M}"
        )

        print(
            f"{window_label:<20}"
            f"{window.arrivals:>10}"
            f"{window.patients_served:>10}"
            f"{window.average_queue_wait_minutes:>12.2f}"
            f"{window.maximum_queue_wait_minutes:>12.2f}"
            f"{window.utilization:>13.2%}"
            f"{window.sla_breach_rate:>13.2%}"
        )


if __name__ == "__main__":
    main()
