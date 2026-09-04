from datetime import datetime, timedelta
from statistics import mean
from config.settings import load_config
from simulator.arrival_process import PoissonArrivalProcess
from simulator.event_models import PatientType
from simulator.service_process import ServiceTimeGenerator
from simulator.simpy_engine import SimPyQueueEngine
from simulator.staffing_optimizer import StaffingOptimizer


QUEUE_SLA_MINUTES = 15.0


def calculate_p95(values):
    """Calculate the 95th percentile using nearest-rank style indexing."""
    if not values:
        return 0.0

    values = sorted(values)

    index = int(0.95 * (len(values) - 1))

    return values[index]


def main():
    config = load_config("config/hospital.yaml")

    start_time = datetime.fromisoformat(
        config.simulation.start_time
    )

    end_time = (
        start_time
        + timedelta(
            hours=config.simulation.duration_hours
        )
    )

    arrival_process = PoissonArrivalProcess.from_config(
        config=config,
        seed=42,
    )

    patient_type_weights = {
        PatientType.OUTPATIENT:
            config.patient_profiles["outpatient"].arrival_weight,

        PatientType.INPATIENT:
            config.patient_profiles["inpatient"].arrival_weight,

        PatientType.WALK_IN:
            config.patient_profiles["walk_in"].arrival_weight,

        PatientType.FOLLOW_UP:
            config.patient_profiles["follow_up"].arrival_weight,
    }

    arrivals_datetime = (
        arrival_process.generate_patient_arrivals(
            start_time=start_time,
            end_time=end_time,
            patient_type_rates=patient_type_weights,
        )
    )

    arrivals = [
        (
            (
                arrival_time - start_time
            ).total_seconds() / 60,
            patient_type,
        )
        for arrival_time, patient_type
        in arrivals_datetime
    ]

    print("=" * 115)
    print("SIMPY STAFFING EXPERIMENT")
    print("=" * 115)

    print(f"Patients generated: {len(arrivals)}")
    print(f"Queue SLA:           {QUEUE_SLA_MINUTES:.0f} minutes")
    optimizer = StaffingOptimizer(
        hourly_cost_per_staff=35,
        target_sla_percentage=95,
        queue_sla_minutes=QUEUE_SLA_MINUTES,
    )
    print()

    print(
        f"{'Staff':<8}"
        f"{'Patients':>12}"
        f"{'Avg Wait':>14}"
        f"{'P95 Wait':>14}"
        f"{'Max Wait':>14}"
        f"{'SLA <=15':>14}"
        f"{'Total Wait':>16}"
        f"{'Utilization':>16}"
    )

    print("-" * 115)

    scenarios = []

    for station_count in [2, 3, 4, 5]:

        service_generator = ServiceTimeGenerator(
            config=config,
            seed=43,
        )

        service_durations = [
            service_generator.generate_service_time(patient_type)
            for _, patient_type in arrivals
        ]

        engine = SimPyQueueEngine(
            station_count=station_count,
            service_time_generator=service_generator,
        )

        results = engine.run(arrivals)

        waits = [
            result.queue_wait_minutes
            for result in results
        ]

        average_wait = mean(waits)
        p95_wait = calculate_p95(waits)
        max_wait = max(waits)
        total_wait = sum(waits)

        patients_within_sla = sum(
            wait <= QUEUE_SLA_MINUTES
            for wait in waits
        )

        sla_percentage = (
            patients_within_sla
            / len(waits)
            * 100
            if waits
            else 0.0
        )

        total_service_minutes = sum(
            result.service_duration_minutes
            for result in results
        )

        simulation_minutes = (
            end_time - start_time
        ).total_seconds() / 60

        available_staff_minutes = (
            simulation_minutes * station_count
        )

        utilization = (
            total_service_minutes
            / available_staff_minutes
            * 100
            if available_staff_minutes > 0
            else 0.0
        )

        scenario = optimizer.evaluate_scenario(
            staff_count=station_count,
            average_wait_minutes=average_wait,
            p95_wait_minutes=p95_wait,
            max_wait_minutes=max_wait,
            sla_percentage=sla_percentage,
            utilization_percentage=utilization,
        )

        scenarios.append(scenario)

        print(
            f"{station_count:<8}"
            f"{len(results):>12}"
            f"{average_wait:>14.2f}"
            f"{p95_wait:>14.2f}"
            f"{max_wait:>14.2f}"
            f"{sla_percentage:>13.1f}%"
            f"{total_wait:>16.2f}"
            f"{utilization:>15.1f}%"
        )

    optimal = optimizer.find_minimum_staffing(
        scenarios
    )

    print()
    print("=" * 80)
    print("STAFFING RECOMMENDATION")
    print("=" * 80)

    print(f"Required staffing: {optimal.staff_count}")
    print(
        f"Target SLA:        "
        f"{optimizer.target_sla_percentage:.1f}%"
    )
    print(
        f"Expected SLA:      "
        f"{optimal.sla_percentage:.1f}%"
    )
    print(
        f"Expected avg wait: "
        f"{optimal.average_wait_minutes:.2f} minutes"
    )
    print(
        f"Expected P95 wait: "
        f"{optimal.p95_wait_minutes:.2f} minutes"
    )
    print(
        f"Utilization:       "
        f"{optimal.utilization_percentage:.1f}%"
    )
    print(
        f"Hourly staff cost: "
        f"${optimal.hourly_staffing_cost:.2f}"
    )


if __name__ == "__main__":
    main()