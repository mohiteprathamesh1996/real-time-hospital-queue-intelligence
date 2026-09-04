from datetime import datetime, timedelta
from statistics import mean

from config.settings import load_config
from simulator.capacity_scenarios import (
    CapacityScenarioGenerator,
)
from simulator.service_process import (
    ServiceTimeGenerator,
)
from simulator.simpy_engine import (
    SimPyQueueEngine,
)


QUEUE_SLA_MINUTES = 15.0
TARGET_SLA_PERCENTAGE = 95.0

STAFFING_LEVELS = [3, 4, 5, 6, 7]

DEMAND_SCENARIOS = [
    ("Baseline", 1.0),
    ("Demand +20%", 1.2),
    ("Demand +40%", 1.4),
    ("Demand +60%", 1.6),
]


def calculate_p95(values):

    if not values:
        return 0.0

    values = sorted(values)

    index = int(
        0.95 * (len(values) - 1)
    )

    return values[index]


def main():

    config = load_config(
        "config/hospital.yaml"
    )

    start_time = datetime.fromisoformat(
        config.simulation.start_time
    )

    end_time = (
        start_time
        + timedelta(
            hours=config.simulation.duration_hours
        )
    )

    scenario_generator = (
        CapacityScenarioGenerator(
            config=config,
            start_time=start_time,
            end_time=end_time,
            seed=42,
        )
    )

    print("=" * 110)
    print("CAPACITY SCENARIO EXPERIMENT")
    print("=" * 110)

    print()

    for scenario_name, multiplier in DEMAND_SCENARIOS:

        scenario = scenario_generator.generate(
            name=scenario_name,
            demand_multiplier=multiplier,
        )

        print()
        print("=" * 110)
        print(
            f"{scenario.name} "
            f"({scenario.demand_multiplier:.1f}x demand)"
        )
        print(
            f"Patients: {len(scenario.arrivals)}"
        )
        print("=" * 110)

        # Generate service durations once.
        service_generator = (
            ServiceTimeGenerator(
                config=config,
                seed=43,
            )
        )

        service_durations = [
            service_generator.generate_service_time(
                patient_type
            )
            for _, patient_type
            in scenario.arrivals
        ]

        simulation_minutes = (
            end_time - start_time
        ).total_seconds() / 60

        print(
            f"{'Staff':<8}"
            f"{'Avg Wait':>14}"
            f"{'P95 Wait':>14}"
            f"{'SLA':>12}"
            f"{'Utilization':>16}"
            f"{'Status':>14}"
        )

        print("-" * 90)

        for staff_count in STAFFING_LEVELS:

            engine = SimPyQueueEngine(
                station_count=staff_count,
                service_time_generator=(
                    service_generator
                ),
            )

            results = engine.run(
                scenario.arrivals,
                service_durations=(
                    service_durations
                ),
            )

            waits = [
                result.queue_wait_minutes
                for result in results
            ]

            average_wait = mean(waits)

            p95_wait = calculate_p95(waits)

            patients_within_sla = sum(
                wait <= QUEUE_SLA_MINUTES
                for wait in waits
            )

            sla_percentage = (
                patients_within_sla
                / len(waits)
                * 100
            )

            total_service_minutes = sum(
                result.service_duration_minutes
                for result in results
            )

            available_staff_minutes = (
                simulation_minutes
                * staff_count
            )

            utilization = (
                total_service_minutes
                / available_staff_minutes
                * 100
            )

            status = (
                "MEETS SLA"
                if sla_percentage
                >= TARGET_SLA_PERCENTAGE
                else "FAILS SLA"
            )

            print(
                f"{staff_count:<8}"
                f"{average_wait:>14.2f}"
                f"{p95_wait:>14.2f}"
                f"{sla_percentage:>11.1f}%"
                f"{utilization:>15.1f}%"
                f"{status:>14}"
            )


if __name__ == "__main__":
    main()