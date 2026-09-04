from datetime import datetime, timedelta

from config.settings import load_config
from simulator.capacity_scenarios import CapacityScenarioGenerator
from simulator.resilience_planner import ResiliencePlanner
from simulator.service_process import ServiceTimeGenerator
from simulator.simpy_engine import SimPyQueueEngine
from simulator.staff_absence_scenarios import (
    StaffAbsenceScenarioGenerator,
)


QUEUE_SLA_MINUTES = 15.0
TARGET_SLA_PERCENTAGE = 95.0

DEMAND_SCENARIOS = [
    ("Baseline", 1.0),
    ("Demand +20%", 1.2),
    ("Demand +40%", 1.4),
    ("Demand +60%", 1.6),
]

PLANNED_STAFFING = [4, 5, 6, 7, 8, 9]

ABSENCE_LEVELS = [0, 1, 2]


def calculate_metrics(results):
    """Calculate queue performance metrics for a simulation run."""

    waits = [
        result.queue_wait_minutes
        for result in results
    ]

    sla_count = sum(
        wait <= QUEUE_SLA_MINUTES
        for wait in waits
    )

    average_wait = sum(waits) / len(waits)

    sorted_waits = sorted(waits)

    p95_index = int(
        0.95 * (len(sorted_waits) - 1)
    )

    p95_wait = sorted_waits[p95_index]

    sla_percentage = (
        sla_count / len(waits) * 100
    )

    total_service_minutes = sum(
        result.service_duration_minutes
        for result in results
    )

    return {
        "average_wait": average_wait,
        "p95_wait": p95_wait,
        "sla_percentage": sla_percentage,
        "total_service_minutes": total_service_minutes,
    }


def main():

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    config = load_config(
        "config/hospital.yaml"
    )

    resilience_planner = ResiliencePlanner(
        target_sla_percentage=TARGET_SLA_PERCENTAGE
    )

    absence_generator = (
        StaffAbsenceScenarioGenerator()
    )

    all_results = []

    # ------------------------------------------------------------------
    # Simulation time window
    # ------------------------------------------------------------------

    start_time = datetime.fromisoformat(
        config.simulation.start_time
    )

    end_time = (
        start_time
        + timedelta(
            hours=config.simulation.duration_hours
        )
    )

    # ------------------------------------------------------------------
    # Capacity scenario generator
    # ------------------------------------------------------------------

    capacity_generator = CapacityScenarioGenerator(
        config=config,
        start_time=start_time,
        end_time=end_time,
        seed=42,
    )

    # ------------------------------------------------------------------
    # Experiment header
    # ------------------------------------------------------------------

    print("=" * 110)
    print("STAFF RESILIENCE EXPERIMENT")
    print("=" * 110)

    print(
        f"Target SLA: >= {TARGET_SLA_PERCENTAGE:.1f}% "
        f"of patients waiting <= {QUEUE_SLA_MINUTES:.0f} minutes"
    )

    print()

    # ------------------------------------------------------------------
    # Demand scenarios
    # ------------------------------------------------------------------

    for demand_name, demand_multiplier in DEMAND_SCENARIOS:

        # --------------------------------------------------------------
        # Generate demand scenario
        # --------------------------------------------------------------

        scenario = capacity_generator.generate(
            name=demand_name,
            demand_multiplier=demand_multiplier,
        )

        # --------------------------------------------------------------
        # Generate service durations ONCE
        #
        # The same service durations are reused for every staffing
        # scenario. This makes the experiment controlled.
        # --------------------------------------------------------------

        service_generator = ServiceTimeGenerator(
            config,
            seed=43,
        )

        service_durations = [
            service_generator.generate_service_time(
                patient_type
            )
            for _, patient_type in scenario.arrivals
        ]

        # --------------------------------------------------------------
        # Scenario header
        # --------------------------------------------------------------

        print()
        print("=" * 110)

        print(
            f"{demand_name} "
            f"({demand_multiplier:.1f}x demand)"
        )

        print(
            f"Patients: {len(scenario.arrivals)}"
        )

        print("=" * 110)

        # --------------------------------------------------------------
        # Table header
        # --------------------------------------------------------------

        print(
            f"{'Planned':>8}"
            f"{'Absent':>8}"
            f"{'Available':>10}"
            f"{'Avg Wait':>14}"
            f"{'P95 Wait':>14}"
            f"{'SLA':>12}"
            f"{'Status':>15}"
        )

        print("-" * 100)

        # --------------------------------------------------------------
        # Staffing scenarios
        # --------------------------------------------------------------

        for planned_staff in PLANNED_STAFFING:

            for unavailable_staff in ABSENCE_LEVELS:

                # Cannot have more absent staff than planned staff
                if unavailable_staff >= planned_staff:
                    continue

                # ------------------------------------------------------
                # Determine available staff
                # ------------------------------------------------------

                availability = (
                    absence_generator.generate(
                        planned_staff=planned_staff,
                        unavailable_staff=unavailable_staff,
                    )
                )

                # ------------------------------------------------------
                # Run SimPy simulation
                #
                # IMPORTANT:
                # service_durations is explicitly supplied so every
                # staffing scenario uses identical service demand.
                # ------------------------------------------------------

                engine = SimPyQueueEngine(
                    station_count=availability.available_staff,
                    service_time_generator=ServiceTimeGenerator(
                        config,
                        seed=43,
                    ),
                )

                results = engine.run(
                    scenario.arrivals,
                    service_durations=service_durations,
                )

                # ------------------------------------------------------
                # Calculate metrics
                # ------------------------------------------------------

                metrics = calculate_metrics(
                    results
                )

                # ------------------------------------------------------
                # Evaluate against resilience policy
                # ------------------------------------------------------

                resilience_result = (
                    resilience_planner.evaluate(
                        demand_scenario=demand_name,
                        planned_staff=planned_staff,
                        unavailable_staff=unavailable_staff,
                        available_staff=availability.available_staff,
                        sla_percentage=metrics[
                            "sla_percentage"
                        ],
                        average_wait_minutes=metrics[
                            "average_wait"
                        ],
                        p95_wait_minutes=metrics[
                            "p95_wait"
                        ],
                    )
                )

                all_results.append(
                    resilience_result
                )

                # ------------------------------------------------------
                # Status
                # ------------------------------------------------------

                status = (
                    "MEETS SLA"
                    if resilience_result.meets_sla
                    else "FAILS SLA"
                )

                # ------------------------------------------------------
                # Print result
                # ------------------------------------------------------

                print(
                    f"{planned_staff:>8}"
                    f"{unavailable_staff:>8}"
                    f"{availability.available_staff:>10}"
                    f"{metrics['average_wait']:>14.2f}"
                    f"{metrics['p95_wait']:>14.2f}"
                    f"{metrics['sla_percentage']:>11.1f}%"
                    f"{status:>15}"
                )

    # ==================================================================
    # RESILIENCE DECISION MATRIX
    # ==================================================================

    print()
    print()
    print("=" * 90)
    print("RESILIENCE DECISION MATRIX")
    print("=" * 90)

    print(
        f"Target: >= {TARGET_SLA_PERCENTAGE:.1f}% "
        f"of patients waiting <= {QUEUE_SLA_MINUTES:.0f} minutes"
    )

    print()

    print(
        f"{'Demand':<20}"
        f"{'Normal':>12}"
        f"{'1 Absence':>15}"
        f"{'2 Absences':>16}"
    )

    print("-" * 65)

    # --------------------------------------------------------------
    # Find minimum planned staffing for every demand / absence
    # combination.
    # --------------------------------------------------------------

    for demand_name, _ in DEMAND_SCENARIOS:

        demand_results = [
            result
            for result in all_results
            if result.demand_scenario == demand_name
        ]

        minimum_staffing = []

        for absence_count in ABSENCE_LEVELS:

            minimum = (
                resilience_planner.minimum_resilient_staffing(
                    demand_results,
                    unavailable_staff=absence_count,
                )
            )

            if minimum is None:
                minimum_staffing.append("N/A")
            else:
                minimum_staffing.append(
                    str(minimum)
                )

        print(
            f"{demand_name:<20}"
            f"{minimum_staffing[0]:>12}"
            f"{minimum_staffing[1]:>15}"
            f"{minimum_staffing[2]:>16}"
        )

    print()

    print("=" * 90)
    print("INTERPRETATION")
    print("=" * 90)

    print(
        "Each value represents the minimum PLANNED staffing "
        "required to maintain the target SLA under the specified "
        "staff-absence condition."
    )

    print(
        "For example, if the matrix shows 7 under '1 Absence', "
        "the lab needs 7 planned staff so that it can still "
        "meet the SLA with one staff member unavailable."
    )

    print("=" * 90)


if __name__ == "__main__":
    main()