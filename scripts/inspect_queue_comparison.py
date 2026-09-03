from simulator.hospital_simulator import HospitalSimulator
from simulator.metrics import MetricsCalculator
from simulator.queue_comparison import QueueComparisonEngine
from simulator.queue_validation import QueueValidationEngine

def main():

    simulator = HospitalSimulator(
        config=simulator_config(),
        seed=42,
    )

    events = simulator.run(
        lab_id="LAB_A"
    )

    calculator = MetricsCalculator(
        queue_sla_minutes=15
    )

    patient_metrics = calculator.calculate_patient_metrics(
        events
    )

    start_time, end_time = (
        simulator.get_simulation_window()
    )

    windows = calculator.calculate_window_metrics(
        patient_metrics=patient_metrics,
        lab_id="LAB_A",
        start_time=start_time,
        end_time=end_time,
        window_minutes=30,
        station_count=4,
    )

    engine = QueueComparisonEngine(
        server_count=4
    )

    comparisons = engine.compare_windows(
        windows=windows,
        patient_metrics=patient_metrics,
    )

    local_comparisons = [
        engine.compare_window_with_local_service_rate(
        window=window,
        patient_metrics=patient_metrics,
    )
    for window in windows
    ]

    print("=" * 100)
    print("SIMULATION VS M/M/c QUEUE MODEL")
    print("=" * 100)

    print()

    print(
        f"{'Window':<15}"
        f"{'λ':>8}"
        f"{'μ Local':>10}"
        f"{'ρ':>9}"
        f"{'M/M/c Wq':>12}"
        f"{'Observed Wq':>15}"
        f"{'Difference':>14}"
        f"{'Status':>12}"
    )

    print("-" * 100)

    for result in local_comparisons:

        window = (
            f"{result.window_start:%H:%M}"
            f"-"
            f"{result.window_end:%H:%M}"
        )

        if result.system_stable:

            theoretical_wait = (
                f"{result.theoretical_wait_minutes:>11.2f}"
            )

            difference = (
                f"{result.wait_difference_minutes:>13.2f}"
            )

            status = "STABLE"

        else:

            theoretical_wait = f"{'UNSTABLE':>11}"
            difference = f"{'N/A':>13}"
            status = "UNSTABLE"

        print(
            f"{window:<15}"
            f"{result.arrival_rate_per_hour:>8.1f}"
            f"{result.service_rate_per_hour:>10.1f}"
            f"{result.theoretical_utilization:>8.1%}"
            f"{theoretical_wait:>12}"
            f"{result.observed_wait_minutes:>15.2f}"
            f"{difference:>14}"
            f"{status:>12}"
        )

    print()
    print("=" * 100)


    print()
    print("=" * 100)
    print("VALIDATION SUMMARY")
    print("=" * 100)

    validation_engine = QueueValidationEngine()

    summary = validation_engine.summarize(
        local_comparisons
    )

    print(
        f"Total windows:              "
        f"{summary.total_windows}"
    )

    print(
        f"Stable windows:             "
        f"{summary.stable_windows}"
    )

    print(
        f"Unstable windows:           "
        f"{summary.unstable_windows}"
    )

    print(
        f"Mean theoretical Wq:        "
        f"{summary.theoretical_mean_wait_minutes:.2f} min"
    )

    print(
        f"Mean observed Wq:           "
        f"{summary.observed_mean_wait_minutes:.2f} min"
    )

    print(
        f"MAE:                         "
        f"{summary.mean_absolute_error_minutes:.2f} min"
    )

    print(
        f"RMSE:                        "
        f"{summary.root_mean_squared_error_minutes:.2f} min"
    )

    print(
        f"Mean bias:                   "
        f"{summary.mean_bias_minutes:.2f} min"
    )



def simulator_config():
    from config.settings import load_config

    return load_config(
        "config/hospital.yaml"
    )


if __name__ == "__main__":
    main()