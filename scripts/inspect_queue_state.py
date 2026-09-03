from config.settings import load_config
from simulator.hospital_simulator import HospitalSimulator
from simulator.metrics import MetricsCalculator
from simulator.queue_state import QueueStateCalculator


def main():
    config = load_config("config/hospital.yaml")

    simulator = HospitalSimulator(
        config=config,
        seed=42,
    )

    events = simulator.run(
        lab_id="LAB_A"
    )

    metrics_calculator = MetricsCalculator()

    patient_metrics = (
        metrics_calculator.calculate_patient_metrics(
            events
        )
    )

    start_time, end_time = (
        simulator.get_simulation_window()
    )

    queue_calculator = QueueStateCalculator()

    queue_states = queue_calculator.calculate_series(
        patient_metrics=patient_metrics,
        lab_id="LAB_A",
        start_time=start_time,
        end_time=end_time,
        window_minutes=30,
    )

    print("=" * 70)
    print("QUEUE BACKLOG / STATE")
    print("=" * 70)

    print(
        f"{'Time':<12}"
        f"{'Patients Waiting':>20}"
    )

    print("-" * 70)

    for state in queue_states:
        print(
            f"{state.timestamp:%H:%M}"
            f"{state.patients_waiting:>20}"
        )


if __name__ == "__main__":
    main()