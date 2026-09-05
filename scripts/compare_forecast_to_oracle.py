from datetime import timedelta

from config.settings import load_config
from decision_engine.counterfactual_engine import (
    CounterfactualStaffingEngine,
)
from simulator.event_models import PatientType
from simulator.simpy_engine import (
    InitialService,
    InitialWaitingPatient,
)
from streaming.spark_session import (
    create_spark_session,
)


PATIENT_METRICS_PATH = (
    "./data/gold/patient_metrics"
)

ORACLE_PATH = (
    "./data/gold/full_timeline_counterfactual"
)

FORECAST_PATH = (
    "./data/gold/probabilistic_staffing_timeline"
)


FORECAST_HORIZON_MINUTES = 30

BASELINE_STAFF = 4
QUEUE_SLA_MINUTES = 15.0
TARGET_SLA_PERCENTAGE = 95.0


def to_patient_type(value):

    if isinstance(
        value,
        PatientType,
    ):
        return value

    return PatientType(value)


def build_current_state(
    patients,
    timestamp,
):
    waiting_rows = (
        patients
        .filter(
            (
                patients.queue_entry_time
                <= timestamp
            )
            & (
                patients.service_start_time
                > timestamp
            )
        )
        .collect()
    )

    initial_waiting = [
        InitialWaitingPatient(
            patient_type=to_patient_type(
                row["patient_type"]
            ),
            service_duration_minutes=float(
                row["service_minutes"]
            ),
            accrued_wait_minutes=(
                (
                    timestamp
                    - row["queue_entry_time"]
                ).total_seconds()
                / 60.0
            ),
        )
        for row in waiting_rows
    ]

    service_rows = (
        patients
        .filter(
            (
                patients.service_start_time
                <= timestamp
            )
            & (
                patients.service_end_time
                > timestamp
            )
        )
        .collect()
    )

    initial_services = [
        InitialService(
            remaining_service_minutes=(
                (
                    row["service_end_time"]
                    - timestamp
                ).total_seconds()
                / 60.0
            )
        )
        for row in service_rows
    ]

    return (
        initial_waiting,
        initial_services,
    )


def build_actual_future(
    patients,
    timestamp,
):
    end_time = (
        timestamp
        + timedelta(
            minutes=(
                FORECAST_HORIZON_MINUTES
            )
        )
    )

    rows = (
        patients
        .filter(
            (
                patients.arrival_time
                >= timestamp
            )
            & (
                patients.arrival_time
                < end_time
            )
        )
        .orderBy(
            "arrival_time"
        )
        .collect()
    )

    arrivals = [
        (
            (
                (
                    row["arrival_time"]
                    - timestamp
                ).total_seconds()
                / 60.0
            ),
            to_patient_type(
                row["patient_type"]
            ),
        )
        for row in rows
    ]

    durations = [
        float(
            row["service_minutes"]
        )
        for row in rows
    ]

    return (
        arrivals,
        durations,
    )


def main():

    spark = create_spark_session(
        "CompareForecastToOracle"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    config = load_config(
        "config/hospital.yaml"
    )

    patients = (
        spark.read
        .format("delta")
        .load(
            PATIENT_METRICS_PATH
        )
    )

    oracle = (
        spark.read
        .format("delta")
        .load(
            ORACLE_PATH
        )
        .select(
            "timestamp",
            "model_additional_staff",
        )
        .alias("o")
    )

    forecast = (
        spark.read
        .format("delta")
        .load(
            FORECAST_PATH
        )
        .filter(
            "forecast_eligible = true"
        )
        .alias("f")
    )

    joined = (
        forecast
        .join(
            oracle,
            forecast.timestamp
            == oracle.timestamp,
            "inner",
        )
        .select(
            forecast.timestamp,
            forecast.forecast_additional_staff,
            forecast.selected_sla_success_probability,
            oracle.model_additional_staff.alias(
                "oracle_additional_staff"
            ),
        )
        .orderBy(
            "timestamp"
        )
        .collect()
    )

    deterministic_engine = (
        CounterfactualStaffingEngine(
            config=config,
            baseline_staff=(
                BASELINE_STAFF
            ),
            queue_sla_minutes=(
                QUEUE_SLA_MINUTES
            ),
            target_sla_percentage=(
                TARGET_SLA_PERCENTAGE
            ),
        )
    )

    exact = 0
    over = 0
    under = 0

    absolute_differences = []
    signed_differences = []

    actual_sla_passes = 0

    evaluated_actual = 0

    for row in joined:

        forecast_staff = int(
            row[
                "forecast_additional_staff"
            ]
            or 0
        )

        oracle_staff = int(
            row[
                "oracle_additional_staff"
            ]
            or 0
        )

        difference = (
            forecast_staff
            - oracle_staff
        )

        absolute_differences.append(
            abs(difference)
        )

        signed_differences.append(
            difference
        )

        if difference == 0:
            exact += 1
        elif difference > 0:
            over += 1
        else:
            under += 1

        timestamp = row["timestamp"]

        (
            initial_waiting,
            initial_services,
        ) = build_current_state(
            patients,
            timestamp,
        )

        (
            actual_arrivals,
            actual_durations,
        ) = build_actual_future(
            patients,
            timestamp,
        )

        actual_result = (
            deterministic_engine.evaluate(
                arrivals=actual_arrivals,
                service_durations=(
                    actual_durations
                ),
                additional_staff=(
                    forecast_staff
                ),
                initial_waiting=(
                    initial_waiting
                ),
                initial_services=(
                    initial_services
                ),
            )
        )

        evaluated_actual += 1

        if actual_result.meets_sla:
            actual_sla_passes += 1

    total = len(joined)

    agreement_percentage = (
        exact / total * 100
        if total
        else 0.0
    )

    mean_absolute_difference = (
        sum(
            absolute_differences
        )
        / len(
            absolute_differences
        )
        if absolute_differences
        else 0.0
    )

    mean_signed_difference = (
        sum(
            signed_differences
        )
        / len(
            signed_differences
        )
        if signed_differences
        else 0.0
    )

    actual_sla_success_rate = (
        actual_sla_passes
        / evaluated_actual
        * 100
        if evaluated_actual
        else 0.0
    )

    print("=" * 110)
    print(
        "FORECAST-DRIVEN VS ORACLE STAFFING"
    )
    print("=" * 110)

    print(
        f"Eligible windows:              "
        f"{total}"
    )

    print(
        f"Exact staffing agreement:      "
        f"{exact}"
    )

    print(
        f"Agreement rate:                "
        f"{agreement_percentage:.1f}%"
    )

    print(
        f"Forecast overstaffing windows: "
        f"{over}"
    )

    print(
        f"Forecast understaffing windows:"
        f" {under}"
    )

    print(
        f"Mean absolute staff error:     "
        f"{mean_absolute_difference:.2f}"
    )

    print(
        f"Mean signed staff error:       "
        f"{mean_signed_difference:.2f}"
    )

    print()

    print("=" * 110)
    print(
        "ACTUAL-FUTURE VALIDATION OF FORECAST POLICY"
    )
    print("=" * 110)

    print(
        f"Windows evaluated:             "
        f"{evaluated_actual}"
    )

    print(
        f"Actual SLA passes:             "
        f"{actual_sla_passes}"
    )

    print(
        f"Actual SLA success rate:       "
        f"{actual_sla_success_rate:.1f}%"
    )

    print()

    print(
        "The staffing recommendation was generated "
        "without future knowledge. Actual future "
        "arrivals are used only here for historical "
        "evaluation."
    )

    print("=" * 110)

    spark.stop()


if __name__ == "__main__":
    main()
