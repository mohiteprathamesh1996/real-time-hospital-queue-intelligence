from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from decision_engine.engine import StaffingDecisionEngine
from decision_engine.models import OperationalState
from streaming.spark_session import create_spark_session


INPUT_PATH = "./data/gold/operational_state_5m"
OUTPUT_PATH = "./data/gold/staffing_decisions_5m"


def main():
    spark = create_spark_session(
        "HospitalStaffingDecisions"
    )

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .format("delta")
        .load(INPUT_PATH)
        .orderBy("window_start")
    )

    engine = StaffingDecisionEngine()

    rows = df.collect()

    decisions = []

    for row in rows:

        state = OperationalState(
            timestamp=row["window_start"],
            lab_id=row["lab_id"],
            patients_arrived=row["patients_arrived"],
            patients_waiting=row["patients_waiting"],
            patients_in_service=row["patients_in_service"],
            utilization_percentage=float(
                row["utilization_percentage"]
            ),
            average_wait_minutes=float(
                row["avg_queue_wait_minutes"]
            ),
            p95_wait_minutes=float(
                row["p95_queue_wait_minutes"]
            ),
            sla_compliance_rate=float(
                row["sla_compliance_rate"]
            ),
            queue_pressure=row["queue_pressure"],
            operational_status=row["operational_status"],
        )

        recommendation = engine.evaluate(state)

        decisions.append(
            (
                recommendation.timestamp,
                recommendation.lab_id,
                recommendation.intervention_required,
                recommendation.severity,
                recommendation.additional_staff,
                recommendation.reason,
            )
        )

    schema = StructType(
        [
            StructField(
                "timestamp",
                df.schema["window_start"].dataType,
                False,
            ),
            StructField(
                "lab_id",
                StringType(),
                False,
            ),
            StructField(
                "intervention_required",
                BooleanType(),
                False,
            ),
            StructField(
                "severity",
                StringType(),
                False,
            ),
            StructField(
                "additional_staff",
                IntegerType(),
                False,
            ),
            StructField(
                "reason",
                StringType(),
                False,
            ),
        ]
    )

    decisions_df = spark.createDataFrame(
        decisions,
        schema=schema,
    )

    (
        decisions_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(OUTPUT_PATH)
    )

    print("=" * 80)
    print("STAFFING DECISIONS CREATED")
    print("=" * 80)
    print(f"Operational windows: {len(rows)}")
    print(f"Decisions written:   {decisions_df.count()}")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()