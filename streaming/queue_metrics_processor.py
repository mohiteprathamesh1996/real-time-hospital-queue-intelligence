from datetime import timedelta

from pyspark.sql import Row
from pyspark.sql.functions import (
    avg,
    col,
    count,
    lit,
    sum as spark_sum,
    when,
)

from streaming.spark_session import create_spark_session


PATIENT_METRICS_PATH = "./data/gold/patient_metrics"
QUEUE_METRICS_PATH = "./data/gold/queue_metrics_5m"

WINDOW_MINUTES = 5
STAFF_COUNT = 4


def main():
    spark = create_spark_session(
        "HospitalQueueMetrics"
    )

    spark.sparkContext.setLogLevel("WARN")

    patients = (
        spark.read
        .format("delta")
        .load(PATIENT_METRICS_PATH)
    )

    bounds = (
        patients
        .select(
            "arrival_time",
            "departure_time",
        )
        .agg(
            {
                "arrival_time": "min",
                "departure_time": "max",
            }
        )
        .collect()[0]
    )

    start_time = bounds[
        "min(arrival_time)"
    ]

    end_time = bounds[
        "max(departure_time)"
    ]

    timestamps = []

    current = start_time.replace(
        second=0,
        microsecond=0,
    )

    while current <= end_time:
        timestamps.append(
            Row(timestamp=current)
        )

        current += timedelta(
            minutes=WINDOW_MINUTES
        )

    timeline = spark.createDataFrame(
        timestamps
    )

    # ---------------------------------------------------------------
    # Queue length
    #
    # A patient is waiting if:
    #
    # queue_entry_time <= timestamp
    # AND
    # service_start_time > timestamp
    # ---------------------------------------------------------------

    queue_state = (
        timeline.alias("t")
        .crossJoin(
            patients.alias("p")
        )
        .groupBy(
            col("t.timestamp")
        )
        .agg(
            spark_sum(
                when(
                    (
                        col("p.queue_entry_time")
                        <= col("t.timestamp")
                    )
                    &
                    (
                        col("p.service_start_time")
                        > col("t.timestamp")
                    ),
                    1,
                ).otherwise(0)
            ).alias(
                "patients_waiting"
            ),

            spark_sum(
                when(
                    (
                        col("p.service_start_time")
                        <= col("t.timestamp")
                    )
                    &
                    (
                        col("p.service_end_time")
                        > col("t.timestamp")
                    ),
                    1,
                ).otherwise(0)
            ).alias(
                "patients_in_service"
            ),
        )
    )

    # ---------------------------------------------------------------
    # Instantaneous staff utilization
    # ---------------------------------------------------------------

    queue_state = (
        queue_state
        .withColumn(
            "utilization_percentage",
            (
                col("patients_in_service")
                / lit(STAFF_COUNT)
            ) * 100,
        )
    )

    (
        queue_state
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .save(
            QUEUE_METRICS_PATH
        )
    )

    print("=" * 80)
    print("QUEUE STATE METRICS CREATED")
    print("=" * 80)

    print(
        f"Snapshots written: "
        f"{queue_state.count()}"
    )

    spark.stop()


if __name__ == "__main__":
    main()