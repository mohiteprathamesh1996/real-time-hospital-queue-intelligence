from streaming.spark_session import create_spark_session


PATIENT_STATE_PATH = "./data/gold/patient_state"


def main():
    spark = create_spark_session(
        "InspectPatientState"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    df = (
        spark.read
        .format("delta")
        .load(PATIENT_STATE_PATH)
    )

    print("=" * 100)
    print("PATIENT STATE")
    print("=" * 100)

    print(f"Patients: {df.count()}")
    print()

    df.printSchema()

    print()

    df.orderBy(
        "arrival_time"
    ).show(
        20,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()