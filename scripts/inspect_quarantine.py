from streaming.spark_session import create_spark_session


QUARANTINE_PATH = "./data/quarantine/hospital_events"


def main():
    spark = create_spark_session(
        "InspectQuarantine"
    )

    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .format("delta")
        .load(QUARANTINE_PATH)
    )

    print("=" * 80)
    print("QUARANTINE TABLE")
    print("=" * 80)

    print(f"Rows: {df.count()}")
    print()

    df.groupBy(
        "quarantine_reason"
    ).count().show(
        truncate=False
    )

    df.show(
        20,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()
