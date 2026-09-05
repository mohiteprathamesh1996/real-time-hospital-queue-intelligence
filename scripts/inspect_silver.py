from streaming.spark_session import create_spark_session


SILVER_PATH = "./data/silver/hospital_events"


def main():
    spark = create_spark_session(
        "InspectSilver"
    )

    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
    )

    print("=" * 80)
    print("SILVER TABLE")
    print("=" * 80)

    print(f"Rows: {df.count()}")
    print()

    df.printSchema()

    print()

    df.orderBy(
        "event_time"
    ).show(
        20,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()