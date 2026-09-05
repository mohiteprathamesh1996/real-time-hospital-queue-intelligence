from streaming.spark_session import create_spark_session


GOLD_PATH = "./data/gold/operational_metrics"


def main():
    spark = create_spark_session(
        "InspectGold"
    )

    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .format("delta")
        .load(GOLD_PATH)
    )

    print("=" * 80)
    print("GOLD OPERATIONAL METRICS")
    print("=" * 80)

    print(f"Rows: {df.count()}")
    print()

    df.orderBy(
        "window_start"
    ).show(
        50,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()