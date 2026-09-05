from streaming.spark_session import create_spark_session


PATH = "./data/gold/operational_metrics_5m"


def main():
    spark = create_spark_session(
        "InspectOperationalMetrics"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    df = (
        spark.read
        .format("delta")
        .load(PATH)
    )

    print("=" * 120)
    print("5-MINUTE GOLD OPERATIONAL METRICS")
    print("=" * 120)

    print(
        f"Windows: {df.count()}"
    )

    print()

    df.orderBy(
        "window_start"
    ).show(
        200,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()