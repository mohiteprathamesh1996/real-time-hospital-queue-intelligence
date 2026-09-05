from streaming.spark_session import create_spark_session


PATH = "./data/gold/queue_metrics_5m"


def main():
    spark = create_spark_session(
        "InspectQueueGold"
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    df = (
        spark.read
        .format("delta")
        .load(PATH)
    )

    print("=" * 80)
    print("GOLD QUEUE STATE")
    print("=" * 80)

    df.orderBy(
        "timestamp"
    ).show(
        200,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()