from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_4.2_2.13:4.4.0",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
            ]),
        )
        .getOrCreate()
    )