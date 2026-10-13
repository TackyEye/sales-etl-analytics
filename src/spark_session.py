from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Sales ETL Analytics")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
