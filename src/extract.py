from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from config import SALES_SCHEMA


def load_sales_data(spark: SparkSession, data_path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(SALES_SCHEMA)
        .csv(str(data_path))
    )
