from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATA_PATH = BASE_DIR / "data" / "sales.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "output"

SALES_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("order_date", StringType(), False),
        StructField("customer", StringType(), False),
        StructField("region", StringType(), False),
        StructField("product", StringType(), False),
        StructField("category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount", DoubleType(), False),
    ]
)
