from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, lit, sum, when


def build_quality_report(raw_sales: DataFrame) -> DataFrame:
    required_columns = ["order_date", "customer", "region", "product", "category"]

    return raw_sales.select(
        count(lit(1)).alias("total_rows"),
        countDistinct("order_id").alias("unique_orders"),
        sum(when(col("order_id").isNull(), 1).otherwise(0)).alias("missing_order_ids"),
        sum(when(col("quantity").isNull() | (col("quantity") <= 0), 1).otherwise(0)).alias(
            "invalid_quantities"
        ),
        sum(when(col("unit_price").isNull() | (col("unit_price") <= 0), 1).otherwise(0)).alias(
            "invalid_prices"
        ),
        sum(when(col("discount").isNull() | (col("discount") < 0), 1).otherwise(0)).alias(
            "invalid_discounts"
        ),
        *[
            sum(when(col(column).isNull(), 1).otherwise(0)).alias(f"missing_{column}")
            for column in required_columns
        ],
    )
