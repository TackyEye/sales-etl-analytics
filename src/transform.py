from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, round, to_date, when


def clean_and_transform_sales(raw_sales: DataFrame) -> DataFrame:
    return (
        raw_sales.dropna(subset=["order_id", "order_date", "region", "product"])
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .filter(col("order_date").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
        .filter(col("discount") >= 0)
        .withColumn("gross_sales", round(col("quantity") * col("unit_price"), 2))
        .withColumn("discount_amount", round(col("gross_sales") * col("discount"), 2))
        .withColumn("net_sales", round(col("gross_sales") - col("discount_amount"), 2))
        .withColumn("month", date_format(col("order_date"), "yyyy-MM"))
        .withColumn(
            "discount_level",
            when(col("discount") == 0, "No discount")
            .when(col("discount") <= 0.05, "Small discount")
            .when(col("discount") <= 0.10, "Medium discount")
            .otherwise("Large discount"),
        )
        .withColumn(
            "order_size",
            when(col("net_sales") < 1000, "Small")
            .when(col("net_sales") < 10000, "Medium")
            .otherwise("Large"),
        )
    )
