from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, countDistinct, desc, round, sum


def build_kpis(sales: DataFrame) -> dict[str, DataFrame]:
    return {
        "revenue_by_region": sales.groupBy("region")
        .agg(
            countDistinct("order_id").alias("orders"),
            round(sum("net_sales"), 2).alias("total_revenue"),
            round(avg("net_sales"), 2).alias("avg_order_value"),
        )
        .orderBy(desc("total_revenue")),
        "revenue_by_category": sales.groupBy("category")
        .agg(
            countDistinct("order_id").alias("orders"),
            round(sum("net_sales"), 2).alias("total_revenue"),
            round(avg("discount"), 4).alias("avg_discount"),
        )
        .orderBy(desc("total_revenue")),
        "product_performance": sales.groupBy("product", "category")
        .agg(
            countDistinct("order_id").alias("orders"),
            round(sum("net_sales"), 2).alias("total_revenue"),
            round(avg("net_sales"), 2).alias("avg_order_value"),
        )
        .orderBy(desc("total_revenue")),
        "monthly_sales_trend": sales.groupBy("month")
        .agg(
            countDistinct("order_id").alias("orders"),
            round(sum("net_sales"), 2).alias("total_revenue"),
        )
        .orderBy("month"),
        "top_customers": sales.groupBy("customer")
        .agg(
            countDistinct("order_id").alias("orders"),
            round(sum("net_sales"), 2).alias("total_revenue"),
        )
        .orderBy(desc("total_revenue"))
        .limit(5),
        "large_discount_orders": sales.filter(col("discount_level") == "Large discount").select(
            "order_id", "customer", "product", "discount", "net_sales"
        ),
    }
