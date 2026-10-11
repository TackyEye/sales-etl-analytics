import csv
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    round,
    sum,
    to_date,
    when,
)


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_PATH = BASE_DIR / "data" / "sales.csv"
OUTPUT_DIR = BASE_DIR / "output"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Sales ETL Analytics")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_sales_data(spark: SparkSession):
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(DATA_PATH))
    )


def clean_and_transform_sales(raw_sales):
    return (
        raw_sales.dropna(subset=["order_id", "order_date", "region", "product"])
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("discount", col("discount").cast("double"))
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
        .withColumn("gross_sales", col("quantity") * col("unit_price"))
        .withColumn("discount_amount", col("gross_sales") * col("discount"))
        .withColumn("net_sales", round(col("gross_sales") - col("discount_amount"), 2))
        .withColumn("month", date_format(col("order_date"), "yyyy-MM"))
        .withColumn(
            "discount_level",
            when(col("discount") == 0, "No discount")
            .when(col("discount") <= 0.05, "Small discount")
            .when(col("discount") <= 0.10, "Medium discount")
            .otherwise("Large discount"),
        )
    )


def show_business_insights(sales):
    print("\nCleaned sales data")
    sales.select(
        "order_id",
        "order_date",
        "region",
        "product",
        "quantity",
        "net_sales",
        "discount_level",
    ).show(10, truncate=False)

    print("\nRevenue by region")
    sales.groupBy("region").agg(round(sum("net_sales"), 2).alias("total_revenue")).orderBy(
        col("total_revenue").desc()
    ).show(truncate=False)

    print("\nTop products by revenue")
    sales.groupBy("product").agg(round(sum("net_sales"), 2).alias("total_revenue")).orderBy(
        col("total_revenue").desc()
    ).show(5, truncate=False)

    print("\nMonthly sales trend")
    sales.groupBy("month").agg(round(sum("net_sales"), 2).alias("total_revenue")).orderBy(
        "month"
    ).show(truncate=False)

    print("\nOrders with large discounts")
    sales.filter(col("discount_level") == "Large discount").select(
        "order_id", "customer", "product", "discount", "net_sales"
    ).show(truncate=False)


def write_dataframe_as_single_csv(dataframe, output_file):
    if output_file.exists():
        if output_file.is_dir():
            shutil.rmtree(output_file)
        else:
            output_file.unlink()

    output_file.parent.mkdir(parents=True, exist_ok=True)
    columns = dataframe.columns

    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        for row in dataframe.collect():
            writer.writerow([row[column] for column in columns])


def save_outputs(sales):
    cleaned_path = OUTPUT_DIR / "cleaned_sales_parquet"
    regional_path = OUTPUT_DIR / "revenue_by_region_csv"
    fallback_cleaned_path = OUTPUT_DIR / "cleaned_sales.csv"
    fallback_regional_path = OUTPUT_DIR / "revenue_by_region.csv"

    regional_revenue = (
        sales.groupBy("region")
        .agg(round(sum("net_sales"), 2).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    try:
        sales.write.mode("overwrite").parquet(str(cleaned_path))

        (
            regional_revenue.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(regional_path))
        )

        print(f"\nSaved cleaned Parquet data to: {cleaned_path}")
        print(f"Saved regional revenue CSV folder to: {regional_path}")
    except Exception as error:
        print("\nSpark could not write Hadoop-style output folders on this Windows setup.")
        print(f"Reason: {error.__class__.__name__}")
        print("Saving standard single CSV files instead.")

        write_dataframe_as_single_csv(sales, fallback_cleaned_path)
        write_dataframe_as_single_csv(regional_revenue, fallback_regional_path)

        print(f"Saved cleaned sales CSV to: {fallback_cleaned_path}")
        print(f"Saved regional revenue CSV to: {fallback_regional_path}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        raw_sales = load_sales_data(spark)
        sales = clean_and_transform_sales(raw_sales)

        show_business_insights(sales)
        save_outputs(sales)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
