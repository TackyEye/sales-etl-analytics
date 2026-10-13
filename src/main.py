import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    date_format,
    desc,
    lit,
    round,
    sum,
    to_date,
    when,
)
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the sales ETL analytics pipeline.")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_DATA_PATH,
        help="Path to the raw sales CSV file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Folder where pipeline outputs will be written.",
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Sales ETL Analytics")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_sales_data(spark: SparkSession, data_path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(SALES_SCHEMA)
        .csv(str(data_path))
    )


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


def show_pipeline_results(sales: DataFrame, quality_report: DataFrame, kpis: dict[str, DataFrame]):
    print("\nData quality report")
    quality_report.show(truncate=False)

    print("\nCleaned sales preview")
    sales.select(
        "order_id",
        "order_date",
        "region",
        "product",
        "quantity",
        "net_sales",
        "order_size",
        "discount_level",
    ).show(10, truncate=False)

    for title, dataframe in kpis.items():
        print(f"\n{title.replace('_', ' ').title()}")
        dataframe.show(truncate=False)


def write_dataframe_as_single_csv(dataframe: DataFrame, output_file: Path):
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


def write_json_file(output_file: Path, data: dict):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def clear_generated_outputs(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    for path in output_dir.iterdir():
        if path.name == ".gitkeep":
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def save_outputs(sales: DataFrame, quality_report: DataFrame, kpis: dict[str, DataFrame], output_dir: Path):
    clear_generated_outputs(output_dir)

    try:
        sales.write.mode("overwrite").parquet(str(output_dir / "cleaned_sales_parquet"))

        for name, dataframe in kpis.items():
            (
                dataframe.coalesce(1)
                .write.mode("overwrite")
                .option("header", True)
                .csv(str(output_dir / name))
            )
        quality_report.coalesce(1).write.mode("overwrite").option("header", True).csv(
            str(output_dir / "data_quality_report")
        )

        print(f"\nSaved Spark output folders to: {output_dir}")
    except Exception as error:
        print("\nSpark could not write Hadoop-style output folders on this Windows setup.")
        print(f"Reason: {error.__class__.__name__}")
        print("Saving standard single CSV files instead.")

        clear_generated_outputs(output_dir)
        write_dataframe_as_single_csv(sales, output_dir / "cleaned_sales.csv")
        write_dataframe_as_single_csv(quality_report, output_dir / "data_quality_report.csv")
        for name, dataframe in kpis.items():
            write_dataframe_as_single_csv(dataframe, output_dir / f"{name}.csv")

        print(f"Saved CSV outputs to: {output_dir}")

    quality_row = quality_report.first().asDict()
    summary = {
        "pipeline": "sales-etl-analytics",
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "input_rows": quality_row["total_rows"],
        "cleaned_rows": sales.count(),
        "unique_orders": quality_row["unique_orders"],
        "outputs": sorted([path.name for path in output_dir.iterdir() if path.name != ".gitkeep"]),
    }
    write_json_file(output_dir / "run_summary.json", summary)


def main():
    args = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        raw_sales = load_sales_data(spark, args.input)
        quality_report = build_quality_report(raw_sales)
        sales = clean_and_transform_sales(raw_sales)
        kpis = build_kpis(sales)

        show_pipeline_results(sales, quality_report, kpis)
        save_outputs(sales, quality_report, kpis, args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
