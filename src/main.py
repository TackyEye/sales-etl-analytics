import argparse
from pathlib import Path

from config import DEFAULT_DATA_PATH, DEFAULT_OUTPUT_DIR
from extract import load_sales_data
from kpis import build_kpis
from quality import build_quality_report
from reporting import show_pipeline_results
from spark_session import create_spark_session
from transform import clean_and_transform_sales
from writers import save_outputs


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


def run_pipeline(input_path: Path, output_dir: Path):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        raw_sales = load_sales_data(spark, input_path)
        quality_report = build_quality_report(raw_sales)
        sales = clean_and_transform_sales(raw_sales)
        kpis = build_kpis(sales)

        show_pipeline_results(sales, quality_report, kpis)
        save_outputs(sales, quality_report, kpis, output_dir)
    finally:
        spark.stop()


def main():
    args = parse_args()
    run_pipeline(args.input, args.output)


if __name__ == "__main__":
    main()
