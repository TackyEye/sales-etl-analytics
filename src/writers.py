import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame


def clear_generated_outputs(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    for path in output_dir.iterdir():
        if path.name == ".gitkeep":
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


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
