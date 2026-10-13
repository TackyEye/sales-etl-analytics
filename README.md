# Sales ETL Analytics With PySpark

This project is a small PySpark data pipeline that turns raw order data into cleaned sales records, data quality checks, and business-ready KPI outputs.

1. Read raw CSV data
2. Apply an explicit schema
3. Run data quality checks
4. Clean and enrich records
5. Build KPI tables
6. Save outputs for analysis

## Project Structure

```text
sales-etl-analytics/
  data/
    sales.csv
  output/
    .gitkeep
  src/
    config.py
    extract.py
    kpis.py
    main.py
    quality.py
    reporting.py
    spark_session.py
    transform.py
    writers.py
  .gitignore
  README.md
  requirements.txt
```

## What The Dataset Contains

The sample file `data/sales.csv` contains fictional sales orders with:

- order dates
- customers
- regions
- products
- categories
- quantities
- prices
- discounts

## Setup

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the project dependencies:

```powershell
pip install -r requirements.txt
```

## How To Run It

From the project folder, run:

```powershell
spark-submit .\src\main.py
```

You can also run it with Python:

```powershell
python .\src\main.py
```

You can pass custom input and output paths:

```powershell
spark-submit .\src\main.py --input .\data\sales.csv --output .\output
```

## What The Script Does

The pipeline reads the CSV file into a Spark DataFrame, then:

- applies a defined schema instead of guessing column types
- checks row counts, unique orders, missing values, and invalid numeric values
- removes rows with missing or invalid important values
- converts `order_date` into a real date column
- calculates `gross_sales`
- calculates `discount_amount`
- calculates `net_sales`
- creates a `month` column for trend analysis
- labels each row by discount size
- labels each row by order size

## Code Organization

- `main.py`: command-line entry point and pipeline orchestration
- `config.py`: default paths and the sales schema
- `spark_session.py`: Spark session creation
- `extract.py`: CSV loading
- `quality.py`: data quality checks
- `transform.py`: cleaning and feature creation
- `kpis.py`: analytics tables
- `reporting.py`: console previews
- `writers.py`: output writing and run summary creation

## Questions This Project Answers

When you run the script, it prints:

- a data quality report
- a preview of cleaned sales data
- revenue by region
- revenue by category
- product performance
- monthly sales trend
- top customers by revenue
- orders with large discounts

## Output Files

After running, the `output` folder will contain:

- `cleaned_sales_parquet/`: cleaned data in Parquet format
- KPI output folders such as `revenue_by_region/`, `revenue_by_category/`, `product_performance/`, and `monthly_sales_trend/`
- `data_quality_report/`: summary of input quality checks
- `run_summary.json`: high-level metadata about the pipeline run

Spark writes CSV and Parquet outputs as folders, not single files. That is normal.

On some Windows machines, Spark may not be able to create Hadoop-style output folders unless Hadoop `winutils.exe` is configured. If that happens, this project automatically saves standard single CSV files instead:

- `cleaned_sales.csv`
- `data_quality_report.csv`
- `revenue_by_region.csv`
- `revenue_by_category.csv`
- `product_performance.csv`
- `monthly_sales_trend.csv`
- `top_customers.csv`
- `large_discount_orders.csv`

The generated output files are ignored by Git so the repository stays focused on source code and sample input data.

## Challenge Ideas

Try changing `src/main.py` to answer these:

- Which category has the highest revenue?
- Who are the top 5 customers by spending?
- Which month had the most orders?
- What is the average discount by region?
- Add another data quality check for duplicate order IDs
- Add a new KPI for product performance by region

## Key PySpark Concepts Practiced

- `SparkSession`
- explicit schemas
- reading CSV files
- DataFrames
- `select`
- `filter`
- `withColumn`
- `groupBy`
- `agg`
- data quality checks
- command-line arguments
- sorting with `orderBy`
- writing Parquet and CSV files
