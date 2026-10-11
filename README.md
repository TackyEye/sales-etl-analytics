# PySpark Project: Sales ETL and Analytics

This project shows how PySpark is used in a typical data engineering workflow:

1. Read raw CSV data
2. Clean and validate rows
3. Create useful calculated columns
4. Group data to answer business questions
5. Save cleaned data for future analysis

## Project Structure

```text
sales-etl-analytics/
  data/
    sales.csv
  output/
    .gitkeep
  src/
    main.py
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

## What The Script Does

The script reads the CSV file into a Spark DataFrame, then:

- removes rows with missing important values
- converts `order_date` into a real date column
- converts quantity, price, and discount into numeric columns
- calculates `gross_sales`
- calculates `discount_amount`
- calculates `net_sales`
- creates a `month` column for trend analysis
- labels each row by discount size

## Questions This Project Answers

When you run the script, it prints:

- a preview of cleaned sales data
- revenue by region
- top products by revenue
- monthly sales trend
- orders with large discounts

## Output Files

After running, the `output` folder will contain:

- `cleaned_sales_parquet/`: cleaned data in Parquet format
- `revenue_by_region_csv/`: regional revenue summary as CSV

Spark writes CSV and Parquet outputs as folders, not single files. That is normal.

On some Windows machines, Spark may not be able to create Hadoop-style output folders unless Hadoop `winutils.exe` is configured. If that happens, this project automatically saves standard single CSV files instead:

- `cleaned_sales.csv`
- `revenue_by_region.csv`

The generated output files are ignored by Git so the repository stays focused on source code and sample input data.

## Challenge Ideas

Try changing `src/main.py` to answer these:

- Which category has the highest revenue?
- Who are the top 5 customers by spending?
- Which month had the most orders?
- What is the average discount by region?
- Add a new column called `order_size` with values like `Small`, `Medium`, and `Large`

## Key PySpark Concepts Practiced

- `SparkSession`
- reading CSV files
- DataFrames
- `select`
- `filter`
- `withColumn`
- `groupBy`
- `agg`
- sorting with `orderBy`
- writing Parquet and CSV files
