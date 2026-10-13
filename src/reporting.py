from pyspark.sql import DataFrame


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
