import os
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from utils import (
    clean_customers,
    clean_orders,
    clean_order_details,
    add_sous_total,
    clean_products,
    clean_employees,
)

# Construit le DataFrame enrichi en joignant et nettoyant toutes les tables
def build_enriched(dataframes: dict) -> DataFrame:
    df_customers = clean_customers(dataframes["customers"])
    df_orders = clean_orders(dataframes["orders"])
    df_order_details = clean_order_details(dataframes["order_details"])
    df_order_details = add_sous_total(df_order_details)
    df_products = clean_products(dataframes["products"])
    df_employees = clean_employees(dataframes["employees"])
    df_categories = dataframes["categories"]
    df_shippers = dataframes["shippers"]

    df_products_enriched = (
        df_products
        .join(df_categories, on="category_id", how="left")
        .select(df_products["*"], df_categories["category_name"])
    )

    df_orders_enriched = (
        df_order_details
        .join(df_orders, on="order_id", how="inner")
        .join(df_products_enriched, on="product_id", how="left")
        .join(df_customers, on="customer_id", how="left")
        .join(df_employees, on="employee_id", how="left")
        .join(df_shippers, df_orders["shipper_id"] == df_shippers["shipper_id"], how="left")
    )

    df_orders_enriched = df_orders_enriched.select(
        F.col("order_id").cast("integer"),
        F.col("customer_id").cast("string"),
        F.col("employee_id").cast("integer"),
        F.col("product_id").cast("integer"),
        F.col("order_date").cast("date"),
        F.col("required_date").cast("date"),
        F.col("shipped_date").cast("date"),
        F.col("freight").cast("double"),
        F.col("is_shipped").cast("boolean"),
        F.col("prix_unitaire").cast("double"),
        F.col("quantite").cast("integer"),
        F.col("discount").cast("double"),
        F.col("sous_total").cast("double"),
        df_customers["company_name"].alias("customer_name"),
        df_customers["country"].alias("customer_country"),
        df_customers["city"].alias("customer_city"),
        df_products_enriched["product_name"],
        F.col("category_name"),
        F.col("en_stock").cast("boolean"),
        df_employees["full_name"],
        df_shippers["company_name"].alias("shipper_name"),
    )

    return df_orders_enriched


if __name__ == "__main__":
    STAGING_DIR = "/home/jovyan/data/staging"
    OUTPUT_DIR = "/home/jovyan/data/transformed"

    spark = SparkSession.builder.appName("transformer").getOrCreate()

    tables = ["customers", "orders", "order_details", "products", "employees", "categories", "shippers"]
    dataframes = {
        name: spark.read.parquet(os.path.join(STAGING_DIR, name)) for name in tables
    }

    df_result = build_enriched(dataframes)
    df_result.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"Résultat enrichi écrit dans {OUTPUT_DIR}")
    spark.stop()