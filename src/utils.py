import os
from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

account_name = os.getenv("account_name")
account_key = os.getenv("account_key")


# Initialisation et listing des conteneurs
def list_containers():
    account_name = os.getenv("account_name")
    account_key = os.getenv("account_key")
    client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    )
    print("Liste des conteneurs dans le compte de stockage :")
    for container in client.list_containers():
        print(f" - {container.name}")


# Nettoyage de la table customers
def clean_customers(df: DataFrame) -> DataFrame:
    df_customers = (
        df.withColumn("company_name", F.trim(F.col("company_name")))
          .withColumn("contact_name", F.initcap(F.trim(F.col("contact_name"))))
          .withColumn("country", F.upper(F.trim(F.col("country"))))
          .withColumn("contact_title", F.trim(F.col("contact_title")))
          .withColumn("address", F.trim(F.col("address")))
          .withColumn("city", F.trim(F.col("city")))
          .withColumn("region", F.trim(F.col("region")))
          .dropDuplicates(["customer_id"])
    )
    
    return df_customers


# Nettoyage de la table orders
def clean_orders(df: DataFrame) -> DataFrame:
    df_orders = (
        df.withColumns({
            "order_date": F.col("order_date").cast("date"),
            "shipped_date": F.col("shipped_date").cast("date"),
            "required_date": F.col("required_date").cast("date"),
            "freight": F.col("freight").cast("decimal(10,2)"),
            "is_shipped": F.col("shipped_date").isNotNull()
        })
        .withColumnRenamed("ship_via", "shipper_id")
        .dropna(subset=["shipped_date"])
    )
    
    return df_orders


# Nettoyage de la table order_details
def clean_order_details(df: DataFrame) -> DataFrame:
    df_order_details = (
        df.withColumns({
            "unit_price": F.col("unit_price").cast("decimal(10,2)"),
            "quantity": F.col("quantity").cast("integer"),
            "discount": F.col("discount").cast("decimal(10,2)")
        })
        .withColumnsRenamed({
            "unit_price": "prix_unitaire",
            "quantity": "quantite"
        })
    )
    
    return df_order_details


# Ajout de la colonne sous_total à la table order_details
def add_sous_total(df: DataFrame) -> DataFrame:
    df_order_details = (
        df.withColumn(
            "sous_total", 
            F.round(F.col("prix_unitaire") * F.col("quantite") * (1 - F.col("discount")),2)
        )
    )

    return df_order_details


# Nettoyage de la table employees
def clean_employees(df: DataFrame) -> DataFrame:
    df_employees = (
        df.select(
            F.col("employee_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("title"),
            F.col("hire_date"),
            F.col("city"),
            F.col("country")
        )
        .withColumns({
            "first_name": F.initcap(F.trim(F.col("first_name"))),
            "last_name": F.initcap(F.trim(F.col("last_name"))),
            "hire_date": F.col("hire_date").cast("date"),
            "city": F.trim(F.col("city")),
            "country": F.upper(F.trim(F.col("country")))
        })
        .withColumn(
            "full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
        )
    )

    return df_employees


# Nettoyage de la table products
def clean_products(df: DataFrame) -> DataFrame:
    df_products = (
        df.withColumns({
            "unit_price": F.col("unit_price").cast("decimal(10,2)"),
            "en_stock": F.col("units_in_stock") > 0
        })
    )

    return df_products