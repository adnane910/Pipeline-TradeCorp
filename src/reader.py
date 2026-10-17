import os
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession, DataFrame


# Téléchargement des fichiers spécifiés depuis Azure vers le dossier local.
def download_raw_files(container_name: str, files: list[str], target_dir: str) -> None:
    
    account_name = os.getenv("account_name")
    account_key = os.getenv("account_key")

    if not account_name or not account_key:
        raise ValueError("Variables d'environnement 'account_name' ou 'account_key' manquantes.")

    client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    )
    container_client = client.get_container_client(container_name)

    for filename in files:
        local_path = os.path.join(target_dir, filename)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        blob_client = container_client.get_blob_client(filename)
        with open(local_path, "wb") as f:
            f.write(blob_client.download_blob().readall())


def get_spark_session() -> SparkSession:
    return SparkSession.builder.appName("TradeCorp_Ingestion").getOrCreate()

# Chargement des fichiers CSV téléchargés en DataFrames PySpark.
def read_csv_files(spark: SparkSession, data_dir: str, files: list[str]) -> dict[str, DataFrame]:
    return {
        f.replace(".csv", ""): spark.read.csv(
            os.path.join(data_dir, f), header=True, inferSchema=True
        )
        for f in files
        
    }

# Téléchargement et chargement des fichiers dans reference (devise et taux de change)
def read_reference(spark: SparkSession, target_dir: str) -> dict[str, DataFrame]:
    files_to_download = [
        "reference/country_currency.csv",
        "reference/exchange_rates.json",
    ]
    download_raw_files("raw", files_to_download, target_dir)

    path_csv = os.path.join(target_dir, "reference", "country_currency.csv")
    path_json = os.path.join(target_dir, "reference", "exchange_rates.json")

    return {
        "country_currency": spark.read.csv(path_csv, header=True, inferSchema=True),
        "exchange_rates": spark.read.json(path_json),
    }


# Téléchargement et chargement des tables nécessaires à build_enriched().
def read_business_data(spark: SparkSession, target_dir: str) -> dict[str, DataFrame]:
    
    business_files = [
        "customers.csv",
        "orders.csv",
        "order_details.csv",
        "products.csv",
        "employees.csv",
        "categories.csv",
        "shippers.csv",
    ]
    download_raw_files("raw", business_files, target_dir)
    return read_csv_files(spark, target_dir, business_files)


if __name__ == "__main__":
    RAW_DIR = "/home/jovyan/data/raw"
    STAGING_DIR = "/home/jovyan/data/staging"

    spark = get_spark_session()

    business_dfs = read_business_data(spark, RAW_DIR)
    reference_dfs = read_reference(spark, RAW_DIR)

    # Écriture de chaque table en Parquet pour que transformer.py puisse les relire
    for name, df in {**business_dfs, **reference_dfs}.items():
        df.write.mode("overwrite").parquet(os.path.join(STAGING_DIR, name))

    print(f"Tables téléchargées et écrites dans {STAGING_DIR} : {list(business_dfs) + list(reference_dfs)}")
    spark.stop()