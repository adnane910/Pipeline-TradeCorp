import os
import glob
import logging
from typing import Dict
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Écrit un DataFrame Spark en format Parquet en local
def write_to_parquet(df: DataFrame, local_path: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).parquet(local_path)


# Envoie un DataFrame Spark vers PostgreSQL
def write_postgres(
    df: DataFrame,
    table_name: str,
    jdbc_url: str,
    properties: Dict[str, str],
    mode: str = "append",
) -> None:
    df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=properties)
    logger.info(f"Écriture PostgreSQL terminée : table='{table_name}', mode='{mode}'")


# Uploade tous les fichiers d'un dossier local vers Azure Blob Storage
# Supprime les anciens fichiers avant l'upload
def upload_directory_to_blob(local_dir: str, container_name: str, remote_prefix: str) -> None:
    account_name = os.getenv("account_name")
    account_key = os.getenv("account_key")

    container_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    ).get_container_client(container_name)

    for blob in container_client.list_blobs(name_starts_with=remote_prefix):
        container_client.delete_blob(blob.name)

    for local_file in glob.glob(os.path.join(local_dir, "*")):
        filename = os.path.basename(local_file)
        with open(local_file, "rb") as f:
            container_client.upload_blob(f"{remote_prefix}/{filename}", f, overwrite=True)

    logger.info(f"Upload terminé vers ADLS : container='{container_name}', prefix='{remote_prefix}'")


if __name__ == "__main__":
    INPUT_DIR = "/home/jovyan/data/transformed"

    spark = (
        SparkSession.builder.appName("writer")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    df = spark.read.parquet(INPUT_DIR)

    # Envoi vers Azure
    upload_directory_to_blob(
        local_dir=INPUT_DIR,
        container_name="clean",
        remote_prefix="orders_enriched",
    )

    # Envoi vers PostgreSQL
    write_postgres(
        df,
        "public.orders_enriched",
        "jdbc:postgresql://postgres:5432/tradecorp_db",
        {
            "user": "tradecorp",
            "password": "tradecorp",
            "driver": "org.postgresql.Driver",
        },
        mode="overwrite",
    )

    spark.stop()