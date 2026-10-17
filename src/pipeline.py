import os
import logging
from dotenv import load_dotenv

from utils import list_containers
from reader import get_spark_session, download_raw_files, read_csv_files, read_reference
from transformer import build_enriched
from writer import write_to_parquet, upload_directory_to_blob, write_postgres  # AJOUT : write_postgres
from enrichment import enrich_with_currency

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv()

# Chemins locaux
LOCAL_RAW_DIR = "/home/jovyan/data/raw"
LOCAL_CLEAN_DIR = "/home/jovyan/data/clean/df_orders_enriched"

# Destination Azure
CLEAN_CONTAINER = "clean"
CLEAN_REMOTE_PREFIX = "df_orders_enriched"

# destination PostgreSQL
POSTGRES_TABLE = "public.orders_enriched"
POSTGRES_URL = "jdbc:postgresql://tradecorp_postgres_2:5432/tradecorp_db"
POSTGRES_PROPERTIES = {
    "user": "tradecorp",
    "password": "tradecorp",
    "driver": "org.postgresql.Driver",
}

FILES = [
    "customers.csv",
    "orders.csv",
    "order_details.csv",
    "products.csv",
    "employees.csv",
    "categories.csv",
    "shippers.csv"
]

def run_pipeline():
    spark = None
    try:
        # 0. Vérification de la connexion Azure
        logger.info("Vérification des conteneurs disponibles...")
        list_containers()

        # 1. Ingestion : téléchargement ADLS -> Local
        logger.info("Téléchargement des CSV métiers depuis le conteneur 'raw'...")
        download_raw_files(container_name="raw", files=FILES, target_dir=LOCAL_RAW_DIR)

        # 2. Lecture PySpark depuis le dossier local
        logger.info("Initialisation de Spark et lecture des fichiers localisés...")
        spark = get_spark_session()
        dataframes = read_csv_files(spark, LOCAL_RAW_DIR, FILES)

        # 3. Transformations & enrichissements
        logger.info("Calcul des transformations...")
        df_enriched = build_enriched(dataframes)

        # 4. Ingestion des référentiels & enrichissement devise/taux de change
        logger.info("Téléchargement et lecture des référentiels (pays/devises & taux)...")
        refs = read_reference(spark, LOCAL_RAW_DIR)

        logger.info("Enrichissement des commandes avec la devise et sous_total_local...")
        df_enriched = enrich_with_currency(
            df_orders_enriched=df_enriched,
            df_country_currency=refs["country_currency"],
            df_exchange_rates=refs["exchange_rates"]
        )

        # 5. Écriture Parquet en local
        logger.info("Écriture du Parquet en local...")
        write_to_parquet(df_enriched, LOCAL_CLEAN_DIR)

        # 6. Upload du Parquet vers ADLS clean
        logger.info("Upload du Parquet vers le conteneur 'clean'...")
        upload_directory_to_blob(LOCAL_CLEAN_DIR, CLEAN_CONTAINER, CLEAN_REMOTE_PREFIX)

        # 7.écriture dans PostgreSQL
        logger.info("Écriture dans PostgreSQL...")
        df_final = spark.read.parquet(LOCAL_CLEAN_DIR)
        write_postgres(df_final, POSTGRES_TABLE, POSTGRES_URL, POSTGRES_PROPERTIES, mode="overwrite")

        logger.info("Pipeline terminé avec succès.")

    except Exception as e:
        logger.error(f"Erreur durant l'exécution du pipeline : {str(e)}", exc_info=True)
        raise e

    finally:
        if spark is not None:
            logger.info("Fermeture de la SparkSession...")
            spark.stop()
            logger.info("SparkSession arrêtée.")

if __name__ == "__main__":
    run_pipeline()