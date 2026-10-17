import os
import json
import logging
import requests
from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

API_URL = "https://api.exchangerate-api.com/v4/latest/USD"
CONTAINER_NAME = "raw"
BLOB_PATH = "reference/exchange_rates.json"


def upload_json() -> None:
    account_name = os.getenv("account_name")
    account_key = os.getenv("account_key")

    if not account_name or not account_key:
        raise ValueError("Variables d'environnement 'account_name' ou 'account_key' manquantes.")

    # 1. Appel de l'API
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    data = response.json()
    logger.info(f"{len(data['rates'])} devises récupérées depuis l'API.")

    # 2. Connexion à Azure
    container_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    ).get_container_client(CONTAINER_NAME)

    # 3. Envoi du JSON dans raw/reference/exchange_rates.json
    blob_client = container_client.get_blob_client(BLOB_PATH)
    blob_client.upload_blob(response.text, overwrite=True)

    logger.info(f"Fichier envoyé dans : {CONTAINER_NAME}/{BLOB_PATH}")


if __name__ == "__main__":
    upload_json()