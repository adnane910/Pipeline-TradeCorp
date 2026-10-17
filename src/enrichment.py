from pyspark.sql import DataFrame
from pyspark.sql.types import MapType, StringType, DoubleType
import pyspark.sql.functions as F

def enrich_with_currency(
    df_orders_enriched: DataFrame, 
    df_country_currency: DataFrame, 
    df_exchange_rates: DataFrame
) -> DataFrame:

    # 1. Jointure entre la table pays et la table devise
    df_orders_enriched = df_orders_enriched.join(
        df_country_currency,
        df_orders_enriched["customer_country"] == df_country_currency["country"],
        how="left"
    )

    
    # 2. Jointure croisée avec la ligne unique de taux de change
    df_rates_map = df_exchange_rates.select(
        F.from_json(
            F.to_json(F.col("rates")),
            MapType(StringType(), DoubleType())
        ).alias("rates")
    )
    df_orders_enriched = df_orders_enriched.crossJoin(df_rates_map)

    
    # 3. Extraction du taux dynamique et calcul du sous-total local
    df_orders_enriched = (
        df_orders_enriched
        .withColumn("taux", F.element_at(F.col("rates"), F.col("currency")))
        .withColumn("sous_total_local", F.round(F.col("sous_total") * F.col("taux"), 2))
        .drop("rates", "taux", "country")
    )
    
    return df_orders_enriched