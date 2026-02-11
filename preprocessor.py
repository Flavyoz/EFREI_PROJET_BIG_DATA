# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    lit,
    concat_ws
)
from pyspark.sql.types import DoubleType, TimestampType
from datetime import datetime


# initialisation de la session Spark
spark = SparkSession.builder \
    .appName("BronzeToSilverProcessor") \
    .getOrCreate()


# on définit les différents paths pour les données et les logs
BRONZE_BASE = "hdfs://namenode:9000/bronze"
SILVER_BASE = "hdfs://namenode:9000/silver"
LOG_BASE = "hdfs://namenode:9000/silver/logs"

ingestion_date = datetime.now().strftime("%Y-%m-%d")


# lecture des données brutes depuis le Bronze
solar_df = spark.read.parquet(BRONZE_BASE + "/solar")
weather_df = spark.read.parquet(BRONZE_BASE + "/weather")


# on convertit les champs de date en timestamp et on drop les champs d'origine 
# afin de s'assurer d'avoir des types corrects
solar_df = solar_df \
    .withColumn("timestamp_production", to_timestamp(col("date_production"))) \
    .drop("date_production")

weather_df = weather_df \
    .withColumn("timestamp", to_timestamp(col("date"))) \
    .drop("date")


# on convertit les champs numériques en double pour faciliter les règles de validation
# le type double est intéressant pour les données de production solaire et les métriques 
# météo qui peuvent être à virgule
solar_df = solar_df \
    .withColumn("energy_production", col("kWH").cast(DoubleType())) \
    .drop("kWH")

weather_df = weather_df \
    .withColumn("cloud_cover", col("cloud_cover").cast(DoubleType())) \
    .withColumn("temperature", col("temperature_2m").cast(DoubleType())) \
    .withColumn("precipitation", col("precipitation").cast(DoubleType())) \
    .drop("temperature_2m")


# Règles de validation pour les données solaires et météo

### Données de production solaire :
# - La production d'énergie doit être positive ou nulle
# - Le timestamp de production doit être présent
# - La date d'installation doit être antérieure au timestamp de production

# D'abord on repère les données valides selon les règles, 
# puis on soustrait ce DataFrame du DataFrame original pour obtenir les données invalides.

solar_valid = solar_df.filter(
    (col("energy_production") >= 0) &
    (col("timestamp_production").isNotNull()) &
    (col("date_installation") < col("timestamp_production"))
)

solar_invalid = solar_df.subtract(solar_valid) \
    .withColumn("error_reason", lit("Invalid solar production or timestamp")) \
    .withColumn("rejected_at", lit(ingestion_date))

solar_invalid_log = solar_invalid.withColumn(
    "log_line",
    concat_ws(
        " | ",
        col("rejected_at"),
        lit("SOLAR"),
        col("error_reason"),
        col("id").cast("string"),
        col("energy_production").cast("string"),
        col("timestamp_production").cast("string"),
        col("date_installation").cast("string")
    )
)



# Données météo :
# - La couverture nuageuse doit être entre 0 et 100%
# - La température doit être dans une plage réaliste (ex: -50 à 60°C)
# - Les précipitations doivent être positives ou nulles
# - Le timestamp doit être présent

# D'abord on repère les données valides selon les règles, 
# puis on soustrait ce DataFrame du DataFrame original pour obtenir les données invalides.

weather_valid = weather_df.filter(
    (col("cloud_cover").between(0, 100)) &
    (col("temperature").between(-50, 60)) &
    (col("precipitation") >= 0) &
    (col("timestamp").isNotNull())
)

weather_invalid = weather_df.subtract(weather_valid) \
    .withColumn("error_reason", lit("Invalid weather metrics or timestamp")) \
    .withColumn("rejected_at", lit(ingestion_date))


weather_invalid_log = weather_invalid.withColumn(
    "log_line",
    concat_ws(
        " | ",
        col("rejected_at"),
        lit("WEATHER"),
        col("error_reason"),
        col("timestamp").cast("string"),
        col("cloud_cover").cast("string"),
        col("temperature").cast("string"),
        col("precipitation").cast("string"),
    )
)


# on ajoute les colonnes de partitionnement (année et mois) pour faciliter les requêtes temporelles

solar_valid = solar_valid \
    .withColumn("year", year(col("timestamp_production"))) \
    .withColumn("month", month(col("timestamp_production")))

weather_valid = weather_valid \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp")))


# écriture des données validées dans la couche silver
# avec partitionnement par année et mois pour optimiser les requêtes temporelles
solar_valid.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(SILVER_BASE + "/solar")

weather_valid.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(SILVER_BASE + "/weather")


# écriture des données invalides dans un dossier de logs pour analyse et correction ultérieure
solar_invalid.write \
    .mode("append") \
    .parquet(LOG_BASE + "/solar")

solar_invalid_log.select("log_line") \
    .write \
    .mode("append") \
    .text(LOG_BASE + "/solar_log")


weather_invalid.write \
    .mode("append") \
    .parquet(LOG_BASE + "/weather")

weather_invalid_log.select("log_line") \
    .write \
    .mode("append") \
    .text(LOG_BASE + "/weather_log")


spark.stop()
