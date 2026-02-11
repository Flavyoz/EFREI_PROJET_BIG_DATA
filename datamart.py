# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, sum, avg, count, year, month, col



# initialisation de la session Spark
spark = SparkSession.builder \
    .appName("SilverToGoldProcessor") \
    .getOrCreate()

SILVER_BASE = "hdfs://namenode:9000/silver"

solar = spark.read.parquet(SILVER_BASE + "/solar")
weather = spark.read.parquet(SILVER_BASE + "/weather")


# on agrège les données de production solaire à l'échelle journalière pour faciliter les analyses
# la colonne timestamp_production est convertie en date pour faire l'agrégation à l'échelle journalière,
# puis on ajoute des colonnes d'année et de mois pour faciliter les analyses temporelles.
# Les métriques d'énergie totale, moyenne et nombre de records sont donc calculées pour chaque jour.
solar_daily = solar \
    .withColumn("date", to_date("timestamp_production")) \
    .groupBy("date", "name") \
    .agg(
        sum("energy_production").alias("total_energy"),
        avg("energy_production").alias("avg_energy"),
        count("*").alias("nb_records")
    ) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date"))


# on agrège les données météo à l'échelle journalière pour faciliter les analyses
# Ici aussi on convertit le timestamp en date pour faire l'agrégation à l'échelle journalière, 
# puis on calcule des métriques comme la température moyenne, la couverture nuageuse moyenne 
# et les précipitations totales pour chaque jour.

weather_daily = weather \
    .withColumn("date", to_date("timestamp")) \
    .groupBy("date") \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("cloud_cover").alias("avg_cloud_cover"),
        sum("precipitation").alias("total_precipitation")
    )

# on joint les données solaires et météo à l'échelle journalière pour créer une table enrichie

solar_weather = solar_daily.join(
    weather_daily,
    on="date",
    how="inner"
)

# connexion à la base de données MySQL et écriture de la table finale
MYSQL_URL = "jdbc:mysql://mysql:3306/gold_solar_energy_weather_datamart?useSSL=false&allowPublicKeyRetrieval=true"
MYSQL_PROPERTIES = {
    "user": "gold_user",
    "password": "gold_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}


solar_daily.write \
    .mode("overwrite") \
    .jdbc(MYSQL_URL, "dm_solar_production", properties=MYSQL_PROPERTIES)

solar_weather.write \
    .mode("overwrite") \
    .jdbc(MYSQL_URL, "dm_solar_weather", properties=MYSQL_PROPERTIES)

spark.stop()