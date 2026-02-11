# -*- coding: utf-8 -*-
import os
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import year, col


# Initialisation de la session Spark avec le connecteur MySQL
spark = (
    SparkSession.builder
    .appName("SolarWeatherFeeder")
    .getOrCreate()
)

# Récupération des données de production solaire depuis MySQL
df_solar = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://host.docker.internal:3306/solar_energy")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "solar_energy_production")
    .option("user", "spark")
    .option("password", "sparkpwd")
    .load()
)

# Récupération des données météorologiques depuis le fichier JSON en local

weather_input_path = "file:///source/open_meteo_hourly.json"
df_weather = spark.read.json(weather_input_path)

# Enrichissement des données avec des métadonnées d'ingestion
now = datetime.now()
df_solar_bronze = df_solar.withColumn("ingestion_date", F.lit(now)) \
                          .withColumn("source", F.lit("mysql_solar")) \
                          .withColumn("year", year(col("date_production")))

df_weather_bronze = df_weather.withColumn("ingestion_date", F.lit(now)) \
                             .withColumn("source", F.lit("open_meteo_json")) \
                             .withColumn("year", year(col("date")))

# Ecriture dans HDFS avec partitionnement temporel à l'échelle de l'année
hdfs_base_path = "hdfs://namenode:9000/bronze"

# Stockage Production Solaire
df_solar_bronze.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(hdfs_base_path + "/solar/")

# Stockage Météo
df_weather_bronze.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(hdfs_base_path + "/weather/")

spark.stop()