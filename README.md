# EFREI_PROJET_BIG_DATA
### Développement d'une application permettant d’analyser l’impact des conditions météorologiques sur la production solaire dans la ville de Calgary, au Canada. L'application se base sur une architecture médaillon pour stocker et transformer les données, qui sont finalement disponibles via un dashboard et une API.
---

## Architecture de la pipeline
Le pipeline est divisé en trois couches logiques stockées sur HDFS et MySQL :
1. **Bronze** : Ingestion des données brutes depuis MySQL et des fichiers JSON.
2. **Silver** : Nettoyage, typage des données et filtrage des anomalies.
3. **Gold** : Agrégation journalière et jointure métier pour alimenter un Dashboard.
---

## Guide d'exécution

Exécutez les commandes suivantes dans l'ordre pour traiter la donnée à travers les différentes couches :

### 1. Ingestion (Couche Bronze)
Ce script récupère les données de production depuis la base relationnelle et la météo depuis un JSON local pour les stocker sur HDFS.
Les données sont stockées brutes, sans transformations, au format parquet. Elles peuvent être retrouvées dans le répertoire /bronze. L'arborescence peut être consultée à tout moment depuis http://localhost:9870/

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.mysql:mysql-connector-j:8.0.33 /app/feeder.py
```

### 2. Nettoyage (Couche Silver) :
Ce script applique des règles de validation (ex: température réaliste, production positive) et sépare les données valides des données corrompues (logs).

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --executor-cores 2 /app/preprocessor.py
```

### 3. Agrégation & Datamart (Couche Gold)
Ce script crée les tables finales agrégées par jour et par site, puis les exporte vers le MySQL "Gold" pour la visualisation.

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.mysql:mysql-connector-j:8.0.33 /app/datamart.py
```

### 4. Visualisation et accès aux données
Une fois le pipeline terminé, vous pouvez accéder aux résultats :
API : Rendez-vous sur http://localhost:8000/docs pour tester les endpoints FastAPI.
Dashboard : Fichier "Dasboard_final.twb" sur ce repo

## Monitoring Technique

Pour surveiller le bon déroulement des jobs Spark et l'état du cluster :

* **Spark Master :** http://localhost:8080
* **Spark UI (pendant l'exécution) :** http://localhost:4040
