from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

app = FastAPI(title="Solar Energy API")

DATABASE_URL = "mysql+pymysql://gold_user:gold_password@mysql:3306/gold_solar_energy_weather_datamart"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# test pour vérifier que ça fonctionne
@app.get("/")
def root():
    return {"message": "Solar Energy Datamart API"}



# endpoint pour récupérer les données de production solaire journalière
# default: int = Query(default=50, ge=1, le=500) permet de limiter le nombre de résultats retournés,
# avec une valeur par défaut de 50 et des limites de 1 à 500 pour éviter les requêtes trop lourdes.
@app.get("/production/daily")
def get_daily_production(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0)
):
    query = """
        SELECT * FROM dm_solar_production
        ORDER BY date DESC
        LIMIT :limit OFFSET :offset
    """

    with engine.connect() as connection:
        result = connection.execute(
            text(query),
            {"limit": limit, "offset": offset}
        )
        data = [dict(row) for row in result.mappings()]

    return {
        "limit": limit,
        "offset": offset,
        "count": len(data),
        "data": data
    }


# endpoint pour récupérer les données enrichies de production solaire et météo à l'échelle journalière
@app.get("/production/weather")
def get_enriched_data(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0)
):
    query = """
        SELECT * FROM dm_solar_weather
        ORDER BY date DESC
        LIMIT :limit OFFSET :offset
    """

    with engine.connect() as connection:
        result = connection.execute(
            text(query),
            {"limit": limit, "offset": offset}
        )
        data = [dict(row) for row in result.mappings()]

    return {
        "limit": limit,
        "offset": offset,
        "count": len(data),
        "data": data
    }


# endpoint pour récupérer les données de production solaire journalière pour une date spécifique
@app.get("/production/daily/{date}")
def get_daily_by_date(date: str):
    query = "SELECT * FROM dm_solar_production WHERE date = :date"

    with engine.connect() as connection:
        result = connection.execute(
            text(query),
            {"date": date}
        )
        data = [dict(row) for row in result.mappings()]

    return {
        "count": len(data),
        "data": data
    }
