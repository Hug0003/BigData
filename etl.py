"""
ETL : MinIO (données brutes) → PostgreSQL (données structurées)

Lit le dernier snapshot OpenSky depuis MinIO, enrichit chaque avion avec :
  - origin_country      : pays d'immatriculation (champ natif OpenSky)
  - current_country     : pays survolé, dérivé des coordonnées GPS en offline
                          (GeoNames via reverse_geocoder, pas d'appel API)
  - destination_country : NULL pour l'instant (nécessite AeroAPI)

Usage:
    python etl.py
"""
import os
import logging
from datetime import datetime, timezone

import pycountry
import reverse_geocoder as rg
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.minio_storage import MinioStorage

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Colonnes retournées par l'API OpenSky (état d'avion, 17 champs)
OPENSKY_COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]

# Colonnes à charger dans PostgreSQL
DB_COLUMNS = [
    "icao24", "callsign", "origin_country",
    "current_country", "current_country_code",
    "longitude", "latitude", "baro_altitude", "geo_altitude",
    "on_ground", "velocity", "true_track", "vertical_rate",
    "squawk", "position_source", "data_timestamp",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_storage() -> MinioStorage:
    return MinioStorage(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        bucket_name=os.getenv("MINIO_BUCKET_NAME", "raw-api-data"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def _make_engine():
    url = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'admin')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'admin123')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'bigdata_db')}"
    )
    return create_engine(url)


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_latest_opensky(storage: MinioStorage) -> dict:
    """Retourne le JSON du snapshot OpenSky le plus récent depuis MinIO."""
    objects = list(
        storage.client.list_objects(storage.bucket_name, prefix="opensky_states/", recursive=True)
    )
    if not objects:
        raise FileNotFoundError("Aucun fichier opensky_states/ trouvé dans MinIO. Lancez d'abord main.py.")

    latest = sorted(objects, key=lambda o: o.last_modified)[-1]
    logger.info("Chargement depuis MinIO : %s", latest.object_name)
    return storage.get_json(latest.object_name)


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform(raw_data: dict) -> pd.DataFrame:
    """Parse, filtre et enrichit le snapshot OpenSky."""
    states = raw_data.get("states", [])
    data_ts = datetime.fromtimestamp(raw_data["time"], tz=timezone.utc)
    logger.info("Snapshot OpenSky horodaté : %s (%d avions au total)", data_ts, len(states))

    df = pd.DataFrame(states, columns=OPENSKY_COLUMNS)
    df["callsign"] = df["callsign"].str.strip()
    df["data_timestamp"] = data_ts

    # Ne garder que les avions en vol avec des coordonnées GPS valides
    df = df[
        (df["on_ground"] == False)
        & df["longitude"].notna()
        & df["latitude"].notna()
    ].copy()
    logger.info("Avions en vol avec GPS valide : %d", len(df))

    df = _add_current_country(df)
    return df


def _add_current_country(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reverse-geocoding en masse via GeoNames (offline, pas d'appel API).
    reverse_geocoder.search() utilise un KD-tree sur les données GeoNames :
    renvoie le code pays ISO-3166-1 alpha-2 le plus proche de chaque point.
    Les avions au-dessus des océans reçoivent le code du pays côtier le plus proche.
    """
    logger.info("Reverse geocoding offline de %d positions (GeoNames)...", len(df))
    coords = list(zip(df["latitude"], df["longitude"]))
    results = rg.search(coords, verbose=False)

    df["current_country_code"] = [r["cc"] for r in results]
    df["current_country"] = df["current_country_code"].map(_cc_to_name)
    logger.info("Reverse geocoding terminé.")
    return df


def _cc_to_name(cc: str) -> str:
    """Convertit un code ISO-2 (ex: 'FR') en nom complet (ex: 'France')."""
    try:
        return pycountry.countries.get(alpha_2=cc).name
    except AttributeError:
        return cc


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load(df: pd.DataFrame, engine) -> None:
    """Insère les données transformées dans la table aircraft_states."""
    df_load = df[DB_COLUMNS].copy()
    df_load.to_sql("aircraft_states", engine, if_exists="append", index=False)
    logger.info("Inséré %d lignes dans aircraft_states.", len(df_load))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_etl():
    logger.info("=== Démarrage ETL MinIO → PostgreSQL ===")

    storage = _make_storage()
    engine = _make_engine()

    raw_data = extract_latest_opensky(storage)
    df = transform(raw_data)
    load(df, engine)

    logger.info("=== ETL terminé avec succès ===")


if __name__ == "__main__":
    run_etl()
