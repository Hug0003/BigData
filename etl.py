"""
ETL : MinIO (données brutes) → PostgreSQL (données structurées)

Lit le dernier snapshot OpenSky depuis MinIO, enrichit chaque avion avec :
  - origin_country      : pays d'immatriculation (champ natif OpenSky)
  - current_country     : pays survolé OU océan/mer survolé(e)

Détection terre/mer :
  - geopandas + Natural Earth (polygones pays, résolution 110m) → point-in-polygon.
  - Si le point n'est dans aucun polygone → océan → _get_ocean_name().
  - La jointure spatiale (R-tree) traite les 5000+ avions en une seule passe.

Usage:
    python etl.py
"""
import os
import logging
from datetime import datetime, timezone

import geopandas as gpd
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

# Polygones pays chargés une seule fois (Natural Earth 110m, inclus dans geodatasets)
_WORLD_GDF: gpd.GeoDataFrame | None = None


_NE_COUNTRIES_URL = (
    "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
)


def _get_world() -> gpd.GeoDataFrame:
    """Charge les polygones pays Natural Earth 110m (téléchargé et mis en cache par pooch)."""
    global _WORLD_GDF
    if _WORLD_GDF is None:
        import pooch
        paths = pooch.retrieve(
            url=_NE_COUNTRIES_URL,
            known_hash=None,
            processor=pooch.Unzip(),
        )
        shp = next(p for p in paths if p.endswith(".shp"))
        world = gpd.read_file(shp)
        _WORLD_GDF = world[["NAME", "ISO_A2", "geometry"]].rename(
            columns={"NAME": "country_name", "ISO_A2": "country_code"}
        )
        logger.info("Polygones pays chargés : %d pays", len(_WORLD_GDF))
    return _WORLD_GDF


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


def _get_ocean_name(lat: float, lon: float) -> str:
    """Retourne le nom de l'océan ou de la mer pour un point en dehors des terres."""
    if lon > 180:
        lon -= 360

    if lat > 66.5:
        return "Arctic Ocean"
    if lat < -60:
        return "Southern Ocean"

    if 30 <= lat <= 47 and 5 <= lon <= 37:
        return "Mediterranean Sea"
    if 10 <= lat <= 23 and -87 <= lon <= -60:
        return "Caribbean Sea"
    if 22 <= lat <= 32 and 32 <= lon <= 43:
        return "Red Sea"
    if 23 <= lat <= 30 and 48 <= lon <= 60:
        return "Persian Gulf"
    if 0 <= lat <= 22 and 50 <= lon <= 78:
        return "Arabian Sea"
    if 5 <= lat <= 23 and 80 <= lon <= 100:
        return "Bay of Bengal"
    if 50 <= lat <= 66 and -10 <= lon <= 30:
        return "North Sea / Baltic Sea"

    if 20 <= lon <= 120 and lat < 30:
        return "Indian Ocean"
    if lon >= 120 or lon <= -70:
        return "Pacific Ocean"
    if -70 <= lon <= 20:
        return "Atlantic Ocean"

    return "Ocean"


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
    Détermine le pays ou l'océan survolé pour chaque avion via point-in-polygon.

    1. Construit un GeoDataFrame de points (positions avions).
    2. Jointure spatiale avec les polygones pays Natural Earth (R-tree).
    3. Les avions sans pays (en mer) reçoivent le nom de l'océan via coordonnées.
    """
    logger.info("Chargement des polygones pays (Natural Earth)...")
    world = _get_world()

    logger.info("Détection pays/océan pour %d avions (point-in-polygon)...", len(df))
    aircraft_gdf = gpd.GeoDataFrame(
        df.reset_index(drop=True),
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326",
    )

    joined = gpd.sjoin(
        aircraft_gdf,
        world.set_crs("EPSG:4326", allow_override=True),
        how="left",
        predicate="within",
    )

    # En cas de doublon (avion dans plusieurs polygones imbriqués), garder le premier
    joined = joined[~joined.index.duplicated(keep="first")]

    land_count = joined["country_name"].notna().sum()
    sea_count = joined["country_name"].isna().sum()
    logger.info("  Sur terre : %d  |  En mer : %d", land_count, sea_count)

    df["current_country"] = joined["country_name"].where(
        joined["country_name"].notna(),
        other=df.apply(
            lambda row: _get_ocean_name(row["latitude"], row["longitude"]), axis=1
        ),
    ).values

    # Ne conserver que les codes ISO-2 valides (exactement 2 lettres)
    # Natural Earth retourne "-99" pour les territoires sans code officiel
    raw_codes = joined["country_code"].where(joined["country_code"].notna(), other="").values
    df["current_country_code"] = [
        c if (isinstance(c, str) and len(c) == 2 and c.isalpha()) else ""
        for c in raw_codes
    ]

    return df


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
