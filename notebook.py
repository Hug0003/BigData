# %% [markdown]
# # Analyse Exploratoire des Données (EDA) de l'API OpenSky
# Ce notebook (format script interactif) permet de récupérer les données brutes depuis MinIO, 
# de les traiter avec Pandas, puis de les insérer dans PostgreSQL.
# 
# Si vous utilisez VSCode, vous pouvez cliquer sur "Run Cell" au-dessus des `# %%`

# %%
import os
import pandas as pd
from dotenv import load_dotenv
from src.minio_storage import MinioStorage
from sqlalchemy import create_engine

# Charger les variables d'environnement
load_dotenv()

# %% [markdown]
# ## 1. Connexion à MinIO et récupération des données

# %%
storage = MinioStorage(
    endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    bucket_name=os.getenv("MINIO_BUCKET_NAME", "raw-api-data"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
)

# On liste tous les fichiers du dossier opensky_states/
objects = storage.client.list_objects(storage.bucket_name, prefix="opensky_states/", recursive=True)

# Récupérons le fichier le plus récent
latest_object = None
for obj in objects:
    latest_object = obj.object_name

print(f"Dernier fichier trouvé : {latest_object}")

if latest_object:
    # On télécharge et on parse le JSON
    raw_data = storage.get_json(latest_object)
    print("Données récupérées avec succès !")
    print(f"Timestamp des données : {raw_data.get('time')}")
else:
    print("Aucun fichier trouvé. Avez-vous exécuté le main.py ?")

# %% [markdown]
# ## 2. Transformation avec Pandas
# Les "états" de base (states) dans OpenSky sont une liste de valeurs :
# `[icao24, callsign, origin_country, time_position, last_contact, longitude, latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source]`

# %%
if latest_object and "states" in raw_data and raw_data["states"]:
    columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact", 
        "longitude", "latitude", "baro_altitude", "on_ground", "velocity", 
        "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk", 
        "spi", "position_source"
    ]
    
    # Création du DataFrame
    df = pd.DataFrame(raw_data["states"], columns=columns)
    
    # Nettoyage de base : suppression des espaces inutiles dans les noms de vols
    df["callsign"] = df["callsign"].str.strip()
    
    # Filtrer uniquement les avions en vol avec des coordonnées valides
    df_clean = df[
        (df["on_ground"] == False) & 
        (df["longitude"].notna()) & 
        (df["latitude"].notna())
    ].copy()
    
    print(f"Nombre total d'avions détectés : {len(df)}")
    print(f"Nombre d'avions en vol avec des coordonnées validées : {len(df_clean)}")
    display(df_clean.head())

# %% [markdown]
# ## 3. Envoi des données vers PostgreSQL
# Maintenant que nos données sont propres dans le DataFrame `df_clean`, nous allons les envoyer dans PostgreSQL grâce à la librairie SQLAlchemy.

# %%
if latest_object and "df_clean" in locals() and not df_clean.empty:
    db_user = os.getenv("POSTGRES_USER", "admin")
    db_pass = os.getenv("POSTGRES_PASSWORD", "admin123")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "bigdata_db")
    
    # Chaîne de connexion PostgreSQL
    engine = create_engine(f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
    
    # Sauvegarder le DataFrame dans la table 'opensky_flights'
    # 'replace' pour écraser (ou 'append' pour ajouter à la suite)
    df_clean.to_sql('opensky_flights', engine, if_exists='replace', index=False)
    
    print("Données insérées avec succès dans PostgreSQL ! (Table : opensky_flights)")
