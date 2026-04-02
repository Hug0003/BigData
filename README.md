# Aircraft Tracking — Data Pipeline

Pipeline de données temps réel pour le suivi d'avions.  
Les données brutes sont collectées depuis des APIs publiques, stockées dans un data lake (MinIO), puis transformées et chargées dans un entrepôt de données (PostgreSQL).

---

## Architecture

```
APIs externes
  ├── OpenSky Network   → états en temps réel de tous les avions (ADS-B)
  ├── Geoapify          → reverse geocoding (lat/lon → adresse/pays)

┌─────────────────────────────────────────────────────────────────┐
│  INGESTION  (src/main.py)                                        │
│                                                                  │
│  OpenSky API ──► MinIO  raw-api-data/opensky_states/...json     │
│  Geoapify API ──► MinIO  raw-api-data/geoapify_geocode/...json  │
└─────────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  ETL  (etl.py)                                                   │
│                                                                  │
│  MinIO ──► pandas ──► PostgreSQL  aircraft_states               │
│                                                                  │
│  Enrichissements :                                               │
│   · origin_country      ← champ natif OpenSky                   │
│   · current_country     ← reverse geocoding offline (GeoNames)  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Infrastructure (Docker)

```bash
docker-compose up -d
```

| Service       | Rôle                    | Port(s)         |
|---------------|-------------------------|-----------------|
| MinIO         | Data lake (stockage S3) | `9000` (API), `9001` (console) |
| PostgreSQL    | Data warehouse          | `5432`          |
| Kafka         | Streaming (futur)       | `9092`          |
| Zookeeper     | Coordination Kafka      | `2181`          |
| Spark master  | Traitement distribué    | `7077`, `8080` (UI) |
| Spark worker  | Worker Spark            | —               |

Interfaces web :
- MinIO console : http://localhost:9001 (minioadmin / minioadmin)
- Spark UI : http://localhost:8080

---

## Installation

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

---

## Utilisation

### 1. Ingestion — APIs → MinIO

Collecte les états de tous les avions (≈10 000) et enrichit un échantillon avec Geoapify.

```bash
python src/main.py
```

Produit dans MinIO :
- `raw-api-data/opensky_states/YYYY/MM/DD/HHmmss_<id>.json`
- `raw-api-data/geoapify_geocode/YYYY/MM/DD/HHmmss_<id>.json`

### 2. Migration — Création du schéma PostgreSQL

À lancer une seule fois (ou après ajout d'une nouvelle migration).

```bash
python src/migrate.py
```

Les fichiers SQL sont dans `src/migrations/`, exécutés dans l'ordre alphabétique.  
Les migrations déjà appliquées sont tracées dans la table `_migrations`.

### 3. ETL — MinIO → PostgreSQL

Lit le dernier snapshot OpenSky depuis MinIO, résout le pays survolé pour chaque avion (reverse geocoding offline via GeoNames), et charge le tout dans PostgreSQL.

```bash
python etl.py
```

### 4. Dashboard — Visualisation

```bash
streamlit run dashboard.py
```

Ouvre http://localhost:8501 dans le navigateur.

---

## Structure des fichiers

```
.
├── docker-compose.yml
├── requirements.txt
├── .env                        # variables d'environnement (ne pas committer les clés)
│
├── src/
│   ├── main.py                 # pipeline d'ingestion (APIs → MinIO)
│   ├── api_clients.py          # clients OpenSky, Geoapify
│   ├── minio_storage.py        # wrapper MinIO
│   ├── migrate.py              # runner de migrations SQL
│   └── migrations/
│       └── 001_initial_schema.sql
│
└── etl.py                      # ETL MinIO → PostgreSQL
```

---

## Schéma PostgreSQL

Table `aircraft_states` :

| Colonne               | Type             | Description                                      |
|-----------------------|------------------|--------------------------------------------------|
| `icao24`              | VARCHAR(10)      | Identifiant ICAO 24-bit (ex: `3c6444`)           |
| `callsign`            | VARCHAR(20)      | Indicatif de vol (ex: `AFR1234`)                 |
| `origin_country`      | VARCHAR(100)     | Pays d'immatriculation (source OpenSky)          |
| `current_country`     | VARCHAR(100)     | Pays survolé (reverse geocoding GPS)             |
| `current_country_code`| CHAR(2)          | Code ISO-3166-1 alpha-2 (ex: `FR`)               |

| `longitude/latitude`  | DOUBLE PRECISION | Coordonnées GPS actuelles                        |
| `baro_altitude`       | DOUBLE PRECISION | Altitude barométrique (mètres)                   |
| `velocity`            | DOUBLE PRECISION | Vitesse sol (m/s)                                |
| `true_track`          | DOUBLE PRECISION | Cap (degrés, 0 = nord, sens horaire)             |
| `data_timestamp`      | TIMESTAMPTZ      | Horodatage du snapshot OpenSky                   |
| `ingested_at`         | TIMESTAMPTZ      | Date d'insertion en base                         |

---

## TODO

### Fait
- [x] Infrastructure Docker (MinIO, PostgreSQL, Kafka, Spark)
- [x] Client OpenSky Network (`src/api_clients.py`)
- [x] Client Geoapify reverse geocoding (`src/api_clients.py`)

- [x] Wrapper MinIO avec sauvegarde horodatée (`src/minio_storage.py`)
- [x] Pipeline d'ingestion OpenSky → MinIO (`src/main.py`)
- [x] Enrichissement Geoapify → MinIO pour un échantillon de vols (`src/main.py`)
- [x] Système de migrations SQL avec suivi des versions (`src/migrate.py`)
- [x] Schéma PostgreSQL `aircraft_states` (`src/migrations/001_initial_schema.sql`)
- [x] ETL MinIO → PostgreSQL avec reverse geocoding offline (`etl.py`)
- [x] Résolution du pays survolé pour tous les avions (GeoNames, sans appel API)
- [x] Dashboard Streamlit : carte mondiale, KPIs, top pays, table filtrée (`dashboard.py`)

### À faire

- [ ] Implémenter le streaming Kafka : ingestion continue toutes les N secondes
- [ ] Traitement Spark : agrégations par pays, densité de trafic, etc.
- [ ] Mettre en place une planification (cron / Airflow) pour `main.py` et `etl.py`
- [ ] Gestion des doublons lors des chargements répétés (upsert par `icao24` + `data_timestamp`)
