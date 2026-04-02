# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A data engineering pipeline for real-time aircraft tracking. Data flows from external APIs → MinIO (raw data lake) → PostgreSQL (data warehouse), with Kafka and Spark provisioned for future streaming/processing use.

## Infrastructure

Start all services with Docker Compose:
```bash
docker-compose up -d
```

Services and their ports:
- **MinIO** (data lake): API on `9000`, console at `http://localhost:9001` (minioadmin/minioadmin)
- **PostgreSQL** (data warehouse): `5432` (admin/admin123, database: bigdata_db)
- **Kafka**: `9092`
- **Zookeeper**: `2181`
- **Spark master UI**: `http://localhost:8080`, Spark port `7077`

## Python Environment

```bash
python -m venv venv
venv\Scripts\activate       # Windows
pip install -r requirements.txt
```

## Running the Pipeline

```bash
# Run the data ingestion pipeline (OpenSky → MinIO → Geoapify enrichment)
python src/main.py

# Run ETL: MinIO → pandas transformation → PostgreSQL load
python notebook.py
```

## Architecture

```
External APIs
  ├── OpenSky Network   → aircraft state vectors (real-time)
  ├── Geoapify          → reverse geocoding (lat/lng → address)
  └── FlightAware AeroAPI → flight tracking (initialized, not yet used in pipeline)

src/api_clients.py      → API client classes (OpenSkyClient, GeoapifyClient, AeroAPIClient)
src/minio_storage.py    → MinIO wrapper; saves raw JSON with timestamp-based keys
src/main.py             → DataPipeline orchestrator: collect → store raw → enrich

notebook.py             → EDA + ETL script: read MinIO → pandas transform → load PostgreSQL
```

**Data flow:**
1. `DataPipeline` in `main.py` calls `OpenSkyClient.get_all_states()` and saves the raw response to MinIO under `raw-api-data/` with timestamp-based directory structure.
2. Coordinates are enriched via `GeoapifyClient.reverse_geocode()`.
3. `notebook.py` reads accumulated JSON from MinIO, transforms with pandas (OpenSky returns 17-field state arrays), and inserts into PostgreSQL.

## Configuration

All credentials and endpoints are in `.env`. The `.env` file is tracked by git (modified in working tree) — do not commit API keys. Key variables:
- `GEOAPIFY_API_KEY`, `AEROAPI_KEY`
- `MINIO_*`, `POSTGRES_*`, `KAFKA_*`
