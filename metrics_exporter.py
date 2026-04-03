"""
Exporteur de métriques Prometheus pour le pipeline Aircraft Tracking.

Métriques exposées :
  - Données traitées (PostgreSQL) : lignes totales, snapshots, dernier snapshot
  - Données brutes (MinIO) : fichiers stockés, dernier fichier
  - Pipeline (Airflow) : dernier run, prochain déclenchement, durée

Endpoint : http://localhost:8000/metrics
"""
import os
import time
import logging

from dotenv import load_dotenv
import psycopg2
from minio import Minio
from prometheus_client import start_http_server, Gauge

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Métriques PostgreSQL — données traitées
# ---------------------------------------------------------------------------
AIRCRAFT_TOTAL_ROWS = Gauge(
    "aircraft_states_total_rows",
    "Nombre total de lignes dans aircraft_states",
)
AIRCRAFT_SNAPSHOTS_COUNT = Gauge(
    "aircraft_states_snapshots_count",
    "Nombre de snapshots distincts en base",
)
AIRCRAFT_LATEST_SNAPSHOT_TS = Gauge(
    "aircraft_states_latest_snapshot_unixtime",
    "Timestamp Unix du snapshot le plus récent",
)
AIRCRAFT_ROWS_LATEST_SNAPSHOT = Gauge(
    "aircraft_states_rows_latest_snapshot",
    "Nombre d'avions dans le dernier snapshot",
)

# ---------------------------------------------------------------------------
# Métriques MinIO — données brutes
# ---------------------------------------------------------------------------
MINIO_RAW_FILES_TOTAL = Gauge(
    "minio_opensky_raw_files_total",
    "Nombre total de fichiers JSON bruts OpenSky dans MinIO",
)
MINIO_LATEST_FILE_TS = Gauge(
    "minio_opensky_latest_file_unixtime",
    "Timestamp Unix du dernier fichier brut dans MinIO",
)

# ---------------------------------------------------------------------------
# Métriques Airflow — pipeline
# ---------------------------------------------------------------------------
AIRFLOW_LAST_RUN_TS = Gauge(
    "airflow_dag_last_run_unixtime",
    "Timestamp Unix de la fin du dernier DAG run",
    ["dag_id", "state"],
)
AIRFLOW_NEXT_RUN_TS = Gauge(
    "airflow_dag_next_run_unixtime",
    "Timestamp Unix du prochain déclenchement planifié",
    ["dag_id"],
)
AIRFLOW_LAST_RUN_DURATION = Gauge(
    "airflow_dag_last_run_duration_seconds",
    "Durée du dernier DAG run en secondes",
    ["dag_id"],
)


# ---------------------------------------------------------------------------
# Connexions
# ---------------------------------------------------------------------------

def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "admin123"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
    )


def _minio_client():
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


# ---------------------------------------------------------------------------
# Collecteurs
# ---------------------------------------------------------------------------

def collect_aircraft_metrics():
    try:
        conn = _pg_conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM aircraft_states")
        AIRCRAFT_TOTAL_ROWS.set(cur.fetchone()[0])

        cur.execute("SELECT COUNT(DISTINCT data_timestamp) FROM aircraft_states")
        AIRCRAFT_SNAPSHOTS_COUNT.set(cur.fetchone()[0])

        cur.execute("SELECT MAX(data_timestamp) FROM aircraft_states")
        latest_ts = cur.fetchone()[0]
        if latest_ts:
            AIRCRAFT_LATEST_SNAPSHOT_TS.set(latest_ts.timestamp())
            cur.execute(
                "SELECT COUNT(*) FROM aircraft_states WHERE data_timestamp = %s",
                (latest_ts,),
            )
            AIRCRAFT_ROWS_LATEST_SNAPSHOT.set(cur.fetchone()[0])

        cur.close()
        conn.close()
        logger.debug("aircraft metrics OK")
    except Exception as exc:
        logger.warning("aircraft metrics error: %s", exc)


def collect_minio_metrics():
    try:
        client = _minio_client()
        bucket = os.getenv("MINIO_BUCKET_NAME", "raw-api-data")
        objects = list(client.list_objects(bucket, prefix="opensky_states/", recursive=True))
        MINIO_RAW_FILES_TOTAL.set(len(objects))
        if objects:
            latest = max(objects, key=lambda o: o.last_modified)
            MINIO_LATEST_FILE_TS.set(latest.last_modified.timestamp())
        logger.debug("MinIO metrics OK (%d fichiers)", len(objects))
    except Exception as exc:
        logger.warning("MinIO metrics error: %s", exc)


def collect_airflow_metrics():
    try:
        conn = _pg_conn()
        cur = conn.cursor()

        # Dernier run par DAG (le plus récent start_date)
        cur.execute("""
            SELECT
                dr.dag_id,
                dr.state,
                EXTRACT(EPOCH FROM dr.end_date)                         AS end_ts,
                EXTRACT(EPOCH FROM (dr.end_date - dr.start_date))       AS duration_s
            FROM dag_run dr
            INNER JOIN (
                SELECT dag_id, MAX(start_date) AS max_start
                FROM dag_run
                GROUP BY dag_id
            ) latest ON dr.dag_id = latest.dag_id AND dr.start_date = latest.max_start
        """)
        for dag_id, state, end_ts, duration_s in cur.fetchall():
            label_state = state or "running"
            if end_ts:
                AIRFLOW_LAST_RUN_TS.labels(dag_id=dag_id, state=label_state).set(end_ts)
            if duration_s:
                AIRFLOW_LAST_RUN_DURATION.labels(dag_id=dag_id).set(duration_s)

        # Prochain déclenchement planifié
        cur.execute("""
            SELECT dag_id, EXTRACT(EPOCH FROM next_dagrun)
            FROM dag
            WHERE is_active = true AND next_dagrun IS NOT NULL
        """)
        for dag_id, next_ts in cur.fetchall():
            if next_ts:
                AIRFLOW_NEXT_RUN_TS.labels(dag_id=dag_id).set(next_ts)

        cur.close()
        conn.close()
        logger.debug("Airflow metrics OK")
    except Exception as exc:
        logger.warning("Airflow metrics error: %s", exc)


def collect_all():
    collect_aircraft_metrics()
    collect_minio_metrics()
    collect_airflow_metrics()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("METRICS_PORT", 8000))
    interval = int(os.getenv("METRICS_INTERVAL", 30))
    logger.info("Exporteur Prometheus démarré sur :%d (rafraîchissement toutes les %ds)", port, interval)
    start_http_server(port)
    while True:
        collect_all()
        logger.info("Métriques collectées.")
        time.sleep(interval)
