"""
Runner de migrations SQL.
Execute les fichiers *.sql de src/migrations/ dans l'ordre alphabétique,
en gardant en base la liste des migrations déjà appliquées.

Usage:
    python src/migrate.py
"""
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "admin123"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
    )


def ensure_migrations_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS _migrations (
            id         SERIAL PRIMARY KEY,
            filename   VARCHAR(255) UNIQUE NOT NULL,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)


def get_applied(cur) -> set:
    cur.execute("SELECT filename FROM _migrations")
    return {row[0] for row in cur.fetchall()}


def run():
    logger.info("=== Running database migrations ===")
    conn = get_conn()
    cur = conn.cursor()

    ensure_migrations_table(cur)
    conn.commit()

    applied = get_applied(cur)
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not migration_files:
        logger.warning("No migration files found in %s", MIGRATIONS_DIR)

    for path in migration_files:
        name = path.name
        if name in applied:
            logger.info("  [skip] %s (already applied)", name)
            continue

        logger.info("  [apply] %s", name)
        sql = path.read_text(encoding="utf-8")
        try:
            cur.execute(sql)
            cur.execute("INSERT INTO _migrations (filename) VALUES (%s)", (name,))
            conn.commit()
            logger.info("  [ok]    %s", name)
        except Exception as exc:
            conn.rollback()
            logger.error("  [fail]  %s → %s", name, exc)
            raise

    cur.close()
    conn.close()
    logger.info("=== Migrations done ===")


if __name__ == "__main__":
    run()
