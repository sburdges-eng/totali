#!/usr/bin/env python3
"""
Database migration runner.

Reads SQL files from the schema/ directory in lexicographic order and
executes them against a PostgreSQL + PostGIS database.

Usage:
    python scripts/migrate.py
    python scripts/migrate.py --conn "dbname=groundtruth user=postgres host=localhost"
"""
import argparse
import logging
import os
import sys
from pathlib import Path

import psycopg2

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schema"

DEFAULT_CONN = os.environ.get(
    "GROUNDTRUTH_DB",
    "dbname=groundtruth user=postgres password=password host=localhost port=5432",
)


def run_migrations(conn_string: str, schema_dir: Path = SCHEMA_DIR) -> list[str]:
    """Execute all SQL migration files in order.

    Args:
        conn_string: PostgreSQL connection string.
        schema_dir: Directory containing numbered .sql files.

    Returns:
        List of applied migration filenames.
    """
    sql_files = sorted(schema_dir.glob("*.sql"))
    if not sql_files:
        logger.warning("No SQL files found in %s", schema_dir)
        return []

    applied = []
    conn = psycopg2.connect(conn_string)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            for sql_file in sql_files:
                logger.info("Applying %s ...", sql_file.name)
                sql = sql_file.read_text()
                cur.execute(sql)
                applied.append(sql_file.name)
                logger.info("  ✓ %s applied", sql_file.name)

        conn.commit()
        logger.info("All %d migrations applied successfully", len(applied))
    except Exception as e:
        conn.rollback()
        logger.error("Migration failed: %s", e)
        raise
    finally:
        conn.close()

    return applied


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Run database migrations")
    parser.add_argument(
        "--conn",
        default=DEFAULT_CONN,
        help="PostgreSQL connection string (default: $GROUNDTRUTH_DB or local)",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=SCHEMA_DIR,
        help="Directory containing SQL migration files",
    )
    args = parser.parse_args()

    applied = run_migrations(args.conn, args.schema_dir)
    print(f"\nApplied {len(applied)} migrations: {', '.join(applied)}")


if __name__ == "__main__":
    main()
