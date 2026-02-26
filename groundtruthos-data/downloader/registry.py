"""
PostGIS dataset catalog registry.

Stores metadata, provenance, and spatial index for all ingested datasets.
"""
import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values

logger = logging.getLogger(__name__)

# Default connection params — override via environment or constructor
DEFAULT_DB_PARAMS = {
    "dbname": "groundtruth",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432,
}

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS dataset_catalog (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    tile_id VARCHAR(500) NOT NULL,
    local_path TEXT NOT NULL,
    format VARCHAR(20) NOT NULL,
    crs VARCHAR(200),
    acquisition_date DATE,
    download_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    file_size_bytes BIGINT,
    checksum_sha256 CHAR(64),
    point_count BIGINT,
    point_density_per_m2 FLOAT,
    z_min FLOAT,
    z_max FLOAT,
    vertical_datum VARCHAR(50),
    horizontal_datum VARCHAR(50),
    license JSONB NOT NULL,
    quality_status VARCHAR(20) DEFAULT 'pending'
        CHECK (quality_status IN ('pending', 'passed', 'flagged', 'rejected')),
    quality_notes TEXT,
    metadata JSONB,
    bbox GEOMETRY(Polygon, 4326),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(source, tile_id)
);

CREATE INDEX IF NOT EXISTS idx_catalog_bbox
    ON dataset_catalog USING GIST(bbox);
CREATE INDEX IF NOT EXISTS idx_catalog_source
    ON dataset_catalog(source);
CREATE INDEX IF NOT EXISTS idx_catalog_quality
    ON dataset_catalog(quality_status);
CREATE INDEX IF NOT EXISTS idx_catalog_format
    ON dataset_catalog(format);
"""


class DatasetRegistry:
    """PostGIS-backed dataset catalog."""

    def __init__(self, db_params: dict | None = None):
        self.db_params = db_params or DEFAULT_DB_PARAMS

    @contextmanager
    def _connection(self):
        conn = psycopg2.connect(**self.db_params)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize_schema(self):
        """Create tables and indexes if they don't exist."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
        logger.info("Database schema initialized")

    def register(
        self,
        source: str,
        tile_id: str,
        file_path: Path,
        file_format: str,
        checksum: str,
        license_info: dict,
        metadata: dict | None = None,
        bounds_wgs84: dict | None = None,
    ) -> int:
        """Register a dataset tile in the catalog.

        Args:
            source: Source identifier (e.g., 'usgs_3dep').
            tile_id: Unique tile identifier within source.
            file_path: Local file path.
            file_format: File format (laz, tif, etc.).
            checksum: SHA256 hex digest.
            license_info: License metadata dict.
            metadata: Extracted file metadata.
            bounds_wgs84: Bounding box in WGS84 {x_min, y_min, x_max, y_max}.

        Returns:
            Database ID of the registered record.
        """
        metadata = metadata or {}

        # Build bbox geometry if bounds provided
        bbox_wkt = None
        if bounds_wgs84:
            x0 = bounds_wgs84["x_min"]
            y0 = bounds_wgs84["y_min"]
            x1 = bounds_wgs84["x_max"]
            y1 = bounds_wgs84["y_max"]
            bbox_wkt = (
                f"SRID=4326;POLYGON(("
                f"{x0} {y0},{x1} {y0},{x1} {y1},{x0} {y1},{x0} {y0}))"
            )

        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dataset_catalog
                        (source, tile_id, local_path, format, checksum_sha256,
                         license, metadata, crs, point_count, point_density_per_m2,
                         z_min, z_max, file_size_bytes, bbox)
                    VALUES
                        (%s, %s, %s, %s, %s,
                         %s, %s, %s, %s, %s,
                         %s, %s, %s,
                         ST_GeomFromEWKT(%s))
                    ON CONFLICT (source, tile_id)
                    DO UPDATE SET
                        local_path = EXCLUDED.local_path,
                        checksum_sha256 = EXCLUDED.checksum_sha256,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    (
                        source,
                        tile_id,
                        str(file_path),
                        file_format,
                        checksum,
                        Json(license_info),
                        Json(metadata),
                        metadata.get("crs"),
                        metadata.get("point_count"),
                        metadata.get("density_pts_m2"),
                        metadata.get("bounds", {}).get("z_min"),
                        metadata.get("bounds", {}).get("z_max"),
                        file_path.stat().st_size if file_path.exists() else None,
                        bbox_wkt,
                    ),
                )
                record_id = cur.fetchone()[0]

        logger.info(f"Registered {source}/{tile_id} -> catalog id={record_id}")
        return record_id

    def update_quality(self, record_id: int, status: str, notes: str = ""):
        """Update quality status for a catalog entry."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE dataset_catalog
                    SET quality_status = %s, quality_notes = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (status, notes, record_id),
                )

    def query_by_bbox(
        self,
        x_min: float,
        y_min: float,
        x_max: float,
        y_max: float,
        source: str | None = None,
        quality_status: str | None = None,
    ) -> list[dict]:
        """Query catalog entries intersecting a bounding box (WGS84)."""
        bbox_wkt = (
            f"SRID=4326;POLYGON(("
            f"{x_min} {y_min},{x_max} {y_min},{x_max} {y_max},"
            f"{x_min} {y_max},{x_min} {y_min}))"
        )

        conditions = ["ST_Intersects(bbox, ST_GeomFromEWKT(%s))"]
        params = [bbox_wkt]

        if source:
            conditions.append("source = %s")
            params.append(source)
        if quality_status:
            conditions.append("quality_status = %s")
            params.append(quality_status)

        where_clause = " AND ".join(conditions)

        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, source, tile_id, local_path, format, crs,
                           point_count, quality_status, metadata
                    FROM dataset_catalog
                    WHERE {where_clause}
                    ORDER BY source, tile_id
                    """,
                    params,
                )
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_stats(self) -> dict:
        """Get summary statistics of the catalog."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        source,
                        COUNT(*) as tile_count,
                        SUM(file_size_bytes) as total_bytes,
                        SUM(point_count) as total_points,
                        COUNT(*) FILTER (WHERE quality_status = 'passed') as passed,
                        COUNT(*) FILTER (WHERE quality_status = 'rejected') as rejected,
                        COUNT(*) FILTER (WHERE quality_status = 'pending') as pending
                    FROM dataset_catalog
                    GROUP BY source
                    ORDER BY source
                    """
                )
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def tile_exists(self, source: str, tile_id: str) -> bool:
        """Check if a tile is already registered."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM dataset_catalog WHERE source = %s AND tile_id = %s",
                    (source, tile_id),
                )
                return cur.fetchone() is not None
