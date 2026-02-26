"""
PostGIS dataset registry — high-level operations for registering datasets,
tiles, surfaces, and querying lineage through the knowledge graph.
"""
import hashlib
import logging
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)


def register_dataset(
    conn_string: str,
    source_name: str,
    source_url: str | None = None,
    license_type: str = "Unknown",
    license_meta: dict | None = None,
    crs: str | None = None,
    file_path: str | None = None,
    file_format: str | None = None,
    checksum: str | None = None,
    metadata: dict | None = None,
) -> str:
    """Register a new dataset in the datasets table.

    Args:
        conn_string: PostgreSQL connection string.
        source_name: Name/identifier of the data source.
        source_url: Origin URL.
        license_type: License identifier (e.g., 'Public Domain', 'CC-BY 4.0').
        license_meta: Additional license metadata.
        crs: Coordinate reference system string.
        file_path: Local file path.
        file_format: File format (laz, tif, etc.).
        checksum: SHA256 hex digest.
        metadata: Arbitrary metadata dict.

    Returns:
        UUID of the inserted dataset.
    """
    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO datasets
                    (source_name, source_url, license_type, license_meta,
                     crs, file_path, file_format, checksum, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    source_name,
                    source_url,
                    license_type,
                    Json(license_meta or {}),
                    crs,
                    file_path,
                    file_format,
                    checksum,
                    Json(metadata or {}),
                ),
            )
            dataset_id = cur.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Registered dataset %s → %s", source_name, dataset_id)
    return str(dataset_id)


def register_tile(
    conn_string: str,
    dataset_id: str,
    file_path: str,
    bounds: dict,
    stats: dict | None = None,
) -> str:
    """Register a tile in the lidar_tiles table with PostGIS geometry.

    Args:
        conn_string: PostgreSQL connection string.
        dataset_id: UUID of the parent dataset.
        file_path: Path to the tile file.
        bounds: Dict with x_min, y_min, x_max, y_max.
        stats: Optional dict with point_count, density_pts_m2, z_min,
               z_max, z_std, classification.

    Returns:
        UUID of the inserted tile.
    """
    stats = stats or {}
    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lidar_tiles
                    (dataset_id, file_path, geom, point_count,
                     density_pts_m2, z_min, z_max, z_std, classification)
                VALUES
                    (%s, %s,
                     ST_MakeEnvelope(%s, %s, %s, %s, 4326),
                     %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    dataset_id,
                    file_path,
                    bounds["x_min"], bounds["y_min"],
                    bounds["x_max"], bounds["y_max"],
                    stats.get("point_count"),
                    stats.get("density_pts_m2"),
                    stats.get("z_min"),
                    stats.get("z_max"),
                    stats.get("z_std"),
                    Json(stats.get("classification", {})),
                ),
            )
            tile_id = cur.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Registered tile %s → %s", file_path, tile_id)
    return str(tile_id)


def register_surface(
    conn_string: str,
    version_hash: str,
    rmse: float | None = None,
    parent_surface_id: str | None = None,
    metadata: dict | None = None,
    tile_ids: list[str] | None = None,
) -> str:
    """Register a surface and optionally link it to tiles.

    Args:
        conn_string: PostgreSQL connection string.
        version_hash: SHA256 identifying this surface version.
        rmse: Root mean square error.
        parent_surface_id: UUID of the parent surface (for chains).
        metadata: Arbitrary metadata.
        tile_ids: List of tile UUIDs to link via tile_surface_link.

    Returns:
        UUID of the inserted surface.
    """
    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO surfaces
                    (version_hash, rmse, parent_surface_id, metadata)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (version_hash, rmse, parent_surface_id, Json(metadata or {})),
            )
            surface_id = cur.fetchone()[0]

            # Link tiles if provided
            if tile_ids:
                for tid in tile_ids:
                    cur.execute(
                        """
                        INSERT INTO tile_surface_link (tile_id, surface_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (tid, surface_id),
                    )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Registered surface %s (rmse=%.4f)", surface_id, rmse or 0)
    return str(surface_id)


def get_lineage(conn_string: str, surface_id: str) -> list[dict]:
    """Trace full lineage for a surface through the knowledge graph.

    Uses the lineage_view to join dataset → tile → surface →
    telemetry → outcome.

    Args:
        conn_string: PostgreSQL connection string.
        surface_id: UUID of the surface to trace.

    Returns:
        List of lineage dicts, one per row in the view.
    """
    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    dataset_id, source_name, tile_id, tile_path,
                    density_pts_m2, surface_id, version_hash, surface_rmse,
                    zone_id, soil_resistance, rework_flag,
                    outcome_id, metric_name, predicted_value,
                    measured_value, variance
                FROM lineage_view
                WHERE surface_id = %s
                ORDER BY tile_id
                """,
                (surface_id,),
            )
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()

    lineage = [dict(zip(columns, row)) for row in rows]
    logger.info("Lineage for surface %s: %d rows", surface_id, len(lineage))
    return lineage
