"""
Feature store computation and export layer.

Computes geomorphometric features from tiles and surfaces, stores them
in PostGIS, and exports model-ready datasets as Parquet or DuckDB.
"""
import logging
from pathlib import Path

import laspy
import numpy as np
import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)


def compute_tile_features(
    tile_path: Path,
    tile_id: str,
    feature_version: str,
    conn_string: str | None = None,
) -> dict:
    """Compute geomorphometric features for a single tile.

    Features: elevation stats, slope proxy, curvature proxy, roughness,
    and breakline probability (based on local z variance).

    Args:
        tile_path: Path to tile LAS/LAZ file.
        tile_id: UUID of the tile in lidar_tiles.
        feature_version: Version tag for this feature computation.
        conn_string: Optional connection string to persist features.

    Returns:
        Dict of computed features.
    """
    with laspy.open(str(tile_path)) as reader:
        las = reader.read()

    z = np.array(las.z, dtype=np.float64)
    x = np.array(las.x, dtype=np.float64)
    y = np.array(las.y, dtype=np.float64)

    # Elevation statistics
    z_mean = float(np.mean(z)) if len(z) > 0 else 0.0
    z_std = float(np.std(z)) if len(z) > 0 else 0.0
    z_min = float(np.min(z)) if len(z) > 0 else 0.0
    z_max = float(np.max(z)) if len(z) > 0 else 0.0

    # Slope proxy — average absolute gradient of z along sorted x
    slope_mean = 0.0
    slope_max = 0.0
    if len(z) > 1:
        order = np.argsort(x)
        dz = np.diff(z[order])
        dx = np.diff(x[order])
        valid = np.abs(dx) > 1e-6
        if valid.any():
            gradients = np.abs(dz[valid] / dx[valid])
            slope_mean = float(np.mean(gradients))
            slope_max = float(np.max(gradients))

    # Curvature proxy — second derivative along x
    curvature_mean = 0.0
    if len(z) > 2:
        order = np.argsort(x)
        dz = np.diff(z[order])
        dx = np.diff(x[order])
        valid = np.abs(dx) > 1e-6
        if valid.sum() > 1:
            grad = dz[valid] / dx[valid]
            curvature_mean = float(np.mean(np.abs(np.diff(grad))))

    # Roughness — std of residuals after removing linear trend
    roughness = 0.0
    if len(z) > 2:
        try:
            coeffs = np.polyfit(x, z, 1)
            trend = np.polyval(coeffs, x)
            roughness = float(np.std(z - trend))
        except (np.linalg.LinAlgError, ValueError):
            roughness = z_std

    # Breakline probability — sigmoid of z_std
    breakline_prob = float(1.0 / (1.0 + np.exp(-2.0 * (z_std - 1.0))))

    features = {
        "tile_id": tile_id,
        "feature_version": feature_version,
        "z_mean": round(z_mean, 4),
        "z_std": round(z_std, 4),
        "z_min": round(z_min, 4),
        "z_max": round(z_max, 4),
        "slope_mean": round(slope_mean, 4),
        "slope_max": round(slope_max, 4),
        "curvature_mean": round(curvature_mean, 4),
        "roughness": round(roughness, 4),
        "breakline_prob": round(breakline_prob, 4),
        "feature_vector": {
            "z_mean": round(z_mean, 4),
            "z_std": round(z_std, 4),
            "slope_mean": round(slope_mean, 4),
            "curvature_mean": round(curvature_mean, 4),
            "roughness": round(roughness, 4),
            "breakline_prob": round(breakline_prob, 4),
        },
    }

    if conn_string:
        _persist_tile_features(conn_string, features)

    return features


def _persist_tile_features(conn_string: str, features: dict) -> None:
    """Upsert tile features into the tile_features table."""
    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tile_features
                    (tile_id, feature_version, z_mean, z_std, z_min, z_max,
                     slope_mean, slope_max, curvature_mean, roughness,
                     breakline_prob, feature_vector)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tile_id, feature_version) DO UPDATE SET
                    z_mean = EXCLUDED.z_mean,
                    z_std = EXCLUDED.z_std,
                    z_min = EXCLUDED.z_min,
                    z_max = EXCLUDED.z_max,
                    slope_mean = EXCLUDED.slope_mean,
                    slope_max = EXCLUDED.slope_max,
                    curvature_mean = EXCLUDED.curvature_mean,
                    roughness = EXCLUDED.roughness,
                    breakline_prob = EXCLUDED.breakline_prob,
                    feature_vector = EXCLUDED.feature_vector,
                    computed_at = NOW()
                """,
                (
                    features["tile_id"],
                    features["feature_version"],
                    features["z_mean"],
                    features["z_std"],
                    features["z_min"],
                    features["z_max"],
                    features["slope_mean"],
                    features["slope_max"],
                    features["curvature_mean"],
                    features["roughness"],
                    features["breakline_prob"],
                    Json(features["feature_vector"]),
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def compute_surface_features(
    surface_id: str,
    feature_version: str,
    conn_string: str,
) -> dict:
    """Compute features for a surface record.

    Reads surface metadata from the DB, computes RMSE, cut/fill volumes,
    and slope violation percentage.

    Args:
        surface_id: UUID of the surface.
        feature_version: Version tag.
        conn_string: PostgreSQL connection string.

    Returns:
        Dict of surface features.
    """
    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rmse, metadata FROM surfaces WHERE id = %s",
                (surface_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Surface {surface_id} not found")

            rmse = row[0] or 0.0
            meta = row[1] or {}

            features = {
                "surface_id": surface_id,
                "feature_version": feature_version,
                "rmse": rmse,
                "cut_volume_m3": meta.get("cut_volume_m3", 0.0),
                "fill_volume_m3": meta.get("fill_volume_m3", 0.0),
                "slope_violation_pct": meta.get("slope_violation_pct", 0.0),
            }

            cur.execute(
                """
                INSERT INTO surface_features
                    (surface_id, feature_version, rmse, cut_volume_m3,
                     fill_volume_m3, slope_violation_pct)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (surface_id, feature_version) DO UPDATE SET
                    rmse = EXCLUDED.rmse,
                    cut_volume_m3 = EXCLUDED.cut_volume_m3,
                    fill_volume_m3 = EXCLUDED.fill_volume_m3,
                    slope_violation_pct = EXCLUDED.slope_violation_pct,
                    computed_at = NOW()
                """,
                (
                    surface_id,
                    feature_version,
                    features["rmse"],
                    features["cut_volume_m3"],
                    features["fill_volume_m3"],
                    features["slope_violation_pct"],
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return features


def export_features_parquet(
    tile_ids: list[str],
    feature_version: str,
    output_path: Path,
    conn_string: str,
) -> Path:
    """Export tile features to a Parquet file for model training.

    Args:
        tile_ids: List of tile UUIDs to export.
        feature_version: Feature version to export.
        output_path: Destination Parquet file path.
        conn_string: PostgreSQL connection string.

    Returns:
        Path to written Parquet file.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(tile_ids))
            cur.execute(
                f"""
                SELECT tile_id, feature_version,
                       z_mean, z_std, z_min, z_max,
                       slope_mean, slope_max, curvature_mean,
                       roughness, breakline_prob
                FROM tile_features
                WHERE tile_id::text IN ({placeholders})
                  AND feature_version = %s
                """,
                (*tile_ids, feature_version),
            )
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    if not rows:
        logger.warning("No features found for export")
        return output_path

    # Build Arrow table
    data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    table = pa.table(data)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(output_path))
    logger.info("Exported %d feature rows to %s", len(rows), output_path)
    return output_path


def snapshot_online_store(
    feature_version: str,
    duckdb_path: Path,
    conn_string: str,
) -> Path:
    """Snapshot tile features into a DuckDB file for fast inference lookups.

    Args:
        feature_version: Version to snapshot.
        duckdb_path: Path for the DuckDB file.
        conn_string: PostgreSQL connection string.

    Returns:
        Path to DuckDB file.
    """
    import duckdb

    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tile_id::text, feature_version,
                       z_mean, z_std, z_min, z_max,
                       slope_mean, slope_max, curvature_mean,
                       roughness, breakline_prob
                FROM tile_features
                WHERE feature_version = %s
                """,
                (feature_version,),
            )
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(str(duckdb_path))
    try:
        # Create table
        col_defs = ", ".join(
            f"{c} VARCHAR" if c in ("tile_id", "feature_version") else f"{c} DOUBLE"
            for c in columns
        )
        db.execute(f"CREATE OR REPLACE TABLE tile_features ({col_defs})")

        if rows:
            placeholders = ", ".join(["?"] * len(columns))
            db.executemany(
                f"INSERT INTO tile_features VALUES ({placeholders})",
                rows,
            )
        logger.info("Snapshotted %d rows to %s", len(rows), duckdb_path)
    finally:
        db.close()

    return duckdb_path


def compute_telemetry_features(
    zone_id: str,
    feature_version: str,
    conn_string: str,
) -> dict:
    """Compute features for a telemetry zone.

    Reads aggregated zone stats from telemetry_zones table and persists them
    into the telemetry_features table.

    Args:
        zone_id: UUID of the telemetry zone.
        feature_version: Version tag.
        conn_string: PostgreSQL connection string.

    Returns:
        Dict of computed telemetry features.
    """
    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            # Fetch raw zone data
            cur.execute(
                "SELECT soil_resistance, rework_flag, metadata FROM telemetry_zones WHERE id = %s",
                (zone_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Telemetry zone {zone_id} not found")

            soil_resistance = row[0]
            rework_flag = row[1]
            metadata = row[2] or {}

            # Extract features from metadata if available (assuming normalization put them there)
            # or use top-level columns.
            features = {
                "zone_id": zone_id,
                "feature_version": feature_version,
                "soil_resistance": soil_resistance,
                "rework_flag": rework_flag,
                "blade_pressure": metadata.get("avg_blade_pressure"),
                "speed_m_s": metadata.get("avg_speed_m_s"),
                "pass_count": metadata.get("max_pass_count"),
            }

            cur.execute(
                """
                INSERT INTO telemetry_features
                    (zone_id, feature_version, soil_resistance, rework_flag,
                     blade_pressure, speed_m_s, pass_count, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (zone_id, feature_version) DO UPDATE SET
                    soil_resistance = EXCLUDED.soil_resistance,
                    rework_flag = EXCLUDED.rework_flag,
                    blade_pressure = EXCLUDED.blade_pressure,
                    speed_m_s = EXCLUDED.speed_m_s,
                    pass_count = EXCLUDED.pass_count,
                    computed_at = NOW()
                """,
                (
                    zone_id,
                    feature_version,
                    features["soil_resistance"],
                    features["rework_flag"],
                    features["blade_pressure"],
                    features["speed_m_s"],
                    features["pass_count"],
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return features


def export_telemetry_features_parquet(
    zone_ids: list[str],
    feature_version: str,
    output_path: Path,
    conn_string: str,
) -> Path:
    """Export telemetry features to a Parquet file for model training.

    Args:
        zone_ids: List of zone UUIDs to export.
        feature_version: Feature version to export.
        output_path: Destination Parquet file path.
        conn_string: PostgreSQL connection string.

    Returns:
        Path to written Parquet file.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not zone_ids:
        logger.warning("No zone_ids provided for export")
        return output_path

    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(zone_ids))
            cur.execute(
                f"""
                SELECT zone_id, feature_version,
                       soil_resistance, rework_flag,
                       blade_pressure, speed_m_s, pass_count
                FROM telemetry_features
                WHERE zone_id::text IN ({placeholders})
                  AND feature_version = %s
                """,
                (*zone_ids, feature_version),
            )
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    if not rows:
        logger.warning("No telemetry features found for export")
        return output_path

    # Build Arrow table
    data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    table = pa.table(data)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(output_path))
    logger.info("Exported %d telemetry feature rows to %s", len(rows), output_path)
    return output_path


def snapshot_telemetry_features(
    feature_version: str,
    duckdb_path: Path,
    conn_string: str,
) -> Path:
    """Snapshot telemetry features into a DuckDB file for fast inference lookups.

    Args:
        feature_version: Version to snapshot.
        duckdb_path: Path for the DuckDB file.
        conn_string: PostgreSQL connection string.

    Returns:
        Path to DuckDB file.
    """
    import duckdb

    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT zone_id::text, feature_version,
                       soil_resistance, rework_flag,
                       blade_pressure, speed_m_s, pass_count
                FROM telemetry_features
                WHERE feature_version = %s
                """,
                (feature_version,),
            )
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(str(duckdb_path))
    try:
        # Create table
        col_defs = ", ".join(
            f"{c} VARCHAR" if c in ("zone_id", "feature_version")
            else f"{c} BOOLEAN" if c == "rework_flag"
            else f"{c} DOUBLE"
            for c in columns
        )
        db.execute(f"CREATE OR REPLACE TABLE telemetry_features ({col_defs})")

        if rows:
            placeholders = ", ".join(["?"] * len(columns))
            db.executemany(
                f"INSERT INTO telemetry_features VALUES ({placeholders})",
                rows,
            )
        logger.info("Snapshotted %d telemetry feature rows to %s", len(rows), duckdb_path)
    finally:
        db.close()

    return duckdb_path
