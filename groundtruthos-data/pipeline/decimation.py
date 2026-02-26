"""
Density-adaptive decimation with breakline preservation.

Adjusts voxel size per tile based on local point density, protects
high-gradient regions (breaklines), and logs every decimation to the
audit table.
"""
import json
import logging
import math
import subprocess
import tempfile
from pathlib import Path

import laspy
import numpy as np
import psycopg2

logger = logging.getLogger(__name__)

DEFAULT_BASE_VOXEL = 0.5   # metres
DEFAULT_TARGET_DENSITY = 8  # pts/m²
BREAKLINE_Z_STD_THRESHOLD = 1.5  # metres — regions above this are protected


def compute_density(point_count: int, bounds: dict) -> float:
    """Compute area-based point density (pts/m²).

    Args:
        point_count: Number of points.
        bounds: Dict with x_min, y_min, x_max, y_max.

    Returns:
        Density in points per square metre.
    """
    x_extent = bounds["x_max"] - bounds["x_min"]
    y_extent = bounds["y_max"] - bounds["y_min"]
    area = x_extent * y_extent
    if area <= 0:
        return 0.0
    return point_count / area


def compute_adaptive_voxel_size(
    current_density: float,
    target_density: float,
    base_voxel: float = DEFAULT_BASE_VOXEL,
) -> float:
    """Compute voxel size that achieves the target density.

    Uses sqrt scaling: voxel_new = base_voxel * sqrt(current / target).
    The intuition is that a 2D grid cell area scales as voxel², so to
    reduce density by factor k we scale voxel by sqrt(k).

    Args:
        current_density: Current point density (pts/m²).
        target_density: Desired point density (pts/m²).
        base_voxel: Baseline voxel size.

    Returns:
        Adjusted voxel size in metres.
    """
    if current_density <= 0 or target_density <= 0:
        return base_voxel
    if current_density <= target_density:
        return base_voxel  # no decimation needed
    ratio = current_density / target_density
    return base_voxel * math.sqrt(ratio)


def generate_adaptive_pipeline(
    input_file: str,
    output_file: str,
    current_density: float,
    target_density: float,
    base_voxel: float = DEFAULT_BASE_VOXEL,
) -> dict:
    """Build a PDAL pipeline JSON for adaptive decimation.

    If current density is at or below target, the pipeline is a simple
    read → write pass-through (no voxelgrid filter).

    Args:
        input_file: Input LAS/LAZ path.
        output_file: Output LAS/LAZ path.
        current_density: Current pts/m².
        target_density: Desired pts/m².
        base_voxel: Base voxel cell size.

    Returns:
        PDAL pipeline dict.
    """
    stages: list[dict] = [
        {"type": "readers.las", "filename": input_file},
    ]

    if current_density > target_density:
        voxel = compute_adaptive_voxel_size(current_density, target_density, base_voxel)
        stages.append({"type": "filters.voxelgrid", "cell": voxel})

    stages.append({
        "type": "writers.las",
        "filename": output_file,
        "compression": "laszip",
    })

    return {"pipeline": stages}


def should_protect_region(local_z_std: float, threshold: float = BREAKLINE_Z_STD_THRESHOLD) -> bool:
    """Determine if a region should be protected from decimation.

    High local z standard deviation indicates breaklines, sharp grade
    changes, or retaining walls that must be preserved at full density.

    Args:
        local_z_std: Local standard deviation of elevations (metres).
        threshold: Protection threshold.

    Returns:
        True if the region should be protected.
    """
    return local_z_std >= threshold


def decimate_tile(
    tile_path: Path,
    output_path: Path,
    target_density: float = DEFAULT_TARGET_DENSITY,
    conn_string: str | None = None,
    base_voxel: float = DEFAULT_BASE_VOXEL,
    breakline_threshold: float = BREAKLINE_Z_STD_THRESHOLD,
) -> dict:
    """Run full adaptive decimation on a single tile.

    Steps:
        1. Read tile and compute current density + z_std.
        2. Check breakline protection — skip decimation if protected.
        3. Generate adaptive PDAL pipeline and execute.
        4. Log decimation to `decimation_log` table (if conn_string provided).

    Args:
        tile_path: Input tile LAS/LAZ.
        output_path: Destination for decimated tile.
        target_density: Desired pts/m².
        conn_string: Optional PostgreSQL connection string for audit logging.
        base_voxel: Base voxel cell size.
        breakline_threshold: z_std threshold for breakline protection.

    Returns:
        Dict with decimation results.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Read tile stats
    with laspy.open(str(tile_path)) as reader:
        header = reader.header
        las = reader.read()

    bounds = {
        "x_min": header.x_min, "y_min": header.y_min,
        "x_max": header.x_max, "y_max": header.y_max,
    }
    current_density = compute_density(header.point_count, bounds)
    z_std = float(np.std(las.z)) if len(las.z) > 0 else 0.0
    protected = should_protect_region(z_std, breakline_threshold)

    if protected:
        logger.info(
            "Tile %s protected (z_std=%.2f >= %.2f), copying without decimation",
            tile_path.name, z_std, breakline_threshold,
        )
        import shutil
        shutil.copy2(tile_path, output_path)
        new_density = current_density
        voxel_size = 0.0
    else:
        pipeline = generate_adaptive_pipeline(
            str(tile_path), str(output_path),
            current_density, target_density, base_voxel,
        )

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(pipeline, f)
            pipeline_path = Path(f.name)

        try:
            subprocess.run(
                ["pdal", "pipeline", str(pipeline_path)],
                capture_output=True, text=True, check=True, timeout=600,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Decimation failed for {tile_path}: {e.stderr}") from e
        finally:
            pipeline_path.unlink(missing_ok=True)

        # Compute new density
        with laspy.open(str(output_path)) as reader:
            new_count = reader.header.point_count
        new_density = compute_density(new_count, bounds)
        voxel_size = compute_adaptive_voxel_size(
            current_density, target_density, base_voxel,
        )

    preserved_ratio = new_density / current_density if current_density > 0 else 1.0

    result = {
        "input_path": str(tile_path),
        "output_path": str(output_path),
        "original_density": round(current_density, 2),
        "new_density": round(new_density, 2),
        "voxel_size": round(voxel_size, 4),
        "preserved_ratio": round(preserved_ratio, 4),
        "breakline_protected": protected,
    }

    # Audit log to database
    if conn_string:
        _log_decimation(conn_string, tile_path, result)

    logger.info("Decimated %s: %.1f → %.1f pts/m²", tile_path.name, current_density, new_density)
    return result


def _log_decimation(conn_string: str, tile_path: Path, result: dict) -> None:
    """Insert a record into the decimation_log table."""
    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            # Look up tile_id by file_path
            cur.execute(
                "SELECT id FROM lidar_tiles WHERE file_path = %s",
                (str(tile_path),),
            )
            row = cur.fetchone()
            if row is None:
                logger.warning("No tile record for %s, skipping audit log", tile_path)
                return

            tile_id = row[0]
            cur.execute(
                """
                INSERT INTO decimation_log
                    (tile_id, original_density, new_density, voxel_size,
                     preserved_ratio, breakline_protected, input_path, output_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tile_id,
                    result["original_density"],
                    result["new_density"],
                    result["voxel_size"],
                    result["preserved_ratio"],
                    result["breakline_protected"],
                    result["input_path"],
                    result["output_path"],
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
