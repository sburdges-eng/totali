"""
PDAL-based tiling pipeline for LiDAR point clouds.

Splits large LAS/LAZ files into manageable tiles, extracts per-tile
statistics, and registers results in PostGIS.
"""
import json
import logging
import math
import subprocess
import tempfile
from collections import Counter
from pathlib import Path

import laspy
import numpy as np
import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)

DEFAULT_TILE_LENGTH = 500  # metres
DEFAULT_VOXEL_SIZE = 0.5   # metres


def run_pdal_tiling(
    input_file: Path,
    output_dir: Path,
    target_epsg: str = "EPSG:4326",
    tile_length: float = DEFAULT_TILE_LENGTH,
    voxel_size: float = DEFAULT_VOXEL_SIZE,
) -> list[Path]:
    """Run PDAL pipeline to reproject, decimate, and split a point cloud into tiles.

    Args:
        input_file: Path to input LAS/LAZ.
        output_dir: Directory where tile files are written.
        target_epsg: Target coordinate reference system.
        tile_length: Splitter tile edge length in metres.
        voxel_size: Voxel grid cell size for initial thinning.

    Returns:
        List of paths to generated tile files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_pattern = str(output_dir / "tile_#.laz")

    pipeline = {
        "pipeline": [
            {"type": "readers.las", "filename": str(input_file)},
            {"type": "filters.reprojection", "out_srs": target_epsg},
            {"type": "filters.voxelgrid", "cell": voxel_size},
            {"type": "filters.splitter", "length": tile_length},
            {
                "type": "writers.las",
                "filename": output_pattern,
                "compression": "laszip",
            },
        ]
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(pipeline, f)
        pipeline_path = Path(f.name)

    try:
        result = subprocess.run(
            ["pdal", "pipeline", str(pipeline_path)],
            capture_output=True,
            text=True,
            check=True,
            timeout=1800,
        )
        logger.info("PDAL tiling complete: %s", result.stdout.strip())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"PDAL tiling failed: {e.stderr}") from e
    finally:
        pipeline_path.unlink(missing_ok=True)

    tiles = sorted(output_dir.glob("tile_*.laz"))
    logger.info("Generated %d tiles in %s", len(tiles), output_dir)
    return tiles


def extract_tile_stats(tile_path: Path) -> dict:
    """Extract spatial statistics from a single tile using laspy.

    Returns:
        Dict with bounds, point_count, density, z_std, and classification
        distribution.
    """
    with laspy.open(str(tile_path)) as reader:
        header = reader.header
        las = reader.read()

    x_min, y_min = header.x_min, header.y_min
    x_max, y_max = header.x_max, header.y_max
    z_values = las.z

    x_extent = x_max - x_min
    y_extent = y_max - y_min
    area = x_extent * y_extent if (x_extent > 0 and y_extent > 0) else 0.0

    # Classification distribution
    class_counts = Counter(las.classification.astype(int))
    classification = {str(k): int(v) for k, v in class_counts.items()}

    return {
        "file_path": str(tile_path),
        "bounds": {
            "x_min": float(x_min),
            "y_min": float(y_min),
            "x_max": float(x_max),
            "y_max": float(y_max),
        },
        "point_count": int(header.point_count),
        "density_pts_m2": round(header.point_count / area, 2) if area > 0 else 0.0,
        "z_min": float(header.z_min),
        "z_max": float(header.z_max),
        "z_std": float(np.std(z_values)) if len(z_values) > 0 else 0.0,
        "classification": classification,
    }


def register_tiles_postgis(
    output_dir: Path,
    dataset_id: str,
    conn_string: str,
) -> list[str]:
    """Register all tiles in a directory into PostGIS lidar_tiles table.

    Args:
        output_dir: Directory containing tile LAZ files.
        dataset_id: UUID of the parent dataset record.
        conn_string: PostgreSQL connection string.

    Returns:
        List of inserted tile UUIDs.
    """
    tile_paths = sorted(output_dir.glob("tile_*.laz"))
    if not tile_paths:
        logger.warning("No tiles found in %s", output_dir)
        return []

    tile_ids = []
    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            for tile_path in tile_paths:
                stats = extract_tile_stats(tile_path)
                b = stats["bounds"]

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
                        str(tile_path),
                        b["x_min"], b["y_min"], b["x_max"], b["y_max"],
                        stats["point_count"],
                        stats["density_pts_m2"],
                        stats["z_min"],
                        stats["z_max"],
                        stats["z_std"],
                        Json(stats["classification"]),
                    ),
                )
                tile_id = cur.fetchone()[0]
                tile_ids.append(str(tile_id))
                logger.info("Registered tile %s → %s", tile_path.name, tile_id)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Registered %d tiles for dataset %s", len(tile_ids), dataset_id)
    return tile_ids
