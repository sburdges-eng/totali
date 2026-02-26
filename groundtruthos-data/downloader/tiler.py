"""
Automatic tile partitioning for analysis-ready datasets.

Splits large LiDAR files into uniform 50m x 50m tiles with overlap,
computes height maps for JEPA training input.
"""
import json
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_TILE_SIZE_M = 50.0
DEFAULT_OVERLAP_M = 5.0
DEFAULT_GRID_RESOLUTION_M = 0.5


def tile_lidar_file(
    input_path: Path,
    output_dir: Path,
    tile_size_m: float = DEFAULT_TILE_SIZE_M,
    overlap_m: float = DEFAULT_OVERLAP_M,
    min_points: int = 100,
) -> list[dict]:
    """Split a LAS/LAZ file into uniform tiles.

    Args:
        input_path: Path to source LAS/LAZ file.
        output_dir: Directory for output tiles.
        tile_size_m: Tile edge length in meters.
        overlap_m: Overlap buffer on each side.
        min_points: Minimum points per tile (skip sparse tiles).

    Returns:
        List of tile metadata dicts.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if shutil.which("pdal"):
        return _tile_with_pdal(input_path, output_dir, tile_size_m, overlap_m, min_points)
    return _tile_with_laspy(input_path, output_dir, tile_size_m, overlap_m, min_points)


def _tile_with_pdal(
    input_path: Path,
    output_dir: Path,
    tile_size: float,
    overlap: float,
    min_points: int,
) -> list[dict]:
    """Tile using PDAL splitter filter."""
    # PDAL's filters.splitter handles the heavy lifting
    pipeline = {
        "pipeline": [
            {"type": "readers.las", "filename": str(input_path)},
            {"type": "filters.splitter", "length": tile_size, "buffer": overlap},
            {
                "type": "writers.las",
                "filename": str(output_dir / "tile_#.laz"),
                "compression": "laszip",
            },
        ]
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(pipeline, f)
        pipeline_path = f.name

    try:
        result = subprocess.run(
            ["pdal", "pipeline", pipeline_path],
            capture_output=True,
            text=True,
            check=True,
            timeout=1800,  # 30 min for large files
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"PDAL tiling failed: {e.stderr}") from e
    finally:
        Path(pipeline_path).unlink(missing_ok=True)

    # Collect tile metadata
    tiles = []
    for tile_path in sorted(output_dir.glob("tile_*.laz")):
        meta = _get_tile_meta(tile_path, min_points)
        if meta:
            tiles.append(meta)
        else:
            tile_path.unlink()  # Remove sparse tiles

    logger.info(f"PDAL tiling produced {len(tiles)} tiles from {input_path.name}")
    return tiles


def _tile_with_laspy(
    input_path: Path,
    output_dir: Path,
    tile_size: float,
    overlap: float,
    min_points: int,
) -> list[dict]:
    """Tile using laspy (Python fallback, slower but no PDAL dependency)."""
    import laspy

    with laspy.open(str(input_path)) as reader:
        las = reader.read()

    x = las.x
    y = las.y

    x_min, x_max = float(x.min()), float(x.max())
    y_min, y_max = float(y.min()), float(y.max())

    # Compute tile grid
    n_tiles_x = int(np.ceil((x_max - x_min) / tile_size))
    n_tiles_y = int(np.ceil((y_max - y_min) / tile_size))

    logger.info(
        f"Tiling {input_path.name}: {n_tiles_x}x{n_tiles_y} grid "
        f"({n_tiles_x * n_tiles_y} potential tiles)"
    )

    tiles = []
    tile_num = 0

    for ix in range(n_tiles_x):
        for iy in range(n_tiles_y):
            # Tile bounds with overlap
            tx_min = x_min + ix * tile_size - overlap
            tx_max = x_min + (ix + 1) * tile_size + overlap
            ty_min = y_min + iy * tile_size - overlap
            ty_max = y_min + (iy + 1) * tile_size + overlap

            # Select points in tile
            mask = (x >= tx_min) & (x < tx_max) & (y >= ty_min) & (y < ty_max)
            count = int(mask.sum())

            if count < min_points:
                continue

            # Write tile
            tile_name = f"tile_{tile_num:05d}.laz"
            tile_path = output_dir / tile_name

            new_header = laspy.LasHeader(
                point_format=las.header.point_format,
                version=las.header.version,
            )
            new_header.scales = las.header.scales
            new_header.offsets = las.header.offsets

            # Copy CRS VLRs
            for vlr in las.header.vlrs:
                new_header.vlrs.append(vlr)

            new_las = laspy.LasData(new_header)
            new_las.points = las.points[mask]
            new_las.write(str(tile_path))

            meta = {
                "tile_id": tile_name,
                "file_path": str(tile_path),
                "grid_index": (ix, iy),
                "bounds": {
                    "x_min": tx_min + overlap,  # core bounds (excluding overlap)
                    "y_min": ty_min + overlap,
                    "x_max": tx_max - overlap,
                    "y_max": ty_max - overlap,
                },
                "bounds_with_overlap": {
                    "x_min": tx_min,
                    "y_min": ty_min,
                    "x_max": tx_max,
                    "y_max": ty_max,
                },
                "point_count": count,
                "area_m2": tile_size * tile_size,
                "density_pts_m2": round(count / (tile_size * tile_size), 2),
            }
            tiles.append(meta)
            tile_num += 1

    logger.info(f"laspy tiling produced {len(tiles)} tiles from {input_path.name}")
    return tiles


def _get_tile_meta(tile_path: Path, min_points: int) -> dict | None:
    """Extract metadata from a tiled file. Returns None if below min_points."""
    try:
        import laspy
        with laspy.open(str(tile_path)) as reader:
            header = reader.header
            if header.point_count < min_points:
                return None

            x_extent = header.x_max - header.x_min
            y_extent = header.y_max - header.y_min
            area = x_extent * y_extent if x_extent > 0 and y_extent > 0 else 0

            return {
                "tile_id": tile_path.name,
                "file_path": str(tile_path),
                "bounds": {
                    "x_min": header.x_min,
                    "y_min": header.y_min,
                    "x_max": header.x_max,
                    "y_max": header.y_max,
                    "z_min": header.z_min,
                    "z_max": header.z_max,
                },
                "point_count": header.point_count,
                "area_m2": round(area, 2),
                "density_pts_m2": round(header.point_count / area, 2) if area > 0 else 0,
            }
    except Exception as e:
        logger.warning(f"Could not read tile {tile_path}: {e}")
        return None


def generate_height_map(
    tile_path: Path,
    output_path: Path,
    resolution_m: float = DEFAULT_GRID_RESOLUTION_M,
    tile_size_m: float = DEFAULT_TILE_SIZE_M,
) -> dict:
    """Convert a LiDAR tile to a gridded height map for JEPA input.

    Generates a 2-channel numpy array:
      Channel 0: Elevation (meters, relative to tile minimum)
      Channel 1: Point density (count per cell)

    Args:
        tile_path: Path to LAS/LAZ tile.
        output_path: Path for output .npy file.
        resolution_m: Grid cell size in meters.
        tile_size_m: Expected tile size (for consistent grid dimensions).

    Returns:
        Metadata dict about the height map.
    """
    import laspy

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with laspy.open(str(tile_path)) as reader:
        las = reader.read()

    x = np.array(las.x)
    y = np.array(las.y)
    z = np.array(las.z)

    # Grid dimensions
    grid_size = int(np.ceil(tile_size_m / resolution_m))

    x_min, y_min = float(x.min()), float(y.min())
    z_min = float(z.min())

    # Compute grid indices
    xi = np.clip(((x - x_min) / resolution_m).astype(int), 0, grid_size - 1)
    yi = np.clip(((y - y_min) / resolution_m).astype(int), 0, grid_size - 1)

    # Initialize grids
    elevation = np.full((grid_size, grid_size), np.nan, dtype=np.float32)
    density = np.zeros((grid_size, grid_size), dtype=np.float32)
    z_sum = np.zeros((grid_size, grid_size), dtype=np.float64)
    z_count = np.zeros((grid_size, grid_size), dtype=np.int32)

    # Accumulate (mean elevation per cell)
    np.add.at(z_sum, (yi, xi), z - z_min)
    np.add.at(z_count, (yi, xi), 1)

    valid = z_count > 0
    elevation[valid] = (z_sum[valid] / z_count[valid]).astype(np.float32)
    density[valid] = z_count[valid].astype(np.float32)

    # Stack into 2-channel array: (H, W, 2)
    height_map = np.stack([elevation, density], axis=-1)

    # Save
    np.save(str(output_path), height_map)

    # Metadata
    valid_cells = int(valid.sum())
    total_cells = grid_size * grid_size

    meta = {
        "tile_path": str(tile_path),
        "height_map_path": str(output_path),
        "grid_size": grid_size,
        "resolution_m": resolution_m,
        "channels": ["elevation_relative", "point_density"],
        "z_offset": z_min,
        "x_origin": x_min,
        "y_origin": y_min,
        "valid_cells": valid_cells,
        "total_cells": total_cells,
        "coverage_pct": round(100 * valid_cells / total_cells, 1),
        "elevation_range": float(np.nanmax(elevation) - np.nanmin(elevation[valid])) if valid_cells > 0 else 0,
        "mean_density": float(density[valid].mean()) if valid_cells > 0 else 0,
    }

    return meta


def batch_generate_height_maps(
    tile_dir: Path,
    output_dir: Path,
    resolution_m: float = DEFAULT_GRID_RESOLUTION_M,
    tile_size_m: float = DEFAULT_TILE_SIZE_M,
) -> list[dict]:
    """Generate height maps for all tiles in a directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results = []

    tile_files = sorted(tile_dir.glob("*.laz")) + sorted(tile_dir.glob("*.las"))

    for tile_path in tile_files:
        output_path = output_dir / f"{tile_path.stem}.npy"
        try:
            meta = generate_height_map(tile_path, output_path, resolution_m, tile_size_m)
            results.append(meta)
        except Exception as e:
            logger.warning(f"Height map generation failed for {tile_path.name}: {e}")

    logger.info(f"Generated {len(results)} height maps from {len(tile_files)} tiles")
    return results
