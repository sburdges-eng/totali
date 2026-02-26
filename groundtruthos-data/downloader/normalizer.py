"""
CRS normalization and datum transformation for geospatial files.
"""
import logging
import shutil
import subprocess
import json
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


def detect_utm_zone(longitude: float, latitude: float) -> str:
    """Determine UTM zone EPSG code from WGS84 coordinates."""
    zone_number = int((longitude + 180) / 6) + 1
    if latitude >= 0:
        return f"EPSG:326{zone_number:02d}"
    else:
        return f"EPSG:327{zone_number:02d}"


def get_centroid_from_las(file_path: Path) -> tuple[float, float] | None:
    """Get approximate centroid of a LAS/LAZ file in WGS84.

    Returns (longitude, latitude) or None if CRS cannot be determined.
    """
    try:
        import laspy
        from pyproj import Transformer, CRS

        with laspy.open(str(file_path)) as reader:
            header = reader.header
            cx = (header.x_min + header.x_max) / 2
            cy = (header.y_min + header.y_max) / 2

            # Try to parse CRS
            parsed_crs = header.parse_crs()
            if parsed_crs is None:
                return None

            source_crs = CRS(str(parsed_crs))
            if source_crs.is_geographic:
                return (cx, cy)

            transformer = Transformer.from_crs(source_crs, CRS("EPSG:4326"), always_xy=True)
            lon, lat = transformer.transform(cx, cy)
            return (lon, lat)
    except Exception as e:
        logger.warning(f"Could not determine centroid for {file_path}: {e}")
        return None


def get_centroid_from_raster(file_path: Path) -> tuple[float, float] | None:
    """Get approximate centroid of a GeoTIFF in WGS84."""
    try:
        import rasterio
        from pyproj import Transformer, CRS

        with rasterio.open(str(file_path)) as src:
            if src.crs is None:
                return None

            cx = (src.bounds.left + src.bounds.right) / 2
            cy = (src.bounds.bottom + src.bounds.top) / 2

            if src.crs.is_geographic:
                return (cx, cy)

            transformer = Transformer.from_crs(src.crs, CRS("EPSG:4326"), always_xy=True)
            lon, lat = transformer.transform(cx, cy)
            return (lon, lat)
    except Exception as e:
        logger.warning(f"Could not determine centroid for {file_path}: {e}")
        return None


def reproject_laz(
    input_path: Path,
    output_path: Path,
    target_epsg: str | None = None,
) -> Path:
    """Reproject a LAS/LAZ file to a target CRS.

    If target_epsg is None, auto-detects appropriate UTM zone.
    Uses PDAL if available, falls back to laspy + pyproj.

    Returns path to reprojected file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Auto-detect target CRS if not specified
    if target_epsg is None:
        centroid = get_centroid_from_las(input_path)
        if centroid is None:
            raise ValueError(f"Cannot determine CRS for {input_path} and no target specified")
        target_epsg = detect_utm_zone(centroid[0], centroid[1])
        logger.info(f"Auto-detected target CRS: {target_epsg}")

    # Try PDAL first (faster, handles LAZ compression natively)
    if shutil.which("pdal"):
        return _reproject_laz_pdal(input_path, output_path, target_epsg)

    # Fallback to laspy + pyproj
    return _reproject_laz_python(input_path, output_path, target_epsg)


def _reproject_laz_pdal(input_path: Path, output_path: Path, target_epsg: str) -> Path:
    """Reproject using PDAL CLI."""
    pipeline = {
        "pipeline": [
            {"type": "readers.las", "filename": str(input_path)},
            {"type": "filters.reprojection", "out_srs": target_epsg},
            {"type": "writers.las", "filename": str(output_path), "compression": "laszip"},
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
            timeout=600,
        )
        logger.info(f"PDAL reprojection complete: {output_path}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"PDAL reprojection failed: {e.stderr}") from e
    finally:
        Path(pipeline_path).unlink(missing_ok=True)

    return output_path


def _reproject_laz_python(input_path: Path, output_path: Path, target_epsg: str) -> Path:
    """Reproject using laspy + pyproj (slower, Python-only fallback)."""
    import laspy
    import numpy as np
    from pyproj import Transformer, CRS

    logger.info(f"Using Python fallback for reprojection (PDAL not available)")

    with laspy.open(str(input_path)) as reader:
        header = reader.header
        source_crs_str = header.parse_crs()
        if source_crs_str is None:
            raise ValueError(f"No CRS in source file: {input_path}")

        source_crs = CRS(str(source_crs_str))
        target_crs = CRS(target_epsg)
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)

        # Read all points
        las = reader.read()

    x, y, z = las.x.copy(), las.y.copy(), las.z.copy()
    x_new, y_new = transformer.transform(x, y)

    # Create output with new CRS
    new_header = laspy.LasHeader(
        point_format=las.header.point_format,
        version=las.header.version,
    )
    new_header.offsets = [np.min(x_new), np.min(y_new), np.min(z)]
    new_header.scales = las.header.scales

    # Set CRS via WKT VLR
    new_header.add_crs(target_crs)

    new_las = laspy.LasData(new_header)
    new_las.x = x_new
    new_las.y = y_new
    new_las.z = z

    # Copy other dimensions
    for dim_name in las.point_format.dimension_names:
        if dim_name not in ("X", "Y", "Z"):
            try:
                setattr(new_las, dim_name, getattr(las, dim_name))
            except Exception:
                pass

    new_las.write(str(output_path))
    logger.info(f"Python reprojection complete: {output_path}")
    return output_path


def reproject_geotiff(
    input_path: Path,
    output_path: Path,
    target_epsg: str | None = None,
) -> Path:
    """Reproject a GeoTIFF to target CRS."""
    import rasterio
    from rasterio.warp import calculate_default_transform, reproject, Resampling

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if target_epsg is None:
        centroid = get_centroid_from_raster(input_path)
        if centroid is None:
            raise ValueError(f"Cannot determine CRS for {input_path}")
        target_epsg = detect_utm_zone(centroid[0], centroid[1])

    with rasterio.open(str(input_path)) as src:
        if src.crs is None:
            raise ValueError(f"No CRS in source file: {input_path}")

        from pyproj import CRS
        dst_crs = CRS(target_epsg)

        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds
        )

        kwargs = src.meta.copy()
        kwargs.update(
            {
                "crs": dst_crs.to_wkt(),
                "transform": transform,
                "width": width,
                "height": height,
            }
        )

        with rasterio.open(str(output_path), "w", **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear,
                )

    logger.info(f"GeoTIFF reprojection complete: {output_path}")
    return output_path
