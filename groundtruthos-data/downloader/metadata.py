"""
Metadata extraction for geospatial files.
"""
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_las_metadata(file_path: Path) -> dict:
    """Extract metadata from a LAS/LAZ file.

    Returns dict with: point_count, crs, bounds, z_range,
    point_density_estimate, file_version, point_format.
    """
    import laspy

    metadata = {"file_path": str(file_path), "format": file_path.suffix.lstrip(".")}

    with laspy.open(str(file_path)) as reader:
        header = reader.header

        metadata["point_count"] = header.point_count
        metadata["file_version"] = f"{header.version.major}.{header.version.minor}"
        metadata["point_format"] = header.point_format.id

        metadata["bounds"] = {
            "x_min": header.x_min,
            "y_min": header.y_min,
            "z_min": header.z_min,
            "x_max": header.x_max,
            "y_max": header.y_max,
            "z_max": header.z_max,
        }

        metadata["z_range"] = header.z_max - header.z_min

        # CRS extraction
        crs_str = None
        try:
            for vlr in header.vlrs:
                if vlr.record_id == 34737:
                    # GeoASCII - may contain CRS string
                    crs_str = vlr.record_data.decode("ascii", errors="ignore").strip()
                    break
            if crs_str is None:
                # Try pyproj for more robust CRS parsing
                try:
                    from pyproj import CRS as PyprojCRS
                    # laspy 2.x CRS parsing
                    parsed = header.parse_crs()
                    if parsed:
                        crs_str = str(parsed)
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"CRS extraction failed for {file_path}: {e}")

        metadata["crs"] = crs_str

        # Point density estimate
        x_extent = header.x_max - header.x_min
        y_extent = header.y_max - header.y_min
        if x_extent > 0 and y_extent > 0:
            area = x_extent * y_extent
            metadata["area_m2"] = round(area, 2)
            metadata["density_pts_m2"] = round(header.point_count / area, 2)
        else:
            metadata["area_m2"] = 0
            metadata["density_pts_m2"] = 0

        # Scale and offset
        metadata["scale"] = list(header.scales)
        metadata["offset"] = list(header.offsets)

    return metadata


def extract_raster_metadata(file_path: Path) -> dict:
    """Extract metadata from a GeoTIFF raster file."""
    import rasterio

    metadata = {"file_path": str(file_path), "format": "geotiff"}

    with rasterio.open(str(file_path)) as src:
        metadata["crs"] = src.crs.to_string() if src.crs else None
        metadata["bounds"] = {
            "left": src.bounds.left,
            "bottom": src.bounds.bottom,
            "right": src.bounds.right,
            "top": src.bounds.top,
        }
        metadata["width"] = src.width
        metadata["height"] = src.height
        metadata["bands"] = src.count
        metadata["dtype"] = str(src.dtypes[0])
        metadata["resolution"] = {"x": src.res[0], "y": src.res[1]}
        metadata["nodata"] = src.nodata
        metadata["transform"] = list(src.transform)[:6]

        # Estimate area
        if src.crs and src.crs.is_projected:
            area = (src.bounds.right - src.bounds.left) * (src.bounds.top - src.bounds.bottom)
            metadata["area_m2"] = round(area, 2)

    return metadata


def extract_metadata(file_path: Path) -> dict:
    """Auto-detect file type and extract metadata."""
    suffix = file_path.suffix.lower()
    if suffix in (".las", ".laz"):
        return extract_las_metadata(file_path)
    elif suffix in (".tif", ".tiff"):
        return extract_raster_metadata(file_path)
    else:
        return {
            "file_path": str(file_path),
            "format": suffix.lstrip("."),
            "size_bytes": file_path.stat().st_size,
        }
