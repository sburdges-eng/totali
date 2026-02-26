"""
File integrity and quality validation for downloaded datasets.
"""
import hashlib
import logging
from pathlib import Path
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    passed: bool
    file_path: str
    checks: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(64 * 1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def validate_checksum(file_path: Path, expected_checksum: str | None = None) -> bool:
    """Validate file checksum. Returns True if no expected checksum provided."""
    if expected_checksum is None:
        return True
    return compute_sha256(file_path) == expected_checksum


def validate_lidar(file_path: Path) -> ValidationResult:
    """Validate a LAS/LAZ file for integrity and quality."""
    result = ValidationResult(passed=True, file_path=str(file_path))

    try:
        import laspy
    except ImportError:
        result.warnings.append("laspy not installed, skipping LiDAR validation")
        return result

    try:
        with laspy.open(str(file_path)) as reader:
            header = reader.header

            # Point count check
            point_count = header.point_count
            result.checks["point_count"] = point_count
            if point_count == 0:
                result.passed = False
                result.errors.append("File contains zero points")
                return result

            # Bounds check
            mins = [header.x_min, header.y_min, header.z_min]
            maxs = [header.x_max, header.y_max, header.z_max]
            result.checks["bounds_min"] = mins
            result.checks["bounds_max"] = maxs

            if header.x_max < header.x_min or header.y_max < header.y_min:
                result.passed = False
                result.errors.append("Invalid bounding box: max < min")

            # Elevation sanity
            z_range = header.z_max - header.z_min
            result.checks["z_range"] = z_range
            if header.z_min < -500:
                result.warnings.append(f"Unusually low elevation: {header.z_min}m")
            if header.z_max > 9000:
                result.warnings.append(f"Unusually high elevation: {header.z_max}m")
            if z_range > 5000:
                result.passed = False
                result.errors.append(f"Implausible elevation range: {z_range}m")

            # CRS check
            try:
                vlrs = header.vlrs
                has_crs = any(
                    vlr.record_id in (2111, 2112, 34735, 34736, 34737)
                    for vlr in vlrs
                )
                if not has_crs:
                    # Check for WKT in VLR
                    has_crs = any("wkt" in vlr.description.lower() for vlr in vlrs)
                result.checks["has_crs"] = has_crs
                if not has_crs:
                    result.passed = False
                    result.errors.append("No CRS defined in file")
            except Exception as e:
                result.warnings.append(f"Could not check CRS: {e}")

            # Point density estimate (approximate)
            x_extent = header.x_max - header.x_min
            y_extent = header.y_max - header.y_min
            if x_extent > 0 and y_extent > 0:
                area_m2 = x_extent * y_extent
                density = point_count / area_m2
                result.checks["estimated_density_pts_m2"] = round(density, 2)
                if density < 0.5:
                    result.warnings.append(
                        f"Low point density: {density:.2f} pts/m2"
                    )

    except Exception as e:
        result.passed = False
        result.errors.append(f"Failed to read file: {e}")

    return result


def validate_geotiff(file_path: Path) -> ValidationResult:
    """Validate a GeoTIFF file for integrity and quality."""
    result = ValidationResult(passed=True, file_path=str(file_path))

    try:
        import rasterio
    except ImportError:
        result.warnings.append("rasterio not installed, skipping GeoTIFF validation")
        return result

    try:
        with rasterio.open(str(file_path)) as src:
            result.checks["width"] = src.width
            result.checks["height"] = src.height
            result.checks["bands"] = src.count
            result.checks["dtype"] = str(src.dtypes[0])
            result.checks["resolution"] = src.res

            if src.width == 0 or src.height == 0:
                result.passed = False
                result.errors.append("Raster has zero dimensions")

            if src.crs is None:
                result.passed = False
                result.errors.append("No CRS defined")
            else:
                result.checks["crs"] = src.crs.to_string()

            result.checks["bounds"] = {
                "left": src.bounds.left,
                "bottom": src.bounds.bottom,
                "right": src.bounds.right,
                "top": src.bounds.top,
            }

            # Check for nodata
            result.checks["nodata"] = src.nodata

            # Read a small sample to verify data is not all nodata
            try:
                window = rasterio.windows.Window(0, 0, min(100, src.width), min(100, src.height))
                sample = src.read(1, window=window)
                if src.nodata is not None:
                    valid_count = (sample != src.nodata).sum()
                    result.checks["sample_valid_pixels"] = int(valid_count)
                    if valid_count == 0:
                        result.warnings.append("Sample region is entirely nodata")
            except Exception as e:
                result.warnings.append(f"Could not read sample data: {e}")

    except Exception as e:
        result.passed = False
        result.errors.append(f"Failed to read file: {e}")

    return result


def validate_file(file_path: Path) -> ValidationResult:
    """Auto-detect file type and validate."""
    suffix = file_path.suffix.lower()
    if suffix in (".las", ".laz"):
        return validate_lidar(file_path)
    elif suffix in (".tif", ".tiff"):
        return validate_geotiff(file_path)
    else:
        return ValidationResult(
            passed=True,
            file_path=str(file_path),
            warnings=[f"No validator for file type: {suffix}"],
        )
