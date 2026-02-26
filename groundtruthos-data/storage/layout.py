"""
Canonical storage layout for GroundTruthOS datasets.

Enforces consistent directory structure and naming conventions.
"""
from pathlib import Path


class StorageLayout:
    """Manages the canonical dataset directory structure.

    Layout:
        {root}/
            raw/{source}/          - As-downloaded, unmodified files
            normalized/{source}/   - Reprojected, datum-corrected files
            tiled/lidar/           - 50m x 50m analysis tiles (LAZ)
            tiled/height_maps/     - Gridded numpy arrays for JEPA
            geotech/raw_logs/      - Original geotech files
            geotech/parsed/        - Structured JSON geotech data
            soil/                  - Soil classification datasets
            metadata/licenses/     - Per-source license records
            metadata/manifests/    - Tile manifests
            metadata/quality/      - QC reports
    """

    SUBDIRS = [
        "raw",
        "normalized",
        "tiled/lidar",
        "tiled/height_maps",
        "geotech/raw_logs",
        "geotech/parsed",
        "soil",
        "metadata/licenses",
        "metadata/manifests",
        "metadata/quality",
    ]

    def __init__(self, root: Path):
        self.root = Path(root)

    def initialize(self):
        """Create all directories in the canonical layout."""
        for subdir in self.SUBDIRS:
            (self.root / subdir).mkdir(parents=True, exist_ok=True)

    def raw_dir(self, source: str) -> Path:
        """Directory for raw downloads from a source."""
        path = self.root / "raw" / source
        path.mkdir(parents=True, exist_ok=True)
        return path

    def normalized_dir(self, source: str) -> Path:
        """Directory for normalized/reprojected files."""
        path = self.root / "normalized" / source
        path.mkdir(parents=True, exist_ok=True)
        return path

    def tiled_lidar_dir(self) -> Path:
        return self.root / "tiled" / "lidar"

    def height_maps_dir(self) -> Path:
        return self.root / "tiled" / "height_maps"

    def geotech_raw_dir(self) -> Path:
        return self.root / "geotech" / "raw_logs"

    def geotech_parsed_dir(self) -> Path:
        return self.root / "geotech" / "parsed"

    def soil_dir(self) -> Path:
        return self.root / "soil"

    def license_dir(self) -> Path:
        return self.root / "metadata" / "licenses"

    def manifest_dir(self) -> Path:
        return self.root / "metadata" / "manifests"

    def quality_dir(self) -> Path:
        return self.root / "metadata" / "quality"

    @staticmethod
    def canonical_filename(
        source: str,
        region: str = "",
        year: str = "",
        resolution: str = "",
        tile_id: str = "",
        ext: str = "laz",
    ) -> str:
        """Generate canonical filename.

        Pattern: {source}_{region}_{year}_{resolution}_{tile_id}.{ext}
        Empty parts are omitted.
        """
        parts = [source]
        if region:
            parts.append(region.replace(" ", "_")[:30])
        if year:
            parts.append(str(year))
        if resolution:
            parts.append(resolution.replace(" ", ""))
        if tile_id:
            safe_id = tile_id.replace("/", "_").replace("\\", "_").replace(" ", "_")
            parts.append(safe_id)

        return f"{'_'.join(parts)}.{ext}"
