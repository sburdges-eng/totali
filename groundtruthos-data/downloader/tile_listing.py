"""
API tile listing for public geospatial data sources.

Each source has a list_tiles() function that returns available tile metadata
for a given bounding box.
"""
import logging
import time
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


@dataclass
class TileInfo:
    """Metadata for a single downloadable tile."""
    source: str
    tile_id: str
    download_url: str
    format: str
    size_bytes: int | None = None
    crs: str | None = None
    date: str | None = None
    year: str | None = None
    region: str | None = None
    resolution: str | None = None
    checksum: str | None = None
    bounds_wgs84: dict | None = None  # {x_min, y_min, x_max, y_max}
    extra: dict | None = None


class USGSTNMLister:
    """List available tiles from USGS The National Map API."""

    API_URL = "https://tnmaccess.nationalmap.gov/api/v1/products"
    BATCH_SIZE = 50

    def __init__(self, rate_limit_rps: float = 1.0):
        self.session = requests.Session()
        self.min_interval = 1.0 / rate_limit_rps
        self._last_request = 0.0

    def _rate_wait(self):
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request = time.monotonic()

    def list_tiles(
        self,
        bbox: tuple[float, float, float, float],
        dataset: str = "Lidar Point Cloud (LPC)",
        prod_format: str = "LAZ",
    ) -> list[TileInfo]:
        """List available tiles within a WGS84 bounding box.

        Args:
            bbox: (min_lon, min_lat, max_lon, max_lat) in WGS84.
            dataset: USGS dataset name.
            prod_format: File format filter.

        Returns:
            List of TileInfo objects.
        """
        tiles = []
        offset = 0

        while True:
            self._rate_wait()

            params = {
                "datasets": dataset,
                "bbox": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
                "prodFormats": prod_format,
                "max": self.BATCH_SIZE,
                "offset": offset,
            }

            try:
                response = self.session.get(self.API_URL, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as e:
                logger.error(f"USGS API request failed at offset {offset}: {e}")
                break

            items = data.get("items", [])
            if not items:
                break

            for item in items:
                url = item.get("downloadURL")
                if not url:
                    continue

                pub_date = item.get("publicationDate", "")
                bounding = item.get("boundingBox", {})

                tiles.append(
                    TileInfo(
                        source="usgs_3dep",
                        tile_id=item.get("sourceId", item.get("title", url.split("/")[-1])),
                        download_url=url,
                        format=prod_format.lower(),
                        size_bytes=item.get("sizeInBytes"),
                        crs=item.get("sourceSpatialReference"),
                        date=pub_date,
                        year=pub_date[:4] if pub_date else None,
                        region=item.get("extent", ""),
                        resolution=item.get("qualityLevel", ""),
                        bounds_wgs84={
                            "x_min": bounding.get("minX"),
                            "y_min": bounding.get("minY"),
                            "x_max": bounding.get("maxX"),
                            "y_max": bounding.get("maxY"),
                        } if bounding else None,
                        extra={
                            "title": item.get("title"),
                            "moreInfo": item.get("moreInfo"),
                        },
                    )
                )

            total = data.get("total", 0)
            offset += self.BATCH_SIZE
            logger.info(f"Listed {min(offset, total)}/{total} USGS tiles")

            if offset >= total:
                break

        return tiles

    def list_dem_tiles(
        self,
        bbox: tuple[float, float, float, float],
        resolution: str = "1 meter",
    ) -> list[TileInfo]:
        """List DEM tiles. Resolution: '1 meter', '1/3 arc-second', '1 arc-second'."""
        return self.list_tiles(
            bbox=bbox,
            dataset=f"Digital Elevation Model (DEM) {resolution}",
            prod_format="GeoTIFF",
        )


class OpenTopographyLister:
    """List available datasets from OpenTopography catalog API."""

    API_URL = "https://portal.opentopography.org/API/otCatalog"

    def __init__(self, api_key: str = "", rate_limit_rps: float = 0.5):
        self.api_key = api_key
        self.session = requests.Session()
        self.min_interval = 1.0 / rate_limit_rps
        self._last_request = 0.0

    def _rate_wait(self):
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request = time.monotonic()

    def list_tiles(
        self,
        bbox: tuple[float, float, float, float],
        dataset_type: str = "lidar",
    ) -> list[TileInfo]:
        """List datasets from OpenTopography within bounding box.

        NOTE: OpenTopography returns datasets, not individual tiles.
        Each dataset may contain many tiles.
        """
        self._rate_wait()

        params = {
            "minx": bbox[0],
            "miny": bbox[1],
            "maxx": bbox[2],
            "maxy": bbox[3],
            "detail": True,
            "outputFormat": "json",
            "include_federated": True,
        }
        if self.api_key:
            params["API_Key"] = self.api_key

        try:
            response = self.session.get(self.API_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error(f"OpenTopography API request failed: {e}")
            return []

        tiles = []
        datasets = data.get("Datasets", [])
        for ds in datasets:
            # IMPORTANT: Check license per dataset
            license_info = ds.get("license", "")
            tiles.append(
                TileInfo(
                    source="opentopography",
                    tile_id=ds.get("Dataset", {}).get("shortname", ds.get("name", "")),
                    download_url=ds.get("Dataset", {}).get("url", ""),
                    format="laz",
                    date=ds.get("Dataset", {}).get("dateCollected", ""),
                    bounds_wgs84={
                        "x_min": ds.get("Dataset", {}).get("westBoundLongitude"),
                        "y_min": ds.get("Dataset", {}).get("southBoundLatitude"),
                        "x_max": ds.get("Dataset", {}).get("eastBoundLongitude"),
                        "y_max": ds.get("Dataset", {}).get("northBoundLatitude"),
                    },
                    extra={
                        "license": license_info,
                        "requires_license_review": True,
                        "description": ds.get("Dataset", {}).get("longname", ""),
                    },
                )
            )

        return tiles


class NOAADigitalCoastLister:
    """List LiDAR datasets from NOAA Digital Coast.

    Note: NOAA doesn't have a clean tile-level API. This lists
    dataset-level entries from their directory structure.
    """

    BASE_URL = "https://coast.noaa.gov/htdata/lidar/"

    def __init__(self, rate_limit_rps: float = 1.0):
        self.session = requests.Session()
        self.min_interval = 1.0 / rate_limit_rps
        self._last_request = 0.0

    def _rate_wait(self):
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request = time.monotonic()

    def list_project_directories(self) -> list[str]:
        """List available project directories from NOAA.

        Returns list of project directory URLs.
        This is a rough listing — NOAA structure requires crawling.
        """
        self._rate_wait()
        try:
            response = self.session.get(self.BASE_URL, timeout=60)
            response.raise_for_status()
            # Parse HTML directory listing for hrefs
            import re
            links = re.findall(r'href="([^"]+/)"', response.text)
            return [f"{self.BASE_URL}{link}" for link in links if not link.startswith("..")]
        except requests.RequestException as e:
            logger.error(f"NOAA directory listing failed: {e}")
            return []
