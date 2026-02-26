"""
Parallelized bulk download engine with rate limiting, resume, and progress tracking.
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from .client import DatasetClient
from .validator import validate_file, ValidationResult
from .metadata import extract_metadata
from .tile_listing import TileInfo

logger = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    """Result of a single tile download + validation."""
    tile_info: TileInfo
    success: bool
    file_path: Path | None = None
    checksum: str | None = None
    metadata: dict | None = None
    validation: ValidationResult | None = None
    error: str | None = None
    elapsed_seconds: float = 0.0


@dataclass
class BulkDownloadReport:
    """Summary report for a bulk download batch."""
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    total_bytes: int = 0
    elapsed_seconds: float = 0.0
    results: list[DownloadResult] = field(default_factory=list)

    @property
    def failure_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.failed / self.total


class BulkDownloader:
    """Parallel bulk downloader with validation and metadata extraction.

    Features:
    - Configurable parallelism (max_workers)
    - Per-source rate limiting
    - Skip already-downloaded tiles (via registry check)
    - Automatic validation post-download
    - Metadata extraction
    - Resume interrupted batches
    """

    def __init__(
        self,
        dest_root: Path,
        max_workers: int = 4,
        rate_limit_rps: float = 1.0,
        skip_existing: bool = True,
    ):
        self.dest_root = dest_root
        self.max_workers = max_workers
        self.rate_limit_rps = rate_limit_rps
        self.skip_existing = skip_existing
        self.client = DatasetClient(rate_limit_rps=rate_limit_rps)

    def download_batch(
        self,
        tiles: list[TileInfo],
        subdir: str = "raw",
        validate: bool = True,
        extract_meta: bool = True,
        registry=None,
    ) -> BulkDownloadReport:
        """Download a batch of tiles in parallel.

        Args:
            tiles: List of TileInfo objects to download.
            subdir: Subdirectory under dest_root for this batch.
            validate: Run quality validation on each file.
            extract_meta: Extract metadata from each file.
            registry: Optional DatasetRegistry for dedup and registration.

        Returns:
            BulkDownloadReport with results.
        """
        report = BulkDownloadReport(total=len(tiles))
        start_time = time.monotonic()

        # Filter out already-downloaded tiles
        tiles_to_download = []
        for tile in tiles:
            if self.skip_existing and registry and registry.tile_exists(tile.source, tile.tile_id):
                report.skipped += 1
                logger.debug(f"Skipping existing tile: {tile.source}/{tile.tile_id}")
                continue
            tiles_to_download.append(tile)

        if not tiles_to_download:
            logger.info(f"All {len(tiles)} tiles already downloaded, nothing to do")
            report.elapsed_seconds = time.monotonic() - start_time
            return report

        logger.info(
            f"Downloading {len(tiles_to_download)} tiles "
            f"({report.skipped} skipped) with {self.max_workers} workers"
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            for tile in tiles_to_download:
                future = executor.submit(
                    self._download_single,
                    tile,
                    subdir=subdir,
                    validate=validate,
                    extract_meta=extract_meta,
                )
                futures[future] = tile

            for future in as_completed(futures):
                result = future.result()
                report.results.append(result)

                if result.success:
                    report.succeeded += 1
                    if result.file_path:
                        report.total_bytes += result.file_path.stat().st_size
                else:
                    report.failed += 1
                    logger.warning(
                        f"Failed: {result.tile_info.tile_id} - {result.error}"
                    )

        report.elapsed_seconds = time.monotonic() - start_time
        logger.info(
            f"Bulk download complete: {report.succeeded}/{report.total} succeeded, "
            f"{report.failed} failed, {report.skipped} skipped, "
            f"{report.total_bytes / (1024**3):.2f} GB in {report.elapsed_seconds:.1f}s"
        )
        return report

    def _download_single(
        self,
        tile: TileInfo,
        subdir: str,
        validate: bool,
        extract_meta: bool,
    ) -> DownloadResult:
        """Download, validate, and extract metadata for a single tile."""
        start = time.monotonic()

        # Determine destination path
        source_dir = self.dest_root / subdir / tile.source
        filename = self._make_filename(tile)
        dest_path = source_dir / filename

        try:
            # Download
            dl_result = self.client.download_file(
                file_url=tile.download_url,
                dest_path=dest_path,
                expected_checksum=tile.checksum,
            )

            result = DownloadResult(
                tile_info=tile,
                success=True,
                file_path=dest_path,
                checksum=dl_result["sha256"],
            )

            # Validate
            if validate:
                validation = validate_file(dest_path)
                result.validation = validation
                if not validation.passed:
                    result.success = False
                    result.error = "; ".join(validation.errors)
                    logger.warning(
                        f"Validation failed for {tile.tile_id}: {result.error}"
                    )

            # Extract metadata
            if extract_meta and result.success:
                try:
                    result.metadata = extract_metadata(dest_path)
                except Exception as e:
                    logger.warning(f"Metadata extraction failed for {tile.tile_id}: {e}")

            result.elapsed_seconds = time.monotonic() - start
            return result

        except Exception as e:
            return DownloadResult(
                tile_info=tile,
                success=False,
                error=str(e),
                elapsed_seconds=time.monotonic() - start,
            )

    def _make_filename(self, tile: TileInfo) -> str:
        """Generate canonical filename for a tile."""
        parts = [tile.source]
        if tile.region:
            parts.append(tile.region.replace(" ", "_")[:30])
        if tile.year:
            parts.append(tile.year)
        if tile.resolution:
            parts.append(tile.resolution.replace(" ", ""))

        # Use tile_id as the unique component
        safe_id = tile.tile_id.replace("/", "_").replace("\\", "_").replace(" ", "_")
        parts.append(safe_id)

        ext = tile.format or "laz"
        return f"{'_'.join(parts)}.{ext}"

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
