"""
GroundTruthOS Data Acquisition Pipeline

Main entry point for downloading, validating, normalizing, and registering
public geospatial datasets.

Usage:
    # List available USGS LiDAR tiles for a region
    python main.py list --source usgs_3dep --bbox=-105.5,39.5,-104.5,40.0

    # Download tiles for a region
    python main.py download --source usgs_3dep --bbox=-105.5,39.5,-104.5,40.0

    # Tile downloaded files for JEPA training
    python main.py tile --input datasets/raw/usgs_3dep/ --output datasets/tiled/

    # Generate height maps from tiles
    python main.py heightmaps --input datasets/tiled/lidar/ --output datasets/tiled/height_maps/

    # Full pipeline: list -> download -> validate -> normalize -> tile -> heightmaps
    python main.py pipeline --source usgs_3dep --bbox=-105.5,39.5,-104.5,40.0

    # Initialize database schema
    python main.py init-db

    # Show catalog stats
    python main.py stats
"""
import argparse
import json
import logging
import sys
from pathlib import Path

from config import load_sources
from downloader.client import DatasetClient
from downloader.tile_listing import USGSTNMLister, OpenTopographyLister
from downloader.bulk_downloader import BulkDownloader
from downloader.validator import validate_file
from downloader.metadata import extract_metadata
from downloader.normalizer import reproject_laz, reproject_geotiff
from downloader.registry import DatasetRegistry
from downloader.tiler import tile_lidar_file, batch_generate_height_maps
from storage.layout import StorageLayout
from compliance.compliance_log import (
    ComplianceLogger,
    check_source_compliance,
    AUTO_APPROVED_SOURCES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("groundtruthos")

# Default paths
DEFAULT_DATA_ROOT = Path("datasets")
DEFAULT_COMPLIANCE_LOG = Path("compliance_log.jsonl")


def parse_bbox(bbox_str: str) -> tuple[float, float, float, float]:
    """Parse 'min_lon,min_lat,max_lon,max_lat' string."""
    parts = [float(x.strip()) for x in bbox_str.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be min_lon,min_lat,max_lon,max_lat")
    return tuple(parts)


def get_lister(source: str, sources_config: dict):
    """Get the appropriate tile lister for a source."""
    if source in ("usgs_3dep", "usgs_dem"):
        rate = sources_config.get(source, {}).get("rate_limit_rps", 1.0)
        return USGSTNMLister(rate_limit_rps=rate)
    elif source == "opentopography":
        rate = sources_config.get(source, {}).get("rate_limit_rps", 0.5)
        return OpenTopographyLister(rate_limit_rps=rate)
    else:
        raise ValueError(
            f"No tile lister implemented for source: {source}. "
            f"Available: usgs_3dep, usgs_dem, opentopography"
        )


def cmd_list(args):
    """List available tiles for a source and bbox."""
    sources = load_sources()
    bbox = parse_bbox(args.bbox)

    lister = get_lister(args.source, sources)

    if args.source == "usgs_dem":
        tiles = lister.list_dem_tiles(bbox=bbox)
    else:
        tiles = lister.list_tiles(bbox=bbox)

    logger.info(f"Found {len(tiles)} tiles for {args.source} in bbox {bbox}")

    for tile in tiles:
        size_mb = tile.size_bytes / (1024 * 1024) if tile.size_bytes else 0
        print(f"  {tile.tile_id} | {size_mb:.1f} MB | {tile.date or 'no date'}")

    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w") as f:
            json.dump(
                [
                    {
                        "source": t.source,
                        "tile_id": t.tile_id,
                        "url": t.download_url,
                        "size_bytes": t.size_bytes,
                        "date": t.date,
                        "bounds": t.bounds_wgs84,
                    }
                    for t in tiles
                ],
                f,
                indent=2,
            )
        logger.info(f"Tile list saved to {output_path}")


def cmd_download(args):
    """Download tiles for a source and bbox."""
    sources = load_sources()
    source_config = sources.get(args.source, {})
    bbox = parse_bbox(args.bbox)

    # Compliance check
    status, reason = check_source_compliance(args.source)
    if status == "blocked":
        logger.error(f"Source {args.source} is BLOCKED: {reason}")
        sys.exit(1)
    elif status == "requires_review":
        logger.warning(f"Source {args.source} requires license review: {reason}")
        if not args.force:
            logger.error("Use --force to proceed (you accept responsibility for license review)")
            sys.exit(1)

    # List tiles
    lister = get_lister(args.source, sources)
    if args.source == "usgs_dem":
        tiles = lister.list_dem_tiles(bbox=bbox)
    else:
        tiles = lister.list_tiles(bbox=bbox)

    if not tiles:
        logger.info("No tiles found")
        return

    logger.info(f"Found {len(tiles)} tiles, starting download")

    # Setup
    layout = StorageLayout(Path(args.data_root))
    layout.initialize()

    compliance = ComplianceLogger(Path(args.compliance_log))
    registry = None
    if args.register:
        registry = DatasetRegistry()

    # Download
    max_workers = source_config.get("max_concurrent", 4)
    rate = source_config.get("rate_limit_rps", 1.0)

    with BulkDownloader(
        dest_root=Path(args.data_root),
        max_workers=min(max_workers, args.max_workers),
        rate_limit_rps=rate,
        skip_existing=not args.redownload,
    ) as downloader:
        report = downloader.download_batch(
            tiles=tiles,
            subdir="raw",
            validate=True,
            extract_meta=True,
            registry=registry,
        )

    # Log compliance for successful downloads
    license_info = AUTO_APPROVED_SOURCES.get(args.source, source_config)
    for result in report.results:
        if result.success:
            compliance.log_acquisition(
                source=args.source,
                tile_id=result.tile_info.tile_id,
                file_path=str(result.file_path),
                license_info=license_info,
                integrity={
                    "checksum": result.checksum,
                    "validation_passed": result.validation.passed if result.validation else None,
                },
                approval_status="auto_approved" if status == "auto_approved" else "pending",
            )

            # Register in PostGIS
            if registry and result.metadata:
                registry.register(
                    source=args.source,
                    tile_id=result.tile_info.tile_id,
                    file_path=result.file_path,
                    file_format=result.tile_info.format,
                    checksum=result.checksum,
                    license_info=license_info,
                    metadata=result.metadata,
                    bounds_wgs84=result.tile_info.bounds_wgs84,
                )
        else:
            compliance.log_rejection(
                source=args.source,
                tile_id=result.tile_info.tile_id,
                reason=result.error or "download failed",
            )

    # Print summary
    print(f"\nDownload Summary:")
    print(f"  Total:     {report.total}")
    print(f"  Succeeded: {report.succeeded}")
    print(f"  Failed:    {report.failed}")
    print(f"  Skipped:   {report.skipped}")
    print(f"  Size:      {report.total_bytes / (1024**3):.2f} GB")
    print(f"  Time:      {report.elapsed_seconds:.1f}s")


def cmd_tile(args):
    """Tile downloaded LiDAR files into 50m x 50m analysis tiles."""
    input_dir = Path(args.input)
    output_dir = Path(args.output)

    files = sorted(input_dir.glob("*.laz")) + sorted(input_dir.glob("*.las"))
    if not files:
        logger.error(f"No LAS/LAZ files found in {input_dir}")
        sys.exit(1)

    logger.info(f"Tiling {len(files)} files into {output_dir}")

    all_tiles = []
    for f in files:
        try:
            tiles = tile_lidar_file(
                input_path=f,
                output_dir=output_dir / "lidar",
                tile_size_m=args.tile_size,
                overlap_m=args.overlap,
                min_points=args.min_points,
            )
            all_tiles.extend(tiles)
            logger.info(f"  {f.name}: {len(tiles)} tiles")
        except Exception as e:
            logger.error(f"  {f.name}: FAILED - {e}")

    # Write manifest
    manifest_path = output_dir / "tile_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(all_tiles, f, indent=2)

    print(f"\nTiling Summary:")
    print(f"  Input files: {len(files)}")
    print(f"  Output tiles: {len(all_tiles)}")
    print(f"  Manifest: {manifest_path}")


def cmd_heightmaps(args):
    """Generate height maps from tiled LiDAR for JEPA training."""
    input_dir = Path(args.input)
    output_dir = Path(args.output)

    results = batch_generate_height_maps(
        tile_dir=input_dir,
        output_dir=output_dir,
        resolution_m=args.resolution,
        tile_size_m=args.tile_size,
    )

    # Write metadata
    meta_path = output_dir / "heightmap_manifest.json"
    with open(meta_path, "w") as f:
        json.dump(results, f, indent=2)

    if results:
        avg_coverage = sum(r["coverage_pct"] for r in results) / len(results)
        avg_density = sum(r["mean_density"] for r in results) / len(results)
        print(f"\nHeight Map Summary:")
        print(f"  Generated: {len(results)}")
        print(f"  Grid size: {results[0]['grid_size']}x{results[0]['grid_size']}")
        print(f"  Resolution: {args.resolution}m")
        print(f"  Avg coverage: {avg_coverage:.1f}%")
        print(f"  Avg density: {avg_density:.1f} pts/cell")


def cmd_pipeline(args):
    """Run full pipeline: list -> download -> tile -> heightmaps."""
    logger.info("Running full pipeline")

    # Step 1: Download
    args.register = True
    args.force = False
    args.redownload = False
    cmd_download(args)

    # Step 2: Tile
    raw_dir = Path(args.data_root) / "raw" / args.source
    tile_dir = Path(args.data_root) / "tiled"

    tile_args = argparse.Namespace(
        input=str(raw_dir),
        output=str(tile_dir),
        tile_size=50.0,
        overlap=5.0,
        min_points=100,
    )
    cmd_tile(tile_args)

    # Step 3: Height maps
    hm_args = argparse.Namespace(
        input=str(tile_dir / "lidar"),
        output=str(tile_dir / "height_maps"),
        resolution=0.5,
        tile_size=50.0,
    )
    cmd_heightmaps(hm_args)

    logger.info("Full pipeline complete")


def cmd_init_db(args):
    """Initialize PostGIS database schema."""
    registry = DatasetRegistry()
    registry.initialize_schema()
    print("Database schema initialized")


def cmd_stats(args):
    """Show catalog statistics."""
    registry = DatasetRegistry()
    stats = registry.get_stats()

    if not stats:
        print("Catalog is empty")
        return

    print("\nDataset Catalog Statistics:")
    print("-" * 80)
    print(f"{'Source':<25} {'Tiles':>8} {'Size (GB)':>10} {'Points (M)':>12} {'Passed':>8} {'Rejected':>8}")
    print("-" * 80)

    for row in stats:
        size_gb = (row["total_bytes"] or 0) / (1024**3)
        points_m = (row["total_points"] or 0) / 1e6
        print(
            f"{row['source']:<25} {row['tile_count']:>8} {size_gb:>10.2f} "
            f"{points_m:>12.1f} {row['passed']:>8} {row['rejected']:>8}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="GroundTruthOS Data Acquisition Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # List command
    p_list = subparsers.add_parser("list", help="List available tiles")
    p_list.add_argument("--source", required=True, help="Data source (e.g., usgs_3dep)")
    p_list.add_argument("--bbox", required=True, help="Bounding box: min_lon,min_lat,max_lon,max_lat")
    p_list.add_argument("--output", help="Save tile list to JSON file")

    # Download command
    p_dl = subparsers.add_parser("download", help="Download tiles")
    p_dl.add_argument("--source", required=True, help="Data source")
    p_dl.add_argument("--bbox", required=True, help="Bounding box")
    p_dl.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT), help="Dataset root directory")
    p_dl.add_argument("--compliance-log", default=str(DEFAULT_COMPLIANCE_LOG))
    p_dl.add_argument("--max-workers", type=int, default=4, help="Max parallel downloads")
    p_dl.add_argument("--register", action="store_true", help="Register in PostGIS catalog")
    p_dl.add_argument("--redownload", action="store_true", help="Re-download existing tiles")
    p_dl.add_argument("--force", action="store_true", help="Skip compliance review warnings")

    # Tile command
    p_tile = subparsers.add_parser("tile", help="Tile LiDAR files")
    p_tile.add_argument("--input", required=True, help="Input directory with LAS/LAZ files")
    p_tile.add_argument("--output", required=True, help="Output directory for tiles")
    p_tile.add_argument("--tile-size", type=float, default=50.0, help="Tile size in meters")
    p_tile.add_argument("--overlap", type=float, default=5.0, help="Overlap in meters")
    p_tile.add_argument("--min-points", type=int, default=100, help="Min points per tile")

    # Height maps command
    p_hm = subparsers.add_parser("heightmaps", help="Generate height maps from tiles")
    p_hm.add_argument("--input", required=True, help="Input directory with tiled LAS/LAZ")
    p_hm.add_argument("--output", required=True, help="Output directory for .npy files")
    p_hm.add_argument("--resolution", type=float, default=0.5, help="Grid resolution in meters")
    p_hm.add_argument("--tile-size", type=float, default=50.0, help="Expected tile size in meters")

    # Pipeline command
    p_pipe = subparsers.add_parser("pipeline", help="Run full pipeline")
    p_pipe.add_argument("--source", required=True, help="Data source")
    p_pipe.add_argument("--bbox", required=True, help="Bounding box")
    p_pipe.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT))
    p_pipe.add_argument("--compliance-log", default=str(DEFAULT_COMPLIANCE_LOG))
    p_pipe.add_argument("--max-workers", type=int, default=4)

    # Init DB
    subparsers.add_parser("init-db", help="Initialize PostGIS schema")

    # Stats
    subparsers.add_parser("stats", help="Show catalog statistics")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    commands = {
        "list": cmd_list,
        "download": cmd_download,
        "tile": cmd_tile,
        "heightmaps": cmd_heightmaps,
        "pipeline": cmd_pipeline,
        "init-db": cmd_init_db,
        "stats": cmd_stats,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
