# Primary References

- ALTA/NSPS Minimum Standard Detail Requirements (effective 2026-02-23): official ALTA/NSPS publication.
- NOAA NGS GGXF pages:
  - https://geodesy.noaa.gov
- OGC GGXF Standard:
  - https://www.ogc.org
- Autodesk AutoCAD .NET API docs (command context and document locking):
  - https://help.autodesk.com

## Open point-cloud to parametric prototype stack

- **PDAL:** primary open pipeline for LAS/LAZ/COPC ingestion, reprojection, decimation, and tiling. Favor COPC for large cloud-scale workloads because it supports chunked access and reproducible batch processing.
- **E57 / COPC ingestion note:** user briefings point to direct E57 and COPC flows as a practical Python-first path for point-cloud preprocessing before downstream geometry extraction.
- **Open3D:** useful for local downsampling, clustering, and point-cloud cleanup once PDAL has normalized the source data.
- **pc-skeletor:** candidate branch/centerline extraction tool for tree-like or network-like point clouds when graph skeletons are needed rather than simple axial fits.
- **CadQuery / build123d:** suitable CAD-side target libraries for reconstructing spline/centerline-driven solids once point-cloud paths have been extracted.

## Practical pipeline note

- For messy single-axis objects such as pipes or cables, a pragmatic path is: PDAL normalize/voxelize -> Open3D cleanup -> axis/path extraction -> spline fit -> CadQuery/build123d sweep/export.
- For branching structures, keep graph-skeleton extraction separate from final CAD solid generation so raw scans remain preserved and non-destructive.

## License watchlist

- **PDAL:** permissive/BSD-style and suitable for deterministic preprocessing workflows.
- **CadQuery / build123d:** Apache-2.0-friendly prototype lane for parametric reconstruction.
- **pc-skeletor:** user briefing describes it as MIT; verify upstream before redistribution.
- **untwine:** useful for COPC generation, but GPLv3 means redistribution or bundling decisions should be reviewed before it enters a default commercial toolchain.
