# GroundTruthOS Data — Agentic Completion Plan

Scope: `groundtruthos-data/` — geospatial data pipeline: download, tile, decimate (PDAL),
feature store (PostGIS / COPC), knowledge graph. Python ≥ 3.11.

## Purpose
The data-ingestion + tiling + feature-store layer that produces reproducible ground-truth
datasets for TOTaLi's ML segmentation phase and validation harnesses. Runs at bulk scale
(PDAL + DuckDB + PostGIS).

## Surface
Modules under `pipeline/`:
- `tiling.py` — spatial tiling of point clouds
- `decimation.py` — point decimation
- `features.py` — feature extraction
- `telemetry.py` — run telemetry / metrics
- `run_pdal_batch.py` — PDAL batch driver
- `postgis_copc_postpatch_sql.sql` — post-load PostGIS patches for COPC-sourced tiles

Plus: `downloader/`, `compliance/`, `config/`, `schema/`, `scripts/`, `storage/`, `survey-automation/`.

## Plan
1. **GT-1 Config-driven.** All knobs (tile size, decimation factor, PostGIS endpoints,
   storage paths) live in `config/`. No hardcoded paths.
2. **GT-2 PDAL determinism.** PDAL pipelines use seeded samplers and documented filter
   chains. Two runs on the same input + config produce identical COPC / parquet outputs.
3. **GT-3 Compliance.** `compliance/` enforces any licensing / provenance requirements of
   source datasets. Every downloaded dataset has a provenance record (URL, license, retrieved_at).
4. **GT-4 Storage abstraction.** `storage/` has a single interface that abstracts local disk
   / external SSD / object storage. No direct fs calls outside this module.
5. **GT-5 Schema.** `schema/` holds JSON schemas for every emitted artifact — tile index,
   feature manifest, run manifest. Versioned.
6. **GT-6 Telemetry.** Every run emits a telemetry JSON with stage durations, row counts,
   error counts. Feeds downstream dashboards and regression alarms.
7. **GT-7 Survey-automation tie-in.** `survey-automation/` subdirectory coordinates with
   the top-level `survey-automation-roadmap/` CLI — share schema where possible to avoid drift.
8. **GT-8 Database migrations.** `postgis_copc_postpatch_sql.sql` is applied via a migration
   runner; no ad-hoc `psql` edits to production DBs.

## Rules
- No dataset enters `storage/` without a provenance record.
- No PostGIS schema change without a forward-and-backward migration script.
- PDAL pipelines are committed as JSON (readable), not constructed in-memory.
- COPC files carry their source hash in the metadata; tampering detectable by spot-check.
- Raw datasets are not committed (per TOTaLi `Datasets/` governance).

## Gates
1. `pytest -q` green on this module's test tree.
2. Tiling determinism test: two runs on a reference LAS yield identical COPC bytes.
3. Feature-extraction test: expected feature count on a reference tile within ±0.
4. Provenance test: every downloaded dataset has a valid provenance record before it's tiled.
5. Migration round-trip: apply + rollback + re-apply on a disposable PostGIS instance.

## Tests required
Missing / to add (if not already present):
- `tests/test_tiling_determinism.py`
- `tests/test_features_reference.py`
- `tests/test_provenance.py`
- `tests/test_storage_abstraction.py`

## Dependencies
- **Upstream:** source datasets (USGS, state GIS, etc.) per `compliance/` records.
- **Downstream:** TOTaLi segmentation training + validation, `laser-suite` reference bundles.
- **External:** PDAL (binary), DuckDB, PostGIS, `laspy[lazrs]`, `rasterio`, `pyproj`,
  `pyarrow`, `requests`, `tqdm`.

## Open questions / known debts
- COPC vs LAZ default output — pick one and stick; current code supports both but a default
  should be documented.
- External SSD vs local storage decision for large runs — see `data-reroute/` for a
  related inventory.

## Definition of Done
- GT-1..GT-8 implemented with tests.
- Determinism test green on reference input.
- Provenance test green on every committed reference dataset.
- Migration script has working rollback on a disposable instance.
- Telemetry JSON schema committed and documented.

## Progress (append-only)
- _(empty)_
