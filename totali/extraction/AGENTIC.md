# Deterministic Extraction — Agentic Completion Plan

Scope: `totali/extraction/` — `extractor.py`.

## Purpose
Phase 3. Produces deterministic, reproducible computational geometry from the classified
point cloud: DTM/TIN surfaces, breaklines, contour lines (minor and index), planimetric
vectors (buildings, curbs, hardscape, wires). This is the **measurement** half of the
pipeline — no probabilities, no inference, no FP nondeterminism.

## Inputs / Outputs
- **Input:** segmentation artifacts + validated point cloud; config section `extraction:`.
- **Output:**
  - `artifacts/<run>/extract/dtm.tif` (or TIN)
  - `artifacts/<run>/extract/breaklines.geojson`
  - `artifacts/<run>/extract/contours_minor.geojson`, `contours_index.geojson`
  - `artifacts/<run>/extract/buildings.geojson`, `curbs.geojson`, `hardscape.geojson`, `wires.geojson`
  - Audit events `extract_dtm_start/done`, `extract_breaklines_start/done`, `extract_contours_start/done`,
    `extract_planimetric_start/done`.

## Plan
1. **E-1 TIN/DTM.** Generate TIN from ground-classified points. Enforce
   `max_triangle_edge_length: 50.0 ft`, `max_triangle_angle: 85.0°`, `thin_factor: 0.1`.
   Reject triangles outside bounds; emit `dtm_triangles_rejected` counts.
2. **E-2 Breaklines.** Extract along classified curb/wall/edge features. Constraints
   `min_angle_degrees: 15.0`, `min_length_ft: 5.0`, `smoothing_iterations: 2`.
3. **E-3 Contours.** Minor interval `1.0 ft`, index interval `5.0 ft`, smoothing tolerance
   `0.5`. Contours are generated from the TIN, not independently.
4. **E-4 Planimetrics.** Buildings ≥ `100 sqft`, `simplify_tolerance: 0.25 ft`. Curbs,
   hardscape, wires each produced with a documented rule from the classified mask.
5. **E-5 Determinism.** All steps use fixed-seed algorithms. Any Delaunay / simplification
   library used must have a documented deterministic mode; otherwise wrap to enforce.
6. **E-6 Unit discipline.** Everything in US survey feet. All tolerances from config.
   No hardcoded float literals in `extractor.py`.
7. **E-7 Audit payloads.** Each sub-step emits counts (triangles, breaklines, contour segments,
   polygons) so surveyors have a triage handle.
8. **E-8 Error surfaces.** Degenerate geometry (zero-area polygons, collinear triangles) is
   logged and excluded, never silently repaired here (repair belongs in `cad_shielding/`).

## Rules
- No probabilistic step in this phase. If something feels like a judgment call, it belongs
  in `segmentation/` or `linting/`, not here.
- No `-ffast-math`, no FMA reordering, no threaded reduce with nondeterministic ordering.
- No direct CAD writes. The phase emits GeoJSON/TIFF/TIN artifacts; `cad_shielding/` owns CAD.
- All thresholds from `config.extraction.*`. Reviewers reject any new hardcoded literal.

## Gates
1. `pytest tests/test_extractor.py -v` green.
2. Running extraction twice on the golden segmentation output produces byte-identical
   GeoJSON files (timestamp fields excluded or normalized).
3. Contour totals (minor and index) match expected counts on the golden dataset within ±0.
4. No audit event carries `authoritative: false` from this phase (extraction output IS authoritative
   in the measurement sense — but it has not yet been certified).

## Tests required
Existing:
- `tests/test_extractor.py`

Missing / to add:
- `tests/test_extractor_determinism.py` — double-run byte parity.
- `tests/test_extractor_golden.py` — fixed expected counts on BV_BASE-derived fixture.
- `tests/test_extractor_threshold_propagation.py` — config changes measurably alter output counts.

## Dependencies
- **Upstream:** `totali/segmentation/`, `totali/geodetic/`.
- **Downstream:** `totali/cad_shielding/` (primary consumer), `totali/linting/`.
- **External:** `numpy`, `scipy.spatial` (Delaunay), `shapely`, `rasterio` (TIFF write).

## Open questions / known debts
- `scipy.spatial.Delaunay` is deterministic only under specific input orderings; confirm
  and wrap if needed. Alternative: `triangle` library — heavier install.
- Contour smoothing currently at tolerance `0.5` is a working default, not calibrated.
  Calibration pass should follow survey-feedback after first production use.

## Definition of Done
- E-1..E-8 implemented and tested.
- Determinism test green on two consecutive CI runs.
- Threshold-propagation test demonstrates all `config.extraction.*` values are wired.
- Artifacts consumed cleanly by `cad_shielding/` shield.run() on golden input.

## Progress (append-only)
- _(empty)_
