# Geodetic Gatekeeper — Agentic Completion Plan

Scope: `totali/geodetic/` — `gatekeeper.py`, `crs_inference.py`.

## Purpose
Phase 1. Validates CRS / epoch / units before any geometry leaves the ingestion stage.
Rejects mixed datums. Infers CRS when metadata is missing/ambiguous, routing low-confidence
cases to the quarantine UI on port 5050 for operator decision.

## Inputs / Outputs
- **Input:** raw `.las`/`.laz`/point file; optional sidecar metadata; config section
  `geodetic:` in `config/pipeline.yaml`.
- **Output:** validated/normalized CRS assignment written to `PipelineContext.artifacts`;
  audit events `crs_validated`, `crs_inferred`, `crs_rejected`, `datum_mismatch`.

## Plan
1. **G-1 Config allowlist.** Accepted CRS are `EPSG:2231/2232/2233` (NAD83 Colorado N/C/S)
   and `EPSG:6428/6430/6432` (NAD83(2011) equivalents). All in US survey foot. Reject any
   CRS outside this list unless `reject_on_missing_crs: false` permits inference.
2. **G-2 Epoch enforcement.** Required epoch `2010.0`. Any file carrying a different epoch
   is quarantined unless config explicitly overrides.
3. **G-3 Unit enforcement.** US survey foot. Tolerance `0.01 ft`. A file in metres is
   rejected (never silently reprojected); operator must rerun ingestion after conversion.
4. **G-4 Mixed datum rejection.** `reject_on_mixed_datum: true` is hardcoded default.
   When multiple input files declare different datums, halt the phase and emit
   `datum_mismatch` with file list.
5. **G-5 CRS inference.** `crs_inference.py` — confidence-scored inference from coordinate
   ranges / known transform fingerprints. `confidence_threshold: 0.8`, `auto_assign_high_confidence: true`
   means ≥0.8 is auto-assigned with an audit entry; <0.8 triggers the quarantine UI.
6. **G-6 Quarantine UI bridge.** When inference is ambiguous, start/dial the Flask app on
   `quarantine_ui_port: 5050`, block the pipeline, and resume only after operator selects.
   Emit `crs_quarantined` then `crs_resolved` with operator identity.
7. **G-7 GEOID model.** Use `GEOID18` for orthometric heights. Reject any requested geoid
   not on the allowlist; do not silently substitute.
8. **G-8 PROJ transforms.** All transforms go through `pyproj`. Log the exact
   `pyproj.__version__` and PROJ release in each audit event for reproducibility.
9. **G-9 Deterministic output.** For a given input + config, the phase produces byte-identical
   CRS-assignment artifacts. Test with `tests/test_crs_inference.py` and a pinned dataset.

## Rules
- Never silently reproject between CRSes. Reprojection is a deliberate, audited operation.
- Never substitute a different geoid model to "make it work."
- Metric mode is off by default (`internal_metric_standardization: false`). If enabled
  in a future PR, keep US survey foot as the emitted unit and document the conversion path.
- CRS inference confidence never bypasses the `auto_assign_high_confidence` flag.
- The quarantine UI is the only path for <0.8 confidence cases. No CLI flag may force-assign.
- Audit events are mandatory for every validation decision, pass or fail.

## Gates
1. `pytest tests/test_geodetic.py tests/test_crs_inference.py -v` green.
2. Running the phase on a file tagged `EPSG:2231` + epoch `2010.0` + US survey foot
   produces `crs_validated` audit event and populates `context.artifacts["crs"]`.
3. Running on a mixed-datum input produces `datum_mismatch` + phase fail, no downstream phase runs.
4. Running on a sub-threshold CRS inference case triggers the quarantine UI bind on :5050.
5. Byte-reproducible output on the golden input across two runs.

## Tests required
Existing:
- `tests/test_geodetic.py`
- `tests/test_crs_inference.py`

Missing / to add:
- `tests/test_geodetic_mixed_datum.py` — multi-file mixed-datum rejection.
- `tests/test_geodetic_quarantine_trigger.py` — low-confidence → UI port open path.
- `tests/test_geodetic_deterministic.py` — double-run byte reproducibility.

## Dependencies
- **Upstream:** `totali/audit/`, `totali/pipeline/base_phase.py`.
- **Downstream:** every phase reads the CRS assignment from context; `totali/quarantine_ui/`
  handles the blocking UI step.
- **External:** `pyproj`, GEOID18 geoid grid (must be installed / referenced in config).

## Open questions / known debts
- Epoch tolerance: allow `2010.0 ± δ` or exact match only? Default currently exact.
  Decide and test before Phase 2 ships to production.
- No test yet verifying `pyproj` / PROJ versions are captured in audit payload.

## Definition of Done
- All G-1..G-9 plan items implemented with tests.
- Quarantine UI integration exercised by an end-to-end test (may use Werkzeug test client).
- Golden-input determinism test passes on two CI runs.
- No CRS ever reaches Phase 2 without an audit trail documenting how it was established.

## Progress (append-only)
- 2026-06-17 — G-5/G-6: `crs_confidence_threshold` + `auto_assign_high_confidence` wired in gatekeeper; sub-threshold INFERRED routes to quarantine UI (:5050); `tests/test_geodetic_quarantine_trigger.py` 5 passed.
