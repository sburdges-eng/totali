# GEMINI.md

This file provides guidance to Gemini CLI when working with code in this repository.

## Project overview

TOTaLi is a **defensible spatial drafting pipeline** for civil engineering and land surveying. It ingests LiDAR point clouds and produces Civil 3D-compatible CAD deliverables through five deterministic phases, with a strict human-certification gate: AI classifies (probabilistic, non-authoritative), algorithms measure (deterministic computational geometry), humans certify (PLS remains sovereign).

Core doctrine: no geometry is ever auto-promoted to certified status. Every AI suggestion lands on a DRAFT layer and must be accepted/rejected by a licensed surveyor.

## Directory structure

```
totali/               # Main Python package
  geodetic/           # Phase 1: CRS/epoch/unit validation, PROJ transformations
  segmentation/       # Phase 2: LiDAR ML classifier (PointTransformer v2, ONNX)
  extraction/         # Phase 3: DTM/TIN, breaklines, contours, planimetric vectors
  cad_shielding/      # Phase 4: CAD middleware isolation, geometry healing/quarantine
  linting/            # Phase 5: Ghost suggestions in CAD, surveyor accept/reject UI
  audit/              # Chain-of-custody JSONL event logging
config/
  pipeline.yaml       # Master pipeline configuration (CRS, model paths, tolerances)
tests/                # pytest suite
artifacts/            # Pipeline output artifacts (do not commit large files)
audit_logs/           # JSONL chain-of-custody logs (append-only)
Datasets/             # Geospatial/LiDAR input data (do not commit)
dwg-tool-parser/      # DWG/DXF parsing stub
survey-automation-roadmap/  # Roadmap docs and planning artifacts
```

## Common commands

### Setup
```bash
pip install -r requirements.txt
pip install -e .
```

### Run pipeline
```bash
# Full pipeline
python -m totali.main --input path/to/pointcloud.las --config config/pipeline.yaml

# Individual phases
python -m totali.main --input data.las --phase geodetic
python -m totali.main --input data.las --phase segment
python -m totali.main --input data.las --phase extract
```

### Testing
```bash
pytest                          # All tests (testpaths = tests, -v --tb=short)
pytest tests/test_geodetic.py   # Single file
pytest -k "test_crs"            # By name
```

## Architecture notes

- **Phase 1 — Geodetic Gatekeeper:** Validates CRS (Colorado State Plane, NAD83/NAD83(2011), EPSG:2231-2233/6428-6432), epoch (2010.0), US survey foot units. Rejects mixed datums.
- **Phase 2 — ML Segmentation:** PointTransformer v2 ONNX model. Output is probabilistic — never treated as authoritative.
- **Phase 3 — Deterministic Extraction:** TIN/DTM, breaklines, contours, planimetric vectors computed geometrically.
- **Phase 4 — CAD Shielding:** Geometry healing and middleware isolation. All output lands on `TOTaLi-*-DRAFT` layers only.
- **Phase 5 — Surveyor Linting:** Ghost suggestions with color-coded confidence. `auto_promote: false` — hardcoded, never change.
- **Audit:** Every pipeline event is SHA-256-hashed and written to `audit_logs/` as append-only JSONL.

## Data governance

- **Datasets/**: LiDAR `.las`/`.laz`, DEM, DXF/DWG source files. Do not commit to git.
- **artifacts/**: Pipeline output. Do not commit large binary outputs.
- **audit_logs/**: Append-only chain-of-custody logs. Never edit or delete entries.
- **models/**: ONNX model weights. Do not commit.
- Never hardcode absolute paths — use `config/pipeline.yaml` for all data roots.
- Never commit `.env`, API keys, or credential files.

## Guardrails

- `auto_promote: false` in `config/pipeline.yaml` — never set to true under any circumstances.
- `require_pls_signature: true` — certified deliverables require a licensed surveyor signature.
- All AI/ML output is non-authoritative — must be reviewed and accepted by a human before promotion.
- Do not modify `audit_logs/` entries retroactively.
- DWG/DXF writes always go through the CAD shielding middleware — never write directly to certified layers.
