# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

TOTaLi is a **defensible spatial drafting pipeline** for civil engineering and land surveying. It ingests LiDAR point clouds and produces Civil 3D-compatible CAD deliverables through five deterministic phases, with a strict human-certification gate: AI classifies (probabilistic, non-authoritative), algorithms measure (deterministic computational geometry), humans certify (PLS remains sovereign).

Core doctrine: no geometry is ever auto-promoted to certified status. Every AI suggestion lands on a DRAFT layer and must be accepted/rejected by a licensed surveyor.

## Canonical root

The canonical pipeline code lives in `TOTaLi/totali/` (geodetic, segmentation, extraction, cad_shielding, linting, audit, pipeline phases).

Non-canonical copies that must be resolved:

- `{pipeline,geodetic,...}/totali/` — a literally brace-named directory containing a different module set (orchestration, RAG, agents, training). This is consolidation debt; do not develop here. Its content is mirrored in `workspace-scaffold/apps/totali/`.
- `workspace-scaffold/apps/totali/` — the monorepo destination for orchestration/RAG/agent modules per `workspace-scaffold/docs/migration_manifest.yaml`. Use this path for orchestration work until the brace-dir is deleted.

## Directory structure

```
totali/               # Main Python package
  geodetic/           # Phase 1: CRS/epoch/unit validation, PROJ transformations
  segmentation/       # Phase 2: LiDAR ML classifier (PointTransformer v2, ONNX)
  extraction/         # Phase 3: DTM/TIN, breaklines, contours, planimetric vectors
  cad_shielding/      # Phase 4: CAD middleware isolation, geometry healing/quarantine
  linting/            # Phase 5: Ghost suggestions in CAD, surveyor accept/reject UI
  audit/              # Chain-of-custody JSONL event logging
  agents/             # Agentic pipeline components
  models/             # ONNX model loading
  pipeline/           # Phase orchestration
  quarantine_ui/      # Flask UI for CRS ambiguity resolution (port 5050)
  repl/               # Interactive REPL
config/
  pipeline.yaml       # Master pipeline configuration (CRS, model paths, tolerances)
tests/                # pytest suite
artifacts/            # Pipeline output artifacts (do not commit large files)
audit_logs/           # JSONL chain-of-custody logs (append-only)
Datasets/             # Geospatial/LiDAR input data (do not commit)
dwg-tool-parser/      # DWG/DXF parsing stub (stub; linked from workspace-scaffold)
survey-automation-roadmap/  # Roadmap docs and planning artifacts
Docs/                 # Research PDFs and whitepapers
Experiments/          # Exploratory notebooks and scripts
tools/                # Standalone CLI tools
skills/               # Reusable pipeline skills
AUTOMATICCAD/         # Civil 3D automation scripts
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

- **Phase 1 — Geodetic Gatekeeper:** Validates CRS (Colorado State Plane zones, NAD83/NAD83(2011), EPSG:2231-2233/6428-6432), epoch (2010.0), and units (US survey foot). Rejects mixed datums. CRS inference UI runs on port 5050 when ambiguity is detected.
- **Phase 2 — ML Segmentation:** PointTransformer v2 ONNX model, CPU/CUDA/TensorRT. Classifies ground, vegetation, building, curb, wire, road, bridge, etc. (ASPRS + extended classes). Output is probabilistic — never treated as authoritative.
- **Phase 3 — Deterministic Extraction:** TIN/DTM, breaklines, contours (1 ft minor / 5 ft index), planimetric vectors — all computed geometrically, not inferred.
- **Phase 4 — CAD Shielding:** Geometry healing (close tolerance 0.001, weld vertices, remove duplicates, fix self-intersections). All output lands on `TOTaLi-*-DRAFT` layers. Certified layer suffix is empty (manual promotion only).
- **Phase 5 — Surveyor Linting:** Ghost suggestions at 40% opacity, color-coded by confidence (green/amber/red/magenta for occluded). `auto_promote: false` — hardcoded, never change.
- **Audit:** Every pipeline event (ingest, transform, classify, extract, heal, insert, accept, reject, promote, certify) is SHA-256-hashed and written to `audit_logs/` as JSONL. Logs are append-only chain-of-custody records.

## Pipeline layer naming convention

All DRAFT layers follow `TOTaLi-<DISCIPLINE>-<FEATURE>-DRAFT`. Example:
- `TOTaLi-SURV-DTM-DRAFT` — ground surface
- `TOTaLi-SURV-BRKLN-DRAFT` — breaklines
- `TOTaLi-PLAN-BLDG-DRAFT` — buildings
- `TOTaLi-QA-OCCLUSION` — occlusion zones (no DRAFT suffix; QA layer)

## Data governance

- **Datasets/**: LiDAR `.las`/`.laz`, DEM, DXF/DWG source files. Do not commit to git. Keep locally or on shared network storage.
- **artifacts/**: Pipeline output (DXF, GeoJSON, CSV). Do not commit large binary outputs.
- **audit_logs/**: JSONL chain-of-custody logs. Append-only — never edit or delete entries. These are defensible records.
- **models/**: ONNX model weights. Do not commit. Reference via `config/pipeline.yaml`.
- Never hardcode absolute paths in source — use `config/pipeline.yaml` for all data roots.
- Never commit `.env`, API keys, or credential files.

## Guardrails

- `auto_promote: false` in `config/pipeline.yaml` — never set to true under any circumstances.
- `require_pls_signature: true` — certified deliverables require a licensed surveyor signature.
- All AI/ML output is non-authoritative. It must be reviewed and accepted by a human before promotion.
- Do not modify `audit_logs/` entries retroactively.
- DWG/DXF writes always go through the CAD shielding middleware — never write directly to certified layers.
- CRS changes require re-running the geodetic phase from scratch.

## Agentic completion plan (mandatory for autonomous / agentic work)

The project is instrumented for fully agentic completion. Read before starting work:

- `AGENTIC_COMPLETION_PLAN.md` — top-level wire (dependency order, global rules,
  global gates, agentic outer-loop workflow, pre-merge checklist, completion ledger,
  project-level Definition of Done, escalation protocol).
- `<module>/AGENTIC.md` — per-module plan, rules, gates, tests, DoD. One per module:
  - Core: `totali/pipeline/`, `totali/geodetic/`, `totali/segmentation/`,
    `totali/extraction/`, `totali/cad_shielding/`, `totali/linting/`, `totali/audit/`,
    `totali/agents/`, `totali/models/`, `totali/quarantine_ui/`, `totali/repl/`
  - Tooling: `tests/`, `tools/`, `skills/`
  - Siblings: `survey-automation-roadmap/`, `AUTOMATICCAD/`, `laser-suite/`,
    `dwg-tool-parser/`, `totali-baton/`, `groundtruthos-data/`, `data-reroute/`

Start every work session with: AGENTIC_COMPLETION_PLAN.md → the target module's AGENTIC.md
→ its Plan step → its tests → its gates. Never skip the order.

## C++ rules (mandatory for any C/C++ edit)

Any C/C++ change in `dwg-tool-parser/`, the auracad bridge, FFI surfaces, or vendored
native deps (PROJ/GDAL/PDAL/OpenCASCADE/LibreDWG) must follow `Docs/CXX_AGENTIC_RULES.md`.
That document is the source of truth for:

- Dangers (silent correctness, FFI, agentic-loop, security classes)
- Hard rules (sanitizer coverage, FFI discipline, no `-ffast-math`, deletion review, destructive-op policy)
- Per-edit / refactor / FFI workflows
- Sanitizer matrix (ASAN / UBSAN / TSAN / MSAN / LSAN / CFI) and debug strategies
- Review practices and audit-integrity rules for C++-emitted events
- Pre-merge checklist

Read it before editing. Do not weaken a rule without amending that doc first.

## Shared agentic infrastructure

Shared agents, hooks, and skills live in `workspace-scaffold/`:

- `workspace-scaffold/agents/` — innovator, strategic-implementer, security-reviewer
- `workspace-scaffold/hooks/` — credential guard, scope enforcement, lint-on-write
- `workspace-scaffold/skills/` — reusable pipeline skills

Domain-specific agent: `.claude/agents/civil-cad-agent.md`
