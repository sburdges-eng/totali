# CAD Shielding — Agentic Completion Plan

Scope: `totali/cad_shielding/` — `shield.py`, `geometry_healer.py`.

## Purpose
Phase 4. The only component authorized to write CAD files (DXF/DWG/DGN). Takes extraction
artifacts, heals geometry (close gaps, snap vertices, weld, repair self-intersections),
places output on `TOTaLi-*-DRAFT` layers, and mediates every CAD mutation through the
middleware so certified layers are never touched.

## Inputs / Outputs
- **Input:** extraction artifacts (GeoJSON/TIN/TIFF) + config section `cad_shielding:`.
- **Output:** `artifacts/<run>/cad/draft.dxf` (or `.dwg`/`.dgn`). Audit events
  `heal_start/done`, `cad_write_start/done`, `layer_created`, `geometry_quarantined`.

## Plan
1. **C-1 Healer.** `geometry_healer.GeometryHealer` performs close (`close_tolerance: 0.001`),
   degenerate-face drop (`degenerate_face_threshold: 0.0001`), snap (`snap_tolerance: 0.0001`),
   self-intersection check + repair, vertex weld, duplicate removal, polygon close.
   Each sub-step reports before/after counts to the audit payload.
2. **C-2 Quarantine.** Geometry that cannot be healed is routed to a quarantine side-file
   and emits `geometry_quarantined`. It never reaches the DXF write.
3. **C-3 Layer mapping.** Read `cad_shielding.layer_mapping` from config. Every layer name
   must match `TOTaLi-<DISCIPLINE>-<FEATURE>-DRAFT` (QA layers exempt: `TOTaLi-QA-*`).
   Reject at load time if config introduces a non-conforming name.
4. **C-4 Format switch.** `format: dxf | dwg | dgn`. DXF is the reference implementation
   (via `ezdxf`). DWG writes route through `dwg-tool-parser/` (stub today). DGN deferred.
5. **C-5 Middleware timeout / retry.** `middleware_timeout_sec: 30`, `max_retry: 3`.
   Retries are idempotent — the CAD file is written to a tempfile and atomically renamed
   on success; partial writes never land.
6. **C-6 Certified suffix discipline.** `certified_layer_suffix: ""`. The code path that
   would strip `-DRAFT` only runs inside the promotion step (NOT inside this phase).
   This phase always writes `-DRAFT`.
7. **C-7 Determinism.** DXF entity ordering is stable: group by layer, within layer sort by
   (feature type, id). Emitted file is byte-identical across runs for identical input.
8. **C-8 Round-trip test.** After write, re-read the file with `ezdxf` and assert the
   entity count / layer set matches what was written.

## Rules
- **No direct write to a non-DRAFT layer from this phase.** Ever. Certified writes require
  a separate, audited promotion step that is out of scope here.
- **No bypassing the healer.** Extraction artifacts enter `shield.run()` and either pass through
  healing or quarantine. No path writes raw geometry to CAD.
- Layer name conformity is enforced at config load, not at write time.
- `ezdxf` is the canonical DXF library; do not introduce a second DXF writer without review.
- No file open in `w+` mode that truncates on crash; always tempfile + atomic rename.

## Gates
1. `pytest tests/test_shield.py tests/test_geometry_healer.py -v` green.
2. DXF produced on golden input passes ezdxf round-trip.
3. File is byte-identical across two runs.
4. A synthetic non-conformant layer name in config fails the phase at load (not at write).
5. A degenerate input polygon hits quarantine, never ends up in the DXF.

## Tests required
Existing:
- `tests/test_shield.py`
- `tests/test_geometry_healer.py`
- `tests/test_dwg_parser.py` (DWG bridge)

Missing / to add:
- `tests/test_shield_determinism.py` — byte-identical DXF across runs.
- `tests/test_shield_layer_name_guard.py` — bad config layer name is rejected.
- `tests/test_shield_atomic_write.py` — simulated failure mid-write leaves no partial file.
- `tests/test_healer_quarantine_path.py` — unhealable input is quarantined, audited, not written.

## Dependencies
- **Upstream:** `totali/extraction/`.
- **Downstream:** `totali/linting/` (reads the draft DXF for ghost-suggestion overlay).
- **External:** `ezdxf`. DWG pathway depends on `dwg-tool-parser/` (see its AGENTIC.md).

## Open questions / known debts
- DWG write path is stub. Until `dwg-tool-parser/` ships, DWG requests should fail loudly.
- DGN write is deferred — keep the config key but raise `NotImplementedError`.
- Libre/OpenDWG vs RealDWG licensing review pending before DWG path is turned on.

## Definition of Done
- C-1..C-8 implemented with tests.
- DXF write determinism + round-trip tests green.
- No code path writes a `-DRAFT`-less layer name from this phase.
- Quarantine path exercised by test and produces a readable quarantine sidecar.

## Progress (append-only)
- _(empty)_
