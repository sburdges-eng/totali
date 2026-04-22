# Models — Agentic Completion Plan

Scope: `totali/models/` — `projection.py` plus ONNX loading helpers.

## Purpose
Model-related utilities: ONNX session construction, projection math helpers that do not
belong in the geodetic phase (e.g., small transforms used by multiple phases), and model
manifest / version metadata.

## Plan
1. **M-1 ONNX loader.** Single helper `load_onnx(path, device) -> Session` that:
   validates file existence, checks SHA-256 against an optional manifest, probes device
   availability, returns a configured `onnxruntime.InferenceSession` with deterministic settings.
2. **M-2 Model manifest.** `models/MANIFEST.json` records expected filename, SHA-256,
   input/output signatures, and trained-on dataset. Loader refuses mismatches.
3. **M-3 Projection helpers.** `projection.py` hosts pure, tested math (e.g., 3D→2D plane
   projection used by extraction). No I/O. No ONNX here.
4. **M-4 Determinism.** Sessions created with `enable_mem_pattern=False` when the caller
   requests deterministic mode.
5. **M-5 Exit cleanliness.** Sessions close on context exit; no process-lifetime leaks.

## Rules
- Model weights are **not** committed to git. Paths come from config.
- Loader never downloads. Operator provisions files out-of-band; loader validates.
- Projection helpers stay pure; no side effects, no audit emits (phases emit).
- No GPU context is created eagerly at import time.

## Gates
1. `pytest tests/test_models_*.py -v` green.
2. A missing model file raises `ModelNotFoundError` with actionable message.
3. A SHA-256 mismatch raises `ModelHashMismatch`.
4. `projection.py` covered ≥ 95 % by unit tests.

## Tests required
Missing / to add:
- `tests/test_models_loader.py` — happy path + two failure modes.
- `tests/test_models_projection.py` — pure-math coverage for every helper.

## Dependencies
- **Upstream:** stdlib (`hashlib`), Pydantic (for manifest), `onnxruntime`.
- **Downstream:** `totali/segmentation/` (primary consumer).

## Open questions / known debts
- Model signature introspection via `onnxruntime` vs trusting `MANIFEST.json`: use both —
  manifest as contract, runtime introspection as verification.

## Definition of Done
- M-1..M-5 implemented and tested.
- `MANIFEST.json` committed with at least the production PointTransformer v2 entry.
- Loader + projection helpers ≥ 95 % covered.

## Progress (append-only)
- _(empty)_
