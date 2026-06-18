# Models — Agentic Completion Plan

Scope: `totali/models/` — ONNX loading helpers (`loader.py`).

## Purpose
Model-related utilities: ONNX session construction and model manifest / version
metadata. (The former `projection.py` multimodal early-fusion projector was
removed with the in-process LLM codegen path — see the reconciliation note below.)

## Plan
1. **M-1 ONNX loader.** Single helper `load_onnx(path, device) -> Session` that:
   validates file existence, checks SHA-256 against an optional manifest, probes device
   availability, returns a configured `onnxruntime.InferenceSession` with deterministic settings.
2. **M-2 Model manifest.** `models/MANIFEST.json` records expected filename, SHA-256,
   input/output signatures, and trained-on dataset. Loader refuses mismatches.
3. **M-4 Determinism.** Sessions created with `enable_mem_pattern=False` when the caller
   requests deterministic mode.
4. **M-5 Exit cleanliness.** Sessions close on context exit; no process-lifetime leaks.

> **Removed (M-3):** `projection.py` / `TotaliMultimodalProjector` and the
> `coder_agent` LLM driver were dropped when the pipeline moved away from
> in-process LLM codegen. Do not reintroduce without a fresh decision.

## Rules
- Model weights are **not** committed to git. Paths come from config.
- Loader never downloads. Operator provisions files out-of-band; loader validates.
- No GPU context is created eagerly at import time.

## Gates
1. `pytest tests/test_models_*.py -v` green.
2. A missing model file raises `ModelNotFoundError` with actionable message.
3. A SHA-256 mismatch raises `ModelHashMismatch`.

## Tests required
- `tests/test_models_loader.py` — happy path + two failure modes.

## Dependencies
- **Upstream:** stdlib (`hashlib`), Pydantic (for manifest), `onnxruntime`.
- **Downstream:** `totali/segmentation/` (primary consumer).

## Open questions / known debts
- Model signature introspection via `onnxruntime` vs trusting `MANIFEST.json`: use both —
  manifest as contract, runtime introspection as verification.

## Definition of Done
- M-1, M-2, M-4, M-5 implemented and tested.
- `MANIFEST.json` committed with at least the production PointTransformer v2 entry.
- Loader ≥ 95 % covered.

## Progress (append-only)
- 2026-04-22 — M-1 ONNX loader: `totali/models/loader.py` created; SHA-256 + manifest validation; missing-file raises; `tests/test_models_loader.py` 9 passed. (ledger: M-1 @ 2026-04-22T05:20:00Z)
- 2026-04-22 — M-3-partial: `tests/test_models_projection.py` authored (torch-skipped in minimal venv; runs when torch installed). (ledger: M-3-partial @ 2026-04-22T05:21:00Z)
