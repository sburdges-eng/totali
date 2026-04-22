# ML Segmentation — Agentic Completion Plan

Scope: `totali/segmentation/` — `classifier.py`.

## Purpose
Phase 2. Runs PointTransformer v2 (ONNX) over the validated point cloud and assigns per-point
probabilistic class labels (ASPRS + extended: ground, curb, wire, building, etc.). Output is
**never authoritative** — it feeds downstream deterministic extraction and surveyor linting
where humans accept or reject.

## Inputs / Outputs
- **Input:** validated point cloud (post-geodetic), config section `segmentation:`.
- **Output:** per-point class labels, confidence scores, occlusion flags, voxelized tiles.
  Artifacts under `context.artifacts["segmentation"]`. Audit events
  `classify_start`, `classify_batch`, `classify_done`, `classify_low_confidence`.

## Plan
1. **S-1 Model load.** Load ONNX from `segmentation.model_path`. Validate signature
   (input dims, dtype, output channel count matches `classes` count in config).
   Fail fast on mismatch with a precise error; do not silently coerce.
2. **S-2 Device selection.** `device: cpu | cuda | tensorrt`. Probe availability; if
   requested device is unavailable, fail (do not silently fall back) — deterministic runs
   demand explicit device.
3. **S-3 Class map.** `classes` dict from config is the source of truth. Integer → name
   pairs are validated against the model's output channel count at load.
4. **S-4 Thresholds.** `confidence_threshold: 0.75`, `occlusion_threshold: 0.30`. Points
   below confidence become `uncertain` and are routed to `TOTaLi-QA-FLAGS`; occluded zones
   flagged for `TOTaLi-QA-OCCLUSION`.
5. **S-5 Batching.** `batch_size: 65536` points per forward pass. `voxel_size: 0.05 m` for
   voxelization input prep.
6. **S-6 Determinism.** Set all ONNX runtime seeds, disable nondeterministic optimizations
   (`OMP_NUM_THREADS=1` option or ONNX session `enable_mem_pattern=False` when determinism
   mode is on). Emit the ONNX runtime version in every `classify_start` audit.
7. **S-7 Non-authoritativeness enforcement.** The emitted artifacts carry an
   `authoritative: false` flag. Downstream phases must check; any phase reading segmentation
   output as authoritative is a bug.
8. **S-8 Confidence histogram.** Emit a per-class confidence histogram to the run manifest for
   triage — surveyors use this to calibrate acceptance rates over time.

## Rules
- The segmenter never writes to certified layers directly. Its only legal sink is
  `context.artifacts["segmentation"]`, consumed by extraction and linting.
- Confidence/occlusion thresholds are config-driven; hardcoding them is a rejection.
- No CUDA kernels authored here — the model is consumed as-is. Custom operators require a
  separate, reviewed design doc.
- TensorRT path is optional and must degrade cleanly (fail with clear message if missing).
- A new class id added to `classes:` must be accompanied by a test and a layer-mapping entry
  in `cad_shielding.layer_mapping`.

## Gates
1. `pytest tests/test_classifier.py -v` green.
2. Loading the production ONNX model succeeds on the CI image; CPU backend produces
   byte-identical label vectors across two runs on the golden input.
3. Configured device unavailable → phase fails loudly with a non-zero status and an audit entry.
4. `authoritative: false` flag present on every emitted segmentation artifact.

## Tests required
Existing:
- `tests/test_classifier.py`

Missing / to add:
- `tests/test_classifier_device_fail.py` — requesting CUDA when unavailable raises.
- `tests/test_classifier_determinism.py` — two CPU runs yield identical labels on fixture.
- `tests/test_classifier_class_map.py` — config/class-count mismatch fails at load.

## Dependencies
- **Upstream:** `totali/geodetic/` (validated CRS/units), `totali/models/` (ONNX load helpers).
- **Downstream:** `totali/extraction/`, `totali/linting/`, `totali/cad_shielding/` (via layer mapping).
- **External:** `onnxruntime`, `numpy`, optional `onnxruntime-gpu` / TensorRT.

## Open questions / known debts
- Reference ONNX model weight location is in config but no committed fixture exists.
  Use a small stub model for unit tests; keep production weights out of git (per `models/` governance).
- Voxel size and batch size may need auto-tuning for tile shapes; defer until after Phase 3 ships.

## Definition of Done
- Plan S-1..S-8 implemented with tests.
- Determinism test passes on two consecutive CI runs.
- Every emitted artifact is flagged non-authoritative and audited.
- Confidence histogram appears in `context.artifacts["segmentation"]["summary"]`.

## Progress (append-only)
- _(empty)_
