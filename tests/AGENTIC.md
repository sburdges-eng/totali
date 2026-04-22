# Tests — Agentic Completion Plan

Scope: `tests/` — pytest suite (`testpaths = tests`, `-v --tb=short`).

## Purpose
Single source of truth for automated verification of TOTaLi's behavior. Every module's
Definition of Done references tests in this directory.

## Current inventory
- `test_audit_logger.py`
- `test_base_phase.py`
- `test_civil3d_repl.py`
- `test_classifier.py`
- `test_context.py`
- `test_crs_inference.py`
- `test_dwg_parser.py`
- `test_extractor.py`
- `test_geodetic.py`
- `test_geometry_healer.py`
- `test_integration.py`
- `test_orchestrator.py`
- `test_quarantine_ui.py`
- `test_shield.py`
- `test_surveyor_lint.py`

## Plan
1. **T-1 Layout.** Keep one module-under-test per test file. Integration tests in
   `test_integration.py`. Sub-scenario files use `test_<module>_<scenario>.py`.
2. **T-2 Fixture hierarchy.** `conftest.py` provides:
   - `golden_pointcloud` — small, committed, CRS-known `.las`
   - `golden_config` — pinned copy of `config/pipeline.yaml`
   - `tmp_audit_logger` — fresh logger in a `tmp_path`
   - `fake_civil3d_client` — importable from tests
3. **T-3 Golden regression.** One end-to-end test per pipeline slice:
   - geodetic-only (from golden pointcloud → crs artifact)
   - through segmentation (with a stub ONNX model)
   - through extraction (byte-parity on GeoJSON output)
   - through shielding (byte-parity on DXF)
   - full pipeline (audit chain verifies)
4. **T-4 Determinism battery.** Cross-module determinism tests for extraction, shielding,
   and audit emission.
5. **T-5 Negative-path tests.** For every guard-rail (`auto_promote: true`, bad layer name,
   unknown event, mixed datum, world-bind without auth), a test that asserts the guard fires.
6. **T-6 Gate test.** `test_global_gates.py` confirms the CLI commands in `AGENTIC_COMPLETION_PLAN.md`
   §5 all produce expected exit codes on the golden input.

## Rules
- Tests never touch `audit_logs/`, `Datasets/`, or `artifacts/` at repo root — only
  `tmp_path` from pytest.
- No network access in tests (mark network tests `-m network` and exclude from default run).
- No test weakens an assertion to "make CI green." If a test is wrong, fix the test in a
  separate commit with a written justification.
- Golden fixtures committed under `tests/fixtures/` are small, hand-validated, and have
  their own README documenting provenance.
- Every PR that adds behavior adds or extends a test.

## Gates
1. `pytest -q` green on main.
2. `pytest -q --cov=totali --cov-fail-under=80` green (target; raise once baseline established).
3. `pytest -q -m "not slow"` default; slow/integration tests runnable with `-m slow`.

## Tests required (cross-reference)
Each module's `AGENTIC.md` lists the tests it owns. This file's job is to keep the
layout coherent. When a module adds a test, add it to the inventory above.

## Dependencies
- **Upstream:** every TOTaLi module.
- **External:** `pytest`, `pytest-cov` (optional), `freezegun` (if time-based tests appear).

## Open questions / known debts
- ONNX stub model for `test_classifier.py` — pick a repeatable tiny model (single-layer MLP
  exported to ONNX) and commit under `tests/fixtures/models/`.
- Coverage target: start with 80, raise to 90 after Milestone 3.

## Definition of Done
- Inventory reflects reality (no orphan tests, no missing files listed).
- T-3 end-to-end golden suite green.
- T-5 negative-path battery complete.
- Coverage ≥ 80 % on `totali/`.

## Progress (append-only)
- _(empty)_
