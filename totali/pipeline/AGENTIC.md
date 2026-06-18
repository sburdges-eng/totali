# Pipeline Orchestration — Agentic Completion Plan

Scope: `totali/pipeline/` — `base_phase.py`, `context.py`, `models.py`, `orchestrator.py`.

## Purpose
Enforces the **AI classifies → Algorithms measure → Humans certify** sequence by running
phase processors in a fixed order with prerequisite expansion. Holds the contract every
phase must implement. Owns the `PipelineContext` and `PipelineResult` data model.

## Inputs / Outputs
- **Input:** `config` (dict from `config/pipeline.yaml`), `AuditLogger`, `output_dir`,
  input path (LAS/LAZ/point CSV/etc.), optional `--phase` selector.
- **Output:** `PipelineResult` with per-phase `PhaseResult` records, final success flag,
  populated `PipelineContext.artifacts` / `phase_status` / `errors`.

## Plan
1. **P-1 Contract stability.** Freeze `PipelinePhase` abstract base: `run`, `validate_config`,
   `validate_inputs`, `get_required_inputs`, `get_provided_outputs`. Add explicit docstrings
   for each; never remove a method from the ABC without migrating all phases in one PR.
2. **P-2 Context schema.** `PipelineContext` is a Pydantic model with `input_path`,
   `output_dir`, `artifacts` (dict[str, Path]), `phase_status` (dict[str, str]), `errors`
   (list[str]). Additions require a migration note.
3. **P-3 Phase order constant.** `PHASE_ORDER = ["geodetic","segment","extract","shield","lint"]`
   is source-of-truth. `--phase X` runs X and all upstream phases; never skips prerequisites.
4. **P-4 Failure semantics.** A phase's `validate_inputs` returning `(False, errs)` marks
   the result failed and halts downstream. `run()` raising halts and logs `phase_exception`.
5. **P-5 Audit coupling.** Orchestrator emits `phase_start`, `phase_end`, `phase_failed`,
   `phase_exception` for every phase. Payload includes phase name, duration, summary.
6. **P-6 Timeouts.** Each phase honors `middleware_timeout_sec` from its own config section;
   exceeding it raises `PhaseTimeout`, logged as `phase_timeout`.
7. **P-7 Orchestrator tests.** `tests/test_orchestrator.py` covers: full run happy path,
   single-phase with prereqs, failure propagation, exception handling, timeout, context
   population after each phase.
8. **P-8 CLI wiring.** `totali/main.py` feeds the orchestrator; keep argument surface
   frozen: `--input`, `--config`, `--output-dir`, `--phase`, `--run-id`.

## Rules
- `PipelinePhase` is the **only** permitted base class for new phases. Do not create parallel ABCs.
- `PHASE_ORDER` is immutable at runtime. Never mutate by patching; define extension via a new
  ABC subclass registry if extension is ever needed (not today).
- `PipelineContext` fields are additive-only. Removing or renaming a field is a breaking change
  that requires bumping `project.version` and updating all phases + tests.
- Orchestrator never catches a `KeyboardInterrupt` to "recover" — propagate.
- No phase ever writes to `audit_logs/` directly; it goes through `AuditLogger`.

## Gates
1. `pytest tests/test_orchestrator.py tests/test_base_phase.py tests/test_context.py -v` green.
2. `pytest tests/test_integration.py -v` green (end-to-end exercise of phase order).
3. Orchestrator run with `--phase extract` on golden input produces all three prereq phases'
   artifacts in `PipelineContext.artifacts`.
4. No phase module imports `totali.pipeline.orchestrator` (one-way dependency).

## Tests required
Existing:
- `tests/test_orchestrator.py`
- `tests/test_base_phase.py`
- `tests/test_context.py`
- `tests/test_integration.py` (covers pipeline stitching)

Missing / to add:
- `tests/test_orchestrator_timeout.py` — phase-timeout path (P-6).
- `tests/test_orchestrator_failure.py` — validate_inputs failure halts downstream (P-4).
- `tests/test_phase_contract.py` — every phase class in `totali.*` implements the full ABC.

## Dependencies
- **Upstream:** `totali/audit/` (AuditLogger required at construction).
- **Downstream:** all five phase modules, `totali/main.py`.

## Open questions / known debts
- No registry for dynamic phase discovery; phase imports are hardcoded in
  `PipelineOrchestrator.__init__`. Keep until a second consumer emerges.
- `PipelineResult.success` defaults to `True`; confirm all failure paths flip it before return
  (audit this via the new failure test).

## Definition of Done
- All P-1..P-8 items land with tests.
- `test_phase_contract.py` asserts every real phase satisfies the ABC via introspection.
- Running `python -m totali.main --input <golden> --config config/pipeline.yaml --phase lint`
  produces populated artifacts for all five phases and a valid audit log tail.
- Zero `except Exception` blocks that swallow without re-raising or logging.

## Progress (append-only)
- 2026-04-22 — P-8 CLI wiring: `tests/test_phase_contract.py` 25 passed (5 phases × 5 contract assertions); every phase satisfies the ABC. (ledger: P-8 @ 2026-04-22T03:22:00Z)
- 2026-04-22 — P-6 timeouts: `ThreadPoolExecutor`-based enforcement with `PhaseTimeout` + `phase_timeout` audit event; `middleware_timeout_sec` live for shield phase; `tests/test_orchestrator_timeout.py` 8 passed. (ledger: P-6 @ 2026-04-22T03:58:00Z)
- 2026-04-22 — P-4 failure semantics: `validate_inputs` failure + `run()` exception both halt pipeline with `phase_failed` audit event; `tests/test_orchestrator_failure.py` 5 passed. (ledger: P-4 @ 2026-04-22T04:12:00Z)
