# Surveyor Linting — Agentic Completion Plan

Scope: `totali/linting/` — `surveyor_lint.py`.

## Purpose
Phase 5. Presents the draft CAD to a licensed surveyor with ghost suggestions (40 % opacity,
confidence-colored). Captures accept/reject decisions and writes them to the audit log.
Never auto-promotes.

## Inputs / Outputs
- **Input:** the draft DXF from `cad_shielding/`, the classified / extracted artifacts for
  confidence data, config section `linting:`.
- **Output:** surveyor decision record (`accepted`, `rejected`, `deferred` per feature),
  audit events `lint_session_start`, `feature_accepted`, `feature_rejected`, `feature_deferred`,
  `lint_session_end`. Certified promotion is a **separate** downstream step this phase does
  not perform.

## Plan
1. **L-1 Ghost overlay.** Emit visualization metadata for each suggested feature:
   `opacity: 0.4`, color per `flag_colors` (`#00FF00` high, `#FFAA00` medium, `#FF0000` low,
   `#FF00FF` occluded). Surveyor UI renders these; this module produces the metadata only.
2. **L-2 Accept/reject capture.** Each feature is individually accepted or rejected. No bulk
   accept. No "accept all above confidence X" shortcut.
3. **L-3 PLS signature gate.** `require_pls_signature: true` is hardcoded default. A
   surveyor identity is captured on every session and attached to every event.
4. **L-4 Auto-promote lock.** `auto_promote: false` is hardcoded. The loader rejects configs
   that try to set it `true`; this is a guard-rail test in `tests/test_surveyor_lint.py`.
5. **L-5 Deferred features.** Features marked deferred remain on `TOTaLi-*-DRAFT` and flow
   to the next session. No silent drop.
6. **L-6 Session evidence.** On `lint_session_end` emit a summary: counts by decision,
   surveyor id, session duration, run id.
7. **L-7 Confidence tie-in.** The low-confidence bucket (below segmentation threshold)
   renders red and is pre-selected for reviewer attention.

## Rules
- **Never change `auto_promote` behavior.** Code that reads config must guard against `true`.
- **Never promote silently.** Promotion (drop `-DRAFT` suffix) is a separate, human-driven
  step owned by the surveyor, not this phase.
- **One decision per feature.** No bulk actions in the data model.
- Every accept/reject/defer is an audit event; losing one is a correctness bug.
- Ghost visualization metadata is the only UI-rendering responsibility here — no GUI code.

## Gates
1. `pytest tests/test_surveyor_lint.py -v` green.
2. Guard test: config with `auto_promote: true` raises at load.
3. Accept/reject counts in the session summary match the individual events.
4. Surveyor id appears on every event in a session.

## Tests required
Existing:
- `tests/test_surveyor_lint.py`

Missing / to add:
- `tests/test_linting_auto_promote_guard.py` — config tampering rejected.
- `tests/test_linting_event_count_parity.py` — summary counts == event counts.
- `tests/test_linting_deferred_persists.py` — deferred features survive to next session.

## Dependencies
- **Upstream:** `totali/cad_shielding/` (the draft DXF), `totali/segmentation/` (confidence data).
- **Downstream:** `totali/audit/` (event sink), surveyor UI (out of repo).
- **External:** none specific beyond stdlib + Pydantic models.

## Open questions / known debts
- Promotion workflow (drop `-DRAFT` suffix) does not yet live in code. Today it's a manual
  CAD step. Consider scaffolding a guarded `promote.py` with PLS-signature enforcement —
  but that is a separate module, not part of this phase.
- Session persistence format (JSONL next to audit) vs standalone survey-session file —
  decide before multi-session deferred flow ships.

## Definition of Done
- L-1..L-7 implemented with tests.
- Guard test against `auto_promote: true` green.
- Event/summary parity verified.
- Manual smoke test: fake surveyor session accepts N, rejects M, defers K → audit log reflects
  all events and final summary matches.

## Progress (append-only)
- _(empty)_
- 2026-06-18 — U3 export-blocked-until-certified INTEGRATION test: drives real SurveyorLinter.run() output through export_blocked -> accept_item -> promote_to_certified (was only unit-tested on hand-built items). tests/test_surveyor_lint.py::TestExportBlockedUntilCertifiedIntegration. Suite green.
