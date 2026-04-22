# Civil 3D REPL — Agentic Completion Plan

Scope: `totali/repl/` — `civil3d_repl.py`, `client.py`, `contracts.py`, `critic.py`.

## Purpose
Operator-facing interactive shell that interfaces with Civil 3D for surveyor review,
accept/reject loops, and measurement spot-checks. Provides a typed contract surface so the
REPL's behavior is testable without a live Civil 3D instance.

## Plan
1. **R-1 Contracts.** `contracts.py` hosts Pydantic models for every message crossing the
   REPL boundary: `SurveyorCommand`, `Civil3dResponse`, `DecisionRecord`.
2. **R-2 Client.** `client.py` wraps the Civil 3D IPC (AutoLISP / ObjectARX / .NET bridge).
   Must provide a fake client for tests.
3. **R-3 REPL loop.** `civil3d_repl.py` parses operator intent, calls the client, emits
   audit events for every accept/reject/defer.
4. **R-4 Critic.** `critic.py` performs lightweight validation (e.g., is the selected layer
   a `-DRAFT`? is the surveyor id set?) before forwarding a decision.
5. **R-5 Non-destructive commands only.** The REPL cannot issue raw DWG mutations that
   bypass `cad_shielding/`. Every mutation is a typed command with a known audit path.
6. **R-6 Transcript.** Every session produces a transcript under `artifacts/<run>/repl/session.jsonl`
   mirroring the audit events; operator-readable format.

## Rules
- The REPL never sends a raw `COMMAND` line to Civil 3D. Every call goes through a typed
  contract in `contracts.py`.
- Without a surveyor identity, the REPL refuses to start.
- The critic's checks are pre-send, not post-send. A decision that fails critic never reaches Civil 3D.
- Fake client stays in lock-step with the real one; any drift is a test failure.

## Gates
1. `pytest tests/test_civil3d_repl.py -v` green.
2. Critic test rejects decisions on non-DRAFT layers.
3. Transcript parity test: every audit event has a matching transcript entry.

## Tests required
Existing:
- `tests/test_civil3d_repl.py`

Missing / to add:
- `tests/test_repl_critic.py` — every critic rule covered.
- `tests/test_repl_transcript_parity.py` — audit vs transcript match.
- `tests/test_repl_fake_client.py` — fake client honors the contract surface.

## Dependencies
- **Upstream:** `totali/audit/`, `totali/cad_shielding/` (for layer name validation data),
  `totali/linting/` (for the decision model).
- **Downstream:** Civil 3D instance (runtime, not tested in CI).
- **External:** none in CI; real bridge uses AutoCAD-hosted .NET runtime.

## Open questions / known debts
- Whether to route the REPL through `laser-suite/dotnet` (existing .NET surface) or a
  dedicated bridge. Default: dedicated today, unify later if feasible.

## Definition of Done
- R-1..R-6 implemented and tested.
- Fake client parity test green.
- Transcript + audit parity test green.

## Progress (append-only)
- _(empty)_
