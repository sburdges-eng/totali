# Quarantine UI — Agentic Completion Plan

Scope: `totali/quarantine_ui/` — `app.py` (Flask).

## Purpose
Blocking operator UI on port 5050 that resolves CRS ambiguity when geodetic inference
confidence is below threshold. The pipeline halts until an operator selects the correct
CRS or marks the input rejected.

## Plan
1. **Q-1 Routes.**
   - `GET /` — list pending quarantines with file, inferred candidates, confidence scores.
   - `POST /resolve` — operator selects CRS; server records decision and releases the gate.
   - `POST /reject` — operator marks input rejected; pipeline halts the run.
   - `GET /health` — liveness probe.
2. **Q-2 Gate semantics.** The pipeline waits on a thread event (or file watch). The UI
   writes the resolution to a well-known file, then sets the event.
3. **Q-3 Auth.** Basic auth (env-configured) for any non-localhost bind. Localhost-only by
   default.
4. **Q-4 Audit emission.** Every resolve/reject emits `crs_resolved` / `crs_rejected_by_operator`
   with operator id and timestamp.
5. **Q-5 Idempotent.** A resolution is recorded once; duplicate POSTs return 409 with the
   existing decision.
6. **Q-6 No silent exits.** If the UI is not reachable and the pipeline needs it, the
   orchestrator surfaces a clear error (not a generic timeout).

## Rules
- Port 5050 only (config-driven, but default is fixed). Dual-binding is a misconfiguration.
- Localhost by default. World-bindable only with explicit config + auth.
- No operator identity, no resolution — the POST is rejected.
- The UI writes **one** decision per file; reassignment requires a new quarantine entry.

## Gates
1. `pytest tests/test_quarantine_ui.py -v` green.
2. Werkzeug test client exercises all four routes.
3. Audit events present after resolve/reject.
4. World-bind without auth rejected by config validator.

## Tests required
Existing:
- `tests/test_quarantine_ui.py`

Missing / to add:
- `tests/test_quarantine_ui_auth.py` — unauth POST denied when non-localhost.
- `tests/test_quarantine_ui_idempotent.py` — duplicate resolve returns 409.
- `tests/test_quarantine_ui_audit.py` — events emitted on resolve/reject.

## Dependencies
- **Upstream:** `totali/geodetic/`, `totali/audit/`.
- **Downstream:** pipeline orchestrator (blocks until resolution).
- **External:** Flask, Werkzeug.

## Open questions / known debts
- Whether to persist unresolved quarantines across pipeline restarts — today ephemeral.
  Decide before production.

## Definition of Done
- Q-1..Q-6 implemented and tested.
- CI test exercises the full resolve + reject lifecycle.
- Default bind is localhost; production config example with auth documented in operations doc.

## Progress (append-only)
- _(empty)_
