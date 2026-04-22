# Audit / Chain of Custody — Agentic Completion Plan

Scope: `totali/audit/` — `logger.py`.

## Purpose
Records every pipeline action to an append-only JSONL file with SHA-256 hash chaining.
This is the **defensible legal record** — all TOTaLi's correctness and compliance arguments
rely on this log being trustworthy. Not a logging convenience — a legal artifact.

## Inputs / Outputs
- **Input:** structured event name + payload from every phase and orchestrator checkpoint.
- **Output:** JSONL records under `audit_logs/<project>/<run>.jsonl`. Each record includes
  `ts`, `event`, `payload`, `prev_hash`, `hash`.

## Plan
1. **A-1 Record schema.** Pydantic model `AuditRecord`: `ts` (ISO-8601 UTC), `event` (str),
   `payload` (JSON-serializable), `prev_hash` (hex), `hash` (hex). `hash = sha256(prev_hash + canonical_json(record_without_hash))`.
2. **A-2 Canonical JSON.** Deterministic serialization: `sort_keys=True`, `separators=(',', ':')`,
   UTF-8, newline-terminated. The hash is computed on the exact bytes that will be written.
3. **A-3 Append-only enforcement.** `AuditLogger.open()` opens with `O_APPEND | O_CREAT`;
   writes are `write()` followed by `fsync()`. No seeks, no truncation, no rewrite. On
   Linux/macOS verified by test. Any API that would allow rewrite is not exposed.
4. **A-4 Hash chain verifier.** `totali.audit.verify` CLI walks a log and confirms every
   record's `hash == sha256(prev_hash + canonical_json(rec))`. First record's `prev_hash`
   is the empty string (or repo-genesis constant; pick one and freeze).
5. **A-5 Event allowlist.** `audit.log_events` in config lists accepted event names.
   Emitting an event not in the allowlist raises; this prevents typo'd events polluting the log.
6. **A-6 Rotation.** Logs do not rotate mid-run. A new `run_id` opens a new file. Historical
   files are never modified.
7. **A-7 Fsync on close.** `AuditLogger.close()` fsyncs and flushes, and writes a
   `run_end` record with the run's own final hash. Crash-recovery tests cover half-written lines.
8. **A-8 Logger API.** `logger.log(event: str, payload: dict)` is the only write surface.
   No second method that bypasses hashing, ever.

## Rules
- **`audit_logs/` is never modified by human or agent.** Editing or deleting entries is
  considered evidence tampering for TOTaLi's purposes.
- **No hash algorithm change without a version bump** + repo-wide migration plan. Today:
  `hash_algorithm: sha256`.
- **No partial writes.** A crash mid-line produces a detectable corruption, which the
  verifier reports; silent repair is forbidden.
- **No PII in payloads** beyond what is strictly necessary (surveyor id is allowed and
  expected; raw personal contact info is not).
- **Timestamps in UTC.** Local-time entries are a correctness bug.

## Gates
1. `pytest tests/test_audit_logger.py -v` green.
2. Hash chain verifier runs clean on a fresh run's log.
3. A deliberately tampered line makes the verifier fail.
4. Crash-simulation test (kill -9 mid-log) leaves a detectable state; verifier reports the
   exact broken offset.
5. Audit file has `0600` or `0640` permissions (no world-read) on POSIX.

## Tests required
Existing:
- `tests/test_audit_logger.py`

Missing / to add:
- `tests/test_audit_hash_chain.py` — verify clean chain, detect 1-byte mutation.
- `tests/test_audit_crash_recovery.py` — partial-line recovery semantics.
- `tests/test_audit_event_allowlist.py` — unknown event raises.
- `tests/test_audit_determinism.py` — identical events in identical order yield identical chain.

## Dependencies
- **Upstream:** stdlib (hashlib, json, os, tempfile), Pydantic.
- **Downstream:** every phase emits through this module.
- **External:** none (by design — no external audit service ever).

## Open questions / known debts
- Multi-process writer: not supported today. If the pipeline ever parallelizes phases,
  design a single-writer queue or per-process subfiles + a post-run merger.
- Signed audit (GPG / x509) not implemented; could layer on top without changing the chain.

## Definition of Done
- A-1..A-8 implemented and tested.
- Hash chain verifier ships as a CLI and is called by the global gate #10.
- File permissions test green.
- `totali.audit.verify` is documented in CLAUDE.md and this doc.

## Progress (append-only)
- _(empty)_
