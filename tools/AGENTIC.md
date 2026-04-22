# Tools — Agentic Completion Plan

Scope: `tools/` — `extract_snippets_evidence.py`, `extract_snippets_strict.py`,
`generate_pattern_catalog.py`, `test_schema.py`, `TOTaLi_Cross_Analysis.jsx`.

## Purpose
Standalone CLI / scripts that help operators and agents inspect, validate, or extract
information from TOTaLi artifacts and source. These are **out-of-pipeline** tools; they
never write to `audit_logs/` or promote geometry.

## Plan
1. **TL-1 CLI discipline.** Every script exposes `-h/--help`, exits non-zero on error, and
   is usable without reading its source.
2. **TL-2 Pure outputs.** Tools emit to stdout or an explicit `--output` path. No implicit
   writes to repo-controlled paths (`audit_logs/`, `artifacts/`, `Datasets/`).
3. **TL-3 Schema test.** `test_schema.py` validates that any schema a tool emits conforms to
   a documented JSON schema. If a new schema is added, a new schema file lands in the same PR.
4. **TL-4 Determinism.** Tools are deterministic given input — no randomness, no wall-clock
   in outputs. If a timestamp is useful, accept it from `--now` for reproducibility.
5. **TL-5 README per tool.** `README_<tool>.md` is authoritative; keep in sync with the
   tool's `--help`.

## Rules
- No tool in this directory may modify pipeline configuration or phase outputs in place.
- No tool may emit audit events — they are out-of-pipeline.
- The `.jsx` tool (Civil 3D script) stays here; any script targeting Civil 3D goes in
  `laser-suite/dotnet` or `totali/repl/`, not this directory.

## Gates
1. `pytest tools/test_schema.py -v` green.
2. `python tools/<tool>.py --help` exits 0 for every script.
3. README for each tool exists and matches `--help`.

## Tests required
Existing:
- `tools/test_schema.py`

Missing / to add:
- `tools/test_help.py` — every `.py` script exposes `--help` and exits 0.
- `tools/test_determinism.py` — fixed-input double-run identical-output per tool.

## Dependencies
- **Upstream:** stdlib, minimal project imports.
- **Downstream:** operator CLIs.

## Open questions / known debts
- `TOTaLi_Cross_Analysis.jsx` origin and usage path — document in a README here; consider
  moving to `AUTOMATICCAD/` if it's a CAD-side script.

## Definition of Done
- Every tool has a paired README and `--help`.
- Schema test green.
- Determinism test green.

## Progress (append-only)
- _(empty)_
