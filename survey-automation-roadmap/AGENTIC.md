# Survey Automation Roadmap — Agentic Completion Plan

Scope: `survey-automation-roadmap/` — v2.0.0 production CLI for ingesting mixed survey
inputs (.csv / ASCII .dxf / .crd / .txt / .pts / .asc) with warn-and-quarantine behavior,
CRD converter preflight, golden regression, PT II quality gate, and release-candidate gate.

This subproject is at **v2.0.0 released (2026-02-13)**. PT II Milestones 0–3 delivered.
Treat it as in-maintenance; changes land through the existing gates.

## Purpose
Production-grade CLI layer that feeds validated, normalized survey data to TOTaLi's
pipeline. Owns the mixed-input quarantine model, binary CRD conversion contract,
`phase_presentation` metadata, and trend-tracking baselines.

## Surface
```
survey-automation validate --input-dir <dir> --config <yaml>
survey-automation check-converter --config <yaml> [--sample-crd <path>]
survey-automation profile --input-dir <dir> --output <json> [--quiet] [--config <yaml>]
survey-automation run --input-dir <dir> --config <yaml> --output-dir <dir> [--run-id <id>]
survey-automation doctor [--config <yaml>] [--input-dir <dir>] [--output-dir <dir>] [--sample-crd <path>]
```

Exit codes: `0` clean, `2` warnings/quarantines (still a pass in CI), `3` fatal.

## Plan (maintenance-mode)
1. **SA-1 Trend-baseline hygiene.** `project.baseline_namespace` isolates per-project
   baselines. Any new project gets its own namespace; no cross-project contamination.
2. **SA-2 Converter contract.** `docs/crd-converter.md` is authoritative. Any converter
   swap goes through `check-converter` preflight (static + smoke) before production runs.
3. **SA-3 Quarantine triage.** `docs/operations.md` escalation playbook is the runbook.
   New warning codes land with a triage entry.
4. **SA-4 Gate scripts.** `scripts/pt2_quality_gate.sh` and `scripts/v2_release_candidate_gate.sh`
   are the two mandatory gates. Additions require a milestone note in `docs/roadmap-pt2.md`.
5. **SA-5 Golden verification.** `validation/verify_golden.py` + `validation/write_last_validation.py`
   keep `validation/last_validation.md` current.
6. **SA-6 Doctor.** `survey-automation doctor` stays actionable (returns exit 3 on real
   failures, with per-check `color` metadata). Any new check writes a fix hint.
7. **SA-7 Phase presentation.** `phase_presentation` metadata (ground_truth → phase_1 → 2 → 3)
   never changes exit-code semantics.

## Rules
- v2 scope is locked. Non-trivial feature work opens Milestone 4 in `docs/roadmap-pt2.md` first.
- CI uses `config/pipeline.ci.yaml`. Production uses `config/pipeline.prod.yaml`. Do not cross them.
- `CRD_CONVERTER_COMMAND` carries `{input}` and `{output}` placeholders — never rename.
- Exit-code contract is public API. Changing it is a major-version bump.
- CI treats exit 0 and exit 2 as pass. Do not change this in workflow files without a roadmap note.
- Golden regression must remain PASS on main. A golden-breaking change lands with an updated
  golden set in the same PR, justified in the PR body.

## Gates
1. `pytest -q` green (baseline: `55 passed, 1 skipped` as of 2026-02-13).
2. `scripts/pt2_quality_gate.sh` PASS.
3. `scripts/v2_release_candidate_gate.sh` PASS (for release candidates).
4. `python validation/verify_golden.py` overall status PASS.
5. CI weekly PT II gate (`.github/workflows/pt2-roadmap-gate.yml`) — two consecutive passes required before merge to main.
6. Doc parity: `tests/unit/test_docs_paths.py` green.

## Tests required
Existing suite under `tests/` (pytest). Maintained.

Add-on obligations:
- New converter: add smoke-check fixture under `samples/` and extend `test_check_converter`.
- New warning code: add parser + triage test; document in `docs/operations.md`.

## Dependencies
- **Upstream:** Python env per `pyproject.toml`; CRD converter binary at `scripts/converter`
  or operator-provided equivalent.
- **Downstream:** Feeds validated data to TOTaLi core via `artifacts/<run-id>/normalized/`.

## Open questions / known debts
- CLI architecture rewrite is explicitly out of scope for v2 (roadmap §Scope lock).
- Quarantine behavior for `.dwg` and binary DXF remains out of scope; revisit in Milestone 4 if opened.

## Definition of Done
- v2.0.0 tag remains green on main.
- PT II gate + RC gate both PASS.
- Golden verification PASS on main.
- No unresolved P0/P1 in scope.

## Progress
- 2026-02-13 — v2.0.0 shipped; Milestones 0–3 closed; local + CI evidence in `docs/roadmap-pt2.md`.

## Progress (append-only)
- _(append new dated entries here as maintenance changes land)_
