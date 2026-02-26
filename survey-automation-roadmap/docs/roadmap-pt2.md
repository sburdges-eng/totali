# Project PT II Roadmap (v1.1.0 -> v2.0.0)

## Purpose

Project PT II starts after `v1.0.0` and drives this repository to a production-ready `v2.0.0` with broader input coverage, stronger operational safeguards, and explicit release criteria.

## v2.0.0 goals

1. Expand supported survey input handling while preserving deterministic outputs.
2. Harden operations with repeatable gates, triage guidance, and clear failure semantics.
3. Deliver release artifacts (`CHANGELOG`, runbook, validation evidence) with no open P0/P1 defects in scope.

## Scope lock

In scope for `v2.0.0`:

1. Parser/QC hardening and converter pipeline reliability.
2. CI and local quality-gate parity (`scripts/pt2_quality_gate.sh`).
3. Documentation parity across `README.md`, `docs/operations.md`, and release notes.

Out of scope for `v2.0.0`:

1. CLI architecture rewrite.
2. Removing quarantine behavior for unsupported formats.
3. Packaging/publishing to external registries.

## Cadence and governance

1. Weekly PT II gate: Monday 16:00 UTC via `.github/workflows/pt2-roadmap-gate.yml`.
2. Milestone review: Friday of each active milestone week.
3. Promotion rule: milestone closes only when all listed exit criteria are met and documented in this file.

## Current status (as of 2026-02-13)

1. Milestone 0 is complete.
2. Milestone 1 is complete with two consecutive CI PT II gate passes.
3. Local evidence captured on 2026-02-13:
   - `scripts/pt2_quality_gate.sh` PASS
   - `pytest -q`: `55 passed, 1 skipped`
   - pipeline gate run: `pt2-gate-20260213T092345Z` with accepted exit `2`
   - golden verification overall status: `PASS`
4. Main workflow evidence on 2026-02-13:
   - `CI` run `21981787789`: `PASS`
   - `PT2 Roadmap Gate` run `21982453407`: `PASS`

## Milestones

## Milestone 0 (Kickoff Baseline) - Completed on 2026-02-12

Delivered:

1. PT II roadmap and acceptance criteria.
2. Reusable gate script: `scripts/pt2_quality_gate.sh`.
3. Weekly PT II workflow: `.github/workflows/pt2-roadmap-gate.yml`.

## Milestone 1 (Coverage and Controls) - Target: 2026-02-13 to 2026-03-06

Objective:

1. Ship coverage and hardening updates that remove known correctness gaps while preserving deterministic behavior.

Delivered work:

1. Added text-point ingestion for `.txt`, `.pts`, `.asc` when rows match point-style numeric schema.
2. Made parser column validation config-driven (`required_point_columns`, `required_field_code_columns`).
3. Hardened CRD converter path:
   - static + smoke checks use consistent environment mapping
   - unsupported converted output is quarantined or failed explicitly (no forced mixed parsing)
4. Improved artifact/QC behavior:
   - parquet write failures no longer leave empty placeholder artifacts
   - duplicate `point_id` findings include all duplicate occurrences
   - `profile --quiet` suppresses stdout JSON duplication
5. Added tests and doc-path verification to protect runbook/readme drift.

Exit criteria:

1. `pytest -q` passes.
2. PT II quality gate passes in CI for two consecutive runs.
3. Documentation reflects operator impact for all shipped changes.

Closeout status:

1. Criteria 1, 2, and 3 are met.
2. CI evidence:
   - `21979007569` (success): `https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21979007569`
   - `21978984071` (success): `https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21978984071`

## Milestone 2 (Operational Hardening) - Target: 2026-03-09 to 2026-03-27

Objective:

1. Reduce production noise and tighten release-candidate controls.

Planned work:

1. Add release-candidate gate checklist and enforce it in docs and CI workflow notes.
2. Tune warning-threshold and quarantine triage guidance using recent PT II gate outputs.
3. Complete remaining low-risk hardening/documentation decisions from `BUGS_AND_FIXES.txt` (for example finding-id lifecycle documentation and config/env expansion semantics).
4. Add/adjust tests for any behavior changes introduced by triage or threshold tuning.

Progress notes:

- 2026-02-13: Added executable release-candidate gate script `scripts/v2_release_candidate_gate.sh`.
- 2026-02-13: Added release-candidate checklist `docs/release-candidate-checklist.md` and CI workflow `.github/workflows/v2-release-candidate-gate.yml`.
- 2026-02-13: Switched finding-id finalization to a non-mutating pipeline flow and added dedicated unit coverage.
- 2026-02-13: Executed release-candidate gate end-to-end (`pt2-gate-20260213T022645Z`) with golden verification `PASS`.

Exit criteria:

1. Golden verification remains PASS after each hardening change.
2. CI warning/failure semantics are explicit, tested, and unchanged unless intentionally versioned.
3. `docs/operations.md` escalation playbook and PT II gate notes reflect final v2 behavior.

Closeout status:

1. Criteria 1 and 2 are met with local validation evidence from `scripts/v2_release_candidate_gate.sh`.
2. Criterion 3 is met by updated runbook and release-candidate sections in `docs/operations.md`.
3. Milestone 2 is delivered.

## Milestone 3 (Release Readiness) - Target: 2026-03-30 to 2026-04-10

Objective:

1. Lock v2 scope and ship with full validation evidence.

Planned work:

1. Freeze in-scope feature list and finalize `v2.0.0` changelog entries.
2. Run full pre-release gate (tests, converter preflight, pipeline, golden verification) on `main`.
3. Resolve all in-scope P0/P1 defects and record disposition for deferred items.
4. Tag and release after approval criteria are satisfied.

Progress notes:

- 2026-02-13: Added release notes draft `docs/release-notes-v2.0.0.md`.
- 2026-02-13: Started concurrent execution across release streams 1-6 and added tracker `docs/v2-concurrent-execution.md`.
- 2026-02-13: Ran `scripts/v2_release_candidate_gate.sh` kickoff baseline (`pt2-gate-20260213T074034Z`) to seed stream evidence.
- 2026-02-13: Ran deterministic production-profile batch (`v2-kickoff-20260213T074227Z`) and attached manifest/QC evidence under `artifacts/`.
- 2026-02-13: Ran corrected production-profile batch (`v2-kickoff-fixed-20260213T074455Z`) with converter env set and generated warning triage report `docs/v2-warning-hotspots.md`.
- 2026-02-13: Added production warning-noise controls (`duplicate_point_id_mode`, `unmapped_description_skip_categories`) and tightened production include globs.
- 2026-02-13: Ran noise-tuned production-profile batch (`v2-noise-tuned2-20260213T084839Z`), reducing warnings from `3723` to `20`.
- 2026-02-13: Applied targeted data fixes and binary-DXF exclusions, then ran clean production-profile batch (`v2-noise-fixed-20260213T092320Z`) with `0` warnings.
- 2026-02-13: Captured latest main gate evidence (`CI` run `21981787789`, `PT2` run `21982453407`) as release proof points.

Exit criteria:

1. All required gates pass on `main`.
2. Changelog and release notes are complete and review-approved.
3. No unresolved P0/P1 defects remain for in-scope PT II work.

Closeout status:

1. Criterion 1 is met (`CI` run `21981787789`, `PT2` run `21982453407`).
2. Criterion 2 is met (release notes approved on 2026-02-13).
3. Criterion 3 is met (no unresolved in-scope P0/P1 defects).
4. Milestone 3 is delivered.

## PT II quality gate command

Run from repository root:

```bash
scripts/pt2_quality_gate.sh
```

Gate validates:

1. Test suite health (`pytest -q`).
2. Converter preflight (`check-converter` with CI config).
3. CI-config pipeline run with accepted exit policy (`0` or `2`).
4. Golden regression verification and `validation/last_validation.md` refresh.

## Status tracker

- [x] Milestone 0 kickoff artifacts merged
- [x] Milestone 1 delivered
- [x] Milestone 2 delivered
- [x] Milestone 3 delivered

## Update protocol

When PT II work lands:

1. Add a date-stamped note under the active milestone.
2. Link evidence (run id, gate output, test command) for every exit criterion change.
3. Update milestone checkbox state only when all criteria are met.
