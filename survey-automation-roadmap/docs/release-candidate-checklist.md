# v2.0.0 Release Candidate Checklist

Use this checklist before promoting any `v2.0.0` release candidate.

## Required gate command

Run from repository root:

```bash
scripts/v2_release_candidate_gate.sh
```

This command enforces:

1. PT II baseline gate (`scripts/pt2_quality_gate.sh`).
2. Documentation path consistency check (`tests/unit/test_docs_paths.py`).
3. Production config validation (`config/pipeline.prod.yaml`).
4. Production converter preflight (`check-converter` on production config).
5. Continuous eval gate (`scripts/eval_gate.py`) using held-out quality/failure-bucket metrics.

## Release candidate checklist

- [x] Record gate timestamp and operator name.
- [x] Record PT II gate run id from script output.
- [x] Confirm golden verification status is `PASS`.
- [x] Confirm no unresolved P0/P1 defects remain in scope.
- [x] Confirm `CHANGELOG.md` includes all in-scope PT II entries.
- [x] Confirm `docs/release-notes-v2.0.0.md` is finalized and review-approved.
- [x] Confirm `docs/operations.md` and `README.md` reflect final behavior.
- [x] Confirm CI PT II gate has two consecutive passing runs.

## Evidence to attach

1. Gate command output log.
2. `validation/last_validation.md`.
3. `artifacts/<run-id>/reports/qc_summary.json`.
4. `artifacts/<run-id>/manifest/run_manifest.json`.
5. `artifacts/eval_gate_report.json`.

## Latest local kickoff evidence

1. Gate timestamp: `2026-02-13T09:23:45Z`.
2. Operator: `codex`.
3. PT II gate run id: `pt2-gate-20260213T092345Z`.
4. Golden verification overall status: `PASS`.
5. Full test suite: `55 passed, 1 skipped`.
6. Production profile run id: `v2-noise-fixed-20260213T092320Z` (`exit_code: 0`, warnings `0`).

## Latest CI consecutive-pass evidence

1. Run `21979007569` (success): `https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21979007569`
2. Run `21978984071` (success): `https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21978984071`

## Latest main gate evidence

1. CI run `21981787789` (success): `https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21981787789`
2. PT II run `21982453407` (success): `https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21982453407`

## P0/P1 disposition

1. No unresolved P0/P1 defects remain in the current v2 scope.

## Release notes approval

1. Approved by stakeholder in-session on 2026-02-13.

## Draft release evidence

1. Draft release created on GitHub for tag `v2.0.0` (release id `286036076`).
2. Draft URL: `https://github.com/sburdges-eng/survey-automation-roadmap/releases/tag/untagged-667af97ce7e78638a171`.
3. Note: GitHub uses an `untagged-*` URL slug for draft releases; tag binding is `v2.0.0`.
