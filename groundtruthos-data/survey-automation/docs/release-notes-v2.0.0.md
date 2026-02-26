# Release Notes Draft - v2.0.0

Release date: TBD

## Scope

`v2.0.0` finalizes PT II hardening work on parser behavior, converter handling, operational gates, and release controls.

## Highlights

1. Parser validation now follows config-driven point and field-code required columns.
2. Binary CRD converted output handling is explicit (supported parse path or quarantine/fatal in strict mode).
3. Parquet output failure behavior is deterministic and no longer leaves placeholder files.
4. Duplicate point-id findings now support configurable aggregation (`all_occurrences`, `per_point_id`, `within_file`).
5. New release-candidate gate and checklist enforce pre-release quality controls.
6. Production warning-noise controls now support category-based unmapped-code skipping and tighter include-glob scope.

## Operational additions

1. PT II gate script: `scripts/pt2_quality_gate.sh`
2. Release-candidate gate script: `scripts/v2_release_candidate_gate.sh`
3. Release-candidate checklist: `docs/release-candidate-checklist.md`
4. Manual release-candidate workflow: `.github/workflows/v2-release-candidate-gate.yml`

## Validation evidence (latest local run)

1. Gate command: `scripts/v2_release_candidate_gate.sh`
2. PT II gate run id: `pt2-gate-20260213T092345Z`
3. Tests: `55 passed, 1 skipped`
4. Golden verification overall status: `PASS`
5. Production profile run: `v2-noise-fixed-20260213T092320Z` (`3723` -> `0` warnings vs baseline)

## Known release blockers

1. Maintain clean-run evidence (`v2-noise-fixed-20260213T092320Z`) through tag cut.

## Upgrade and compatibility notes

1. No CLI command removals were introduced in PT II.
2. Converter command environment expansion remains runtime-based (`check-converter` and `run` execution).
3. Existing warning/quarantine exit semantics remain (`0`, `2`, `3`).
