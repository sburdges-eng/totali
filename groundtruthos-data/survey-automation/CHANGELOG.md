# Changelog

## Unreleased - 2026-02-12

### PT II kickoff

- Added PT II roadmap and milestone plan in `docs/roadmap-pt2.md`.
- Added reusable gate script `scripts/pt2_quality_gate.sh` for local and CI execution.
- Added weekly PT II workflow `.github/workflows/pt2-roadmap-gate.yml`.
- Linked PT II roadmap from `README.md`.

### Parser and pipeline hardening

- Parser point/field row length checks now use config-driven column counts (`required_point_columns` and `required_field_code_columns`).
- Added support for configurable field-code required columns in config validation.
- Stopped fallback parsing of unsupported converted CRD output; now quarantines (or fails in `converter_required` mode) with explicit reason.
- Parquet write failures no longer create empty placeholder files.
- Duplicate point-id QC now reports all duplicate occurrences.
- Added `profile --quiet` to suppress stdout JSON output when writing `--output`.
- Updated converter smoke execution to use the same environment mapping as static checks and command execution.
- Added tests that validate documentation path references exist.

### PT II milestone 2 execution

- Added release-candidate gate script `scripts/v2_release_candidate_gate.sh` for pre-release enforcement.
- Added release-candidate checklist `docs/release-candidate-checklist.md` with required evidence artifacts.
- Added manual release-candidate CI workflow `.github/workflows/v2-release-candidate-gate.yml`.
- Switched finding-id assignment to non-mutating finalization (`pipeline._finalize_findings`) and added coverage in `tests/unit/test_finding_ids.py`.
- Added v2 release notes draft `docs/release-notes-v2.0.0.md` and linked it from release documentation.

### PT II milestone 3 kickoff

- Added concurrent execution tracker `docs/v2-concurrent-execution.md` for streams 1-6.
- Refreshed release evidence with kickoff gate run `pt2-gate-20260213T074034Z`.
- Added warning hotspot triage report `docs/v2-warning-hotspots.md` from run `v2-kickoff-fixed-20260213T074455Z`.
- Captured two consecutive PT II CI gate passes (`21979007569`, `21978984071`) in release checklist evidence.
- Added configurable duplicate-point QC aggregation (`all_occurrences`, `per_point_id`, `within_file`).
- Added configurable unmapped-description skip categories for QC noise control.
- Tightened production input globs to supported extensions and reduced production warning volume to `20` in run `v2-noise-tuned2-20260213T084839Z`.
- Removed invalid rows from `.local-datasets/TOTaLi/TEST BRIDGEY.csv` and `.local-datasets/TOTaLi/FGD_Template_With_Preview (4).csv`.
- Excluded known binary DXF files from production discovery and achieved clean production run `v2-noise-fixed-20260213T092320Z` (`exit_code: 0`).

## v1.0.0 - 2026-02-12

### Highlights

- Finalized the v1 CLI workflow for `validate`, `profile`, `run`, and `check-converter`.
- Added production-ready CRD converter checks for environment variable expansion, executable resolution, placeholder validation, and optional smoke conversion.
- Hardened deterministic validation with golden dataset verification and generated validation summaries.
- Added hosted CI closeout config and workflow behavior for portable execution on GitHub-hosted runners.
- Documented operational runbooks for dataset management, converter handling, triage, escalation, and release handoff.

### CI and config portability

- Switched production converter configuration to `CRD_CONVERTER_COMMAND` environment variable usage.
- Added `config/pipeline.ci.yaml` for deterministic repository-scoped CI execution.
- Updated CI exit policy to treat pipeline exit `0` and `2` as non-fatal and `3` as fatal.
