# Operations Runbook

## Daily usage

1. Validate config and input classification.
2. Export `CRD_CONVERTER_COMMAND` with `{input}` and `{output}` placeholders.
3. Ensure either `TOTaLi/` is available or `.local-datasets/TOTaLi/` mirror exists.
4. Run `survey-automation check-converter --config config/pipeline.prod.yaml`.
5. Run pipeline with deterministic `--run-id` when repeatability is needed.
6. Run `survey-automation doctor --config config/pipeline.prod.yaml --input-dir . --output-dir artifacts` when diagnosing environment/config drift.
7. Review `reports/qc_summary.json`, `reports/qc_trend.json`, and `manifest/dataset_snapshot.json`, then inspect quarantine artifacts.

Example:

```bash
export CRD_CONVERTER_COMMAND="$(pwd)/scripts/converter --input {input} --output {output}"
```

`crd.converter_command` environment references are resolved at command execution time (`check-converter` / `run`).

## Phase presentation metadata

Run artifacts include `phase_presentation` in:

1. `reports/qc_summary.json`
2. `manifest/run_manifest.json`

Interpretation:

1. `ground_truth`: snapshot/config context (`snapshot_id`, `qc_profile`, `crd_mode`).
2. `phase_1`: input/config readiness (`files_total`, config validity evidence).
3. `phase_2`: normalization/conversion execution (`files_processed`, `files_quarantined`).
4. `phase_3`: QC/report completion (`exit_code`, `findings_by_severity`).

Status rules:

1. `phase_1` is `pass` when `files_total > 0`, else `fail`.
2. `phase_2` is `pass` when processed files exist and quarantine is zero; `warning` when processed files exist with quarantine; `fail` when no files processed.
3. `phase_3` maps to exit code: `0 -> pass`, `2 -> warning`, `3 -> fail`.

This metadata is presentation-only and does not modify run exit semantics.

## Doctor category colors

`survey-automation doctor` emits per-check `color` values and top-level `presentation` palette metadata.
Default category colors:

1. `config`: `#0072B2`
2. `data`: `#009E73`
3. `environment`: `#D55E00`
4. `converter`: `#CC79A7`

These can be overridden via the config `presentation` block.

## Hosted CI validation (GitHub-hosted runner)

1. CI uses `config/pipeline.ci.yaml` and `samples/input/**/*` only.
2. CI sets `CRD_CONVERTER_COMMAND="${GITHUB_WORKSPACE}/scripts/converter --input {input} --output {output}"`.
3. CI runs:
- `survey-automation check-converter --config config/pipeline.ci.yaml`
- `survey-automation run --input-dir . --config config/pipeline.ci.yaml --output-dir artifacts --run-id "$RUN_ID"`
- `python validation/verify_golden.py`
- `python validation/write_last_validation.py`
4. CI pass criteria for pipeline exit codes:
- `0`: pass
- `2`: pass (warning/quarantine signal)
- `3`: fail (fatal)

Production external-dataset runs remain operator-managed via `config/pipeline.prod.yaml`.

## PT II roadmap gate

Use the shared gate command for PT II milestone readiness:

```bash
scripts/pt2_quality_gate.sh
```

Automated schedule:

- `.github/workflows/pt2-roadmap-gate.yml` runs every Monday at 16:00 UTC.
- Manual dispatch is available for milestone reviews and pre-merge checks.

## v2 release-candidate gate

Use this command for pre-release candidate validation:

```bash
scripts/v2_release_candidate_gate.sh
```

What it adds on top of PT II baseline:

1. Documentation path reference enforcement (`tests/unit/test_docs_paths.py`).
2. Production config validation (`survey-automation validate --config config/pipeline.prod.yaml`).
3. Production converter readiness check (`survey-automation check-converter --config config/pipeline.prod.yaml`).
4. Production QC trend and dataset snapshot artifacts for reproducibility/regression detection.

Checklist and evidence requirements:

- `docs/release-candidate-checklist.md`
- `docs/release-notes-v2.0.0.md`

CI workflow note:

- `.github/workflows/v2-release-candidate-gate.yml` supports manual release-candidate validation and artifact capture.

## Dataset location and disk management

Keep large datasets on external storage and mirror locally as needed:

1. `scripts/dataset_mirror.sh bootstrap-external`
2. `scripts/dataset_mirror.sh sync-external-to-local`
3. `scripts/dataset_mirror.sh switch-to-local` for fast local access.
4. `scripts/dataset_mirror.sh switch-to-external` to return to external-backed symlinks.
5. `scripts/dataset_mirror.sh status` to verify active targets.

## Triage flow

1. Check `quarantine/quarantined_files.json`.
2. If reason is `unsupported_file_type`:
- Confirm extension is in current scope (`.csv`, `.dxf` ASCII, `.crd`, `.txt`/`.pts`/`.asc` text points).
- If out of scope (`.dwg`, `.pcs`, binary `.dxf`), keep quarantined.
3. If reason is `binary_crd_converter_missing` or `binary_crd_converter_failed`:
- Configure/fix `crd.converter_command`.
- Re-run with same input and a new run id.
4. Check `quarantine/quarantined_rows.csv` for malformed rows:
- `bad_column_count`: source row schema mismatch.
- `invalid_numeric`: coordinate parse failure.
- `unknown_schema`: data before recognized header.

## Warning-noise controls

Use production config controls to suppress non-actionable warning volume:

1. `project.qc_profile`:
- `strict`: conservative defaults and no automatic remediation.
- `standard`: production defaults with known noisy exclusions and remediation enabled.
- `legacy`: compatibility-biased defaults for noisier historical datasets.
2. `project.baseline_namespace`:
- Use a unique namespace per project so trend baselines do not bleed across unrelated runs.
3. `validation.duplicate_point_id_mode`:
- `all_occurrences`: one finding per duplicate row.
- `per_point_id`: one finding per duplicate point id across all files.
- `within_file`: one finding per duplicate point id within each source file.
4. `validation.unmapped_description_skip_categories`:
- Skip unmapped description-code findings for configured point categories (for example `Converted` from CRD conversion outputs).
5. `input.include_globs`:
- Restrict to supported extensions (`.csv`, `.dxf`, `.crd`, `.txt`, `.pts`, `.asc`) to avoid unsupported-extension quarantine noise.
6. `validation.trend_tracking`:
- Compare warning/error/critical deltas against the last good run baseline.
- Fail with `qc_regression_spike` when configured spike thresholds are exceeded.
7. `remediation`:
- Optional auto-remediation for blank field codes, duplicate tail blocks, and malformed footer rows.

## Binary CRD handling

- Preferred mode: `auto` with a working converter.
- Strict mode: `converter_required` for controlled production runs.
- Temporary fallback: `text_only` to defer conversion while keeping runs non-fatal.

## Exit code interpretation

- `0`: no findings/quarantine.
- `2`: warnings and/or quarantines present.
- `3`: fatal failure (`all_files_invalid`, config error, or converter-required hard failure).

## Escalation playbook

### Level 1: warning-only degradation (run exits `2`)

Trigger:
- Warning threshold exceeded in `run_manifest.json`.
- Trend spike detected in `reports/qc_trend.json`.
- Quarantine growth but no fatal pipeline failure.

Action:
1. Open `reports/qc_summary.json` and `quarantine/*`.
2. Check `reports/qc_trend.json` deltas vs baseline run.
3. Identify top warning codes and impacted files.
4. Create an issue with run id, warning counts, trend deltas, and top reason codes.
5. Re-run after source/config corrections.

### Level 2: converter degradation

Trigger:
- `binary_crd_converter_failed` appears in findings.
- CRD conversion drift against known-good output.

Action:
1. Run `survey-automation check-converter --config config/pipeline.prod.yaml --sample-crd /abs/path/to/sample.crd`.
2. Validate converter output format against `docs/crd-converter.md`.
3. If converter dependency changed, pin/fix tool version and rerun.

### Level 3: fatal pipeline failure (run exits `3`)

Trigger:
- Config validation failure.
- `processing_error` spikes.
- `all_files_invalid` in findings.

Action:
1. Halt release/tagging.
2. Attach full artifact set for failing run.
3. Open high-priority incident ticket with:
- run id
- failing command
- `run_manifest.json`
- `qc_findings.jsonl` excerpt
4. Resume scheduled runs only after one clean golden verification.
