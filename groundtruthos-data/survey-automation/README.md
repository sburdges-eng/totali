# survey-automation-roadmap

Local Python CLI pipeline for mixed survey inputs (`.csv`, ASCII `.dxf`, `.crd`, text points `.txt`/`.pts`/`.asc`) with warn-and-quarantine behavior.

## Install

```bash
pip install -e .
```

## Large dataset storage (external drive)

Use the helper script to keep large datasets on external storage and optionally mirror locally:

```bash
scripts/dataset_mirror.sh bootstrap-external
scripts/dataset_mirror.sh sync-external-to-local
scripts/dataset_mirror.sh switch-to-local
scripts/dataset_mirror.sh switch-to-external
scripts/dataset_mirror.sh status
```

Defaults:

- External root: `/Volumes/KmiDi-external/survey-automation-roadmap/datasets`
- Local mirror root: `.local-datasets/`

You can override with environment variables:

- `EXTERNAL_DATA_ROOT=/path/to/external/root`
- `LOCAL_MIRROR_ROOT=/path/to/local/mirror`

## Command matrix

| Command | Purpose | Notes |
|---|---|---|
| `survey-automation validate --input-dir <dir> --config <yaml>` | Validate config and report discovered file types | Returns `3` for config errors |
| `survey-automation check-converter --config <yaml> [--sample-crd <path>]` | Preflight CRD converter readiness for production | Returns `3` when converter is not ready |
| `survey-automation profile --input-dir <dir> --output <json>` | Profile files using default config | Backward-compatible behavior |
| `survey-automation profile --input-dir <dir> --output <json> --quiet` | Profile files and write only to output file | Suppresses stdout JSON |
| `survey-automation profile --input-dir <dir> --output <json> --config <yaml>` | Profile files using configured include/exclude globs | Uses config discovery scope |
| `survey-automation run --input-dir <dir> --config <yaml> --output-dir <dir> [--run-id <id>]` | Run full normalization/QC pipeline | Writes artifacts to `artifacts/<run-id>/` |

If you prefer module execution during development:

```bash
PYTHONPATH=src python3 -m survey_automation.cli validate --input-dir . --config config/pipeline.example.yaml
PYTHONPATH=src python3 -m survey_automation.cli check-converter --config config/pipeline.prod.yaml
PYTHONPATH=src python3 -m survey_automation.cli run --input-dir . --config config/pipeline.example.yaml --output-dir artifacts --run-id sample-run
```

## Exit codes

- `0`: completed with no warnings/quarantines.
- `2`: completed with warnings and/or quarantine outputs.
- `3`: fatal error (for example config failure, `all_files_invalid`, or converter-required hard failure).

## Artifact schema locations

Run output root: `artifacts/<run-id>/`

- Normalized datasets:
  - `artifacts/<run-id>/normalized/points.csv`
  - `artifacts/<run-id>/normalized/points.parquet`
  - `artifacts/<run-id>/normalized/field_code_rules.csv`
  - `artifacts/<run-id>/normalized/dxf_entities.csv`
- QC reports:
  - `artifacts/<run-id>/reports/qc_findings.jsonl`
  - `artifacts/<run-id>/reports/qc_summary.json`
- Quarantine:
  - `artifacts/<run-id>/quarantine/quarantined_rows.csv`
  - `artifacts/<run-id>/quarantine/quarantined_files.json`
- Manifest:
  - `artifacts/<run-id>/manifest/run_manifest.json`

`run_manifest.json` includes `tool_version`, `warning_threshold`, and `warning_threshold_exceeded`.

## Config templates and examples

- Main config template: `config/pipeline.example.yaml`
- Hosted CI config: `config/pipeline.ci.yaml`
- Local sample config: `samples/pipeline.sample.yaml`
- Production config: `config/pipeline.prod.yaml`

## Production handoff runbook

1. Set `CRD_CONVERTER_COMMAND` in your environment to a real converter command template.

Default repo converter command:

```bash
export CRD_CONVERTER_COMMAND="$(pwd)/scripts/converter --input {input} --output {output}"
```

`config/pipeline.prod.yaml` references this value with:

```yaml
crd:
  mode: "converter_required"
  converter_command: "${CRD_CONVERTER_COMMAND}"
```

`converter_command` environment variables are expanded at command execution time (check and run), not during `load_config`.

Production warning-noise controls are configurable under `validation`:

- `duplicate_point_id_mode`: `all_occurrences` | `per_point_id` | `within_file`
- `unmapped_description_skip_categories`: category names to skip for unmapped description-code checks (for example `Converted`)

The `scripts/converter` tool parses Carlson `New CRD Format2` binary records and text CRD rows into point-style CSV.
Production config discovery includes supported survey extensions under `TOTaLi/` and `.local-datasets/TOTaLi/` and deduplicates files that resolve to the same real path.

If you use a different converter with positional arguments:

```bash
export CRD_CONVERTER_COMMAND="/absolute/path/to/crd-converter {input} {output}"
```

Keep both placeholders exactly as `{input}` and `{output}`.

2. Validate production config:

```bash
survey-automation validate --input-dir . --config config/pipeline.prod.yaml
```

3. Preflight converter readiness:

```bash
survey-automation check-converter --config config/pipeline.prod.yaml
```

Optional smoke test with a known binary CRD sample:

```bash
survey-automation check-converter --config config/pipeline.prod.yaml --sample-crd /abs/path/to/sample.crd
```

4. Run deterministic production batch:

```bash
RUN_ID="prod-$(date -u +%Y%m%dT%H%M%SZ)"
survey-automation run --input-dir . --config config/pipeline.prod.yaml --output-dir artifacts --run-id "$RUN_ID"
```

5. Run golden regression verification:

```bash
python validation/verify_golden.py
python validation/write_last_validation.py
```

6. Review:
- `artifacts/<run-id>/reports/qc_summary.json`
- `artifacts/<run-id>/manifest/run_manifest.json`
- `validation/last_validation.md`

7. Run release-candidate gate checklist command:

```bash
scripts/v2_release_candidate_gate.sh
```

8. Release tag after successful validation:

```bash
git tag v2.0.0
git push origin v2.0.0
```

## Hosted CI behavior

- GitHub-hosted scheduled/manual validation uses `config/pipeline.ci.yaml` with `samples/input/**/*`.
- CI sets `CRD_CONVERTER_COMMAND` to `${{ github.workspace }}/scripts/converter --input {input} --output {output}`.
- Pipeline exit code policy in CI: `0` and `2` are pass; `3` is fail.
- Golden verification runs in the same CI job and artifacts are uploaded even when earlier steps fail.

## Operations docs

- CRD converter contract: `docs/crd-converter.md`
- Operator runbook: `docs/operations.md`
- Project PT II roadmap: `docs/roadmap-pt2.md`
- Release candidate checklist: `docs/release-candidate-checklist.md`
- Release notes draft: `docs/release-notes-v2.0.0.md`
