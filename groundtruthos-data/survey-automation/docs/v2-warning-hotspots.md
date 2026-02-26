# v2.0.0 Warning Hotspot Report

Generated at: 2026-02-13
Baseline run: `v2-kickoff-fixed-20260213T074455Z`
Tuned run: `v2-noise-tuned2-20260213T084839Z`
Fixed run: `v2-noise-fixed-20260213T092320Z`

## Summary delta

1. Warnings: `3723` -> `20` -> `0`
2. Files total: `13` -> `7` -> `5`
3. Quarantined files: `8` -> `2` -> `0`
4. Errors: `0` -> `0` -> `0`

## What changed

1. Restricted `config/pipeline.prod.yaml` include scope to supported extensions only (`.csv`, `.dxf`, `.crd`, `.txt`, `.pts`, `.asc`).
2. Added `validation.unmapped_description_skip_categories` and set it to `Converted` for production runs.
3. Added `validation.duplicate_point_id_mode` and set production mode to `within_file`.
4. Removed invalid field-code rows and duplicate trailer block from `.local-datasets/TOTaLi/TEST BRIDGEY.csv`.
5. Removed blank field-code rows from `.local-datasets/TOTaLi/FGD_Template_With_Preview (4).csv`.
6. Excluded known binary DXF files (`IIII.dxf`, `XR23173-Sur.dxf`) from production discovery.

## Tuned run distribution by code (intermediate)

1. `duplicate_point_id`: `13`
2. `bad_column_count`: `3`
3. `unsupported_binary_dxf`: `2`
4. `missing_field_code`: `2`

## Final status (fixed run)

1. Production profile run exits clean (`exit_code: 0`).
2. Warning count is `0`.
3. Quarantined file count is `0`.
