# DWG Conversion Backends

## Table of Contents

1. Placeholder contract
2. Recommended backend strategy
3. Example command templates
4. Validation checklist

## 1. Placeholder Contract

The parser accepts a converter command template via `--converter-cmd` (or `DWG_TO_DXF_CMD`).
Use these placeholders:

- `{input}`: absolute path to the source `.dwg`
- `{output}`: absolute path where the parser expects a `.dxf`
- `{output_dir}`: directory containing `{output}`
- `{output_stem}`: filename stem for `{output}` (without extension)

The template must include both `{input}` and `{output}`.

## 2. Recommended Backend Strategy

1. Prefer the DWG converter already used in your environment.
2. Verify the converter command independently before plugging it into the parser.
3. Keep one stable template in `DWG_TO_DXF_CMD` to avoid per-run drift.

## 3. Example Command Templates

Use these as starting points and adjust to your local converter's real CLI:

- Wrapper-style tools (single input/output file):
  `odafc "{input}" "{output}"`
- Directory-style tools:
  `ODAFileConverter "{input}" "{output_dir}" ACAD2018 DXF 0 1 "{output_stem}.dxf"`

If your converter writes a different output filename, update the command so `{output}` is produced exactly.

## 4. Validation Checklist

Before relying on parser output:

1. Confirm converter exits with code `0`.
2. Confirm output file exists at the exact `{output}` path.
3. Run parser on a known small drawing and check:
   - `summary.entity_total > 0`
   - expected layer names appear
   - expected dominant entity types appear (e.g. `LINE`, `LWPOLYLINE`, `TEXT`)
