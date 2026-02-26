# CRD Converter Contract

`survey-automation` supports binary `.crd` files through `crd.converter_command`.

## Command template

Set `crd.converter_command` to a shell command that includes both placeholders:

- `{input}`: absolute path to source `.crd`
- `{output}`: absolute path where converter must write converted content

Built-in repository converter:

```bash
export CRD_CONVERTER_COMMAND="$(pwd)/scripts/converter --input {input} --output {output}"
```

Production config:

```yaml
crd:
  mode: "converter_required"
  converter_command: "${CRD_CONVERTER_COMMAND}"
```

`scripts/converter` supports Carlson `New CRD Format2` binary records and text CRD rows.

Example with positional arguments:

```bash
export CRD_CONVERTER_COMMAND="/absolute/path/to/crd-converter {input} {output}"
```

`converter_command` must include both placeholders exactly as `{input}` and `{output}`.
Environment variable references inside `converter_command` are expanded when converter commands are executed, not during config load.

## Preflight check command

Use `check-converter` before production runs:

```bash
survey-automation check-converter --config config/pipeline.prod.yaml
```

Optional smoke conversion with a known sample:

```bash
survey-automation check-converter --config config/pipeline.prod.yaml --sample-crd /abs/path/to/sample.crd
```

Checks include command presence, unresolved env vars, required placeholders, example-script blocking, executable resolution, and optional smoke conversion output validation.

## Required behavior

1. Exit code `0` on success and non-zero on failure.
2. Write a file to `{output}`.
3. Produce output in one of these parseable formats:
- point-style CSV with `Point#,Northing,Easting,Elevation,...`
- field-code CSV with `Field Code,Layer,Symbol,Linework`
- text CRD style rows (`point_id northing easting elevation [description]`)

## Failure handling

- `crd.mode=auto`: failed conversion quarantines file and run continues.
- `crd.mode=converter_required`: failed conversion is fatal (exit code `3`).
- `crd.mode=text_only`: converter is not used; binary CRD is quarantined.
