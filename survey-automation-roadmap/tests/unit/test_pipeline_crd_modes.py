from copy import deepcopy
from pathlib import Path

import survey_automation.pipeline as pipeline_module
from survey_automation.config import DEFAULT_CONFIG
from survey_automation.pipeline import run_pipeline


def _write_point_csv(path: Path) -> None:
    path.write_text(
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,1000,2000,300,CP TEST,CP TEST,PNTS,No,,Control,\n",
        encoding="utf-8",
    )


def _write_field_code_csv(path: Path) -> None:
    path.write_text(
        "Field Code,Layer,Symbol,Linework\n"
        "CP,PNTS,DOT1,YES\n",
        encoding="utf-8",
    )


def _write_text_points(path: Path) -> None:
    path.write_text(
        "# point_id northing easting elevation description\n"
        "7001 1100.0 2100.0 310.0 CP TEXT_POINT\n",
        encoding="utf-8",
    )


def test_crd_auto_mode_quarantines_binary_without_converter(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = None

    result = run_pipeline(input_dir=input_dir, output_dir=tmp_path / "out", config=config, run_id="auto-mode")
    assert result.exit_code == 3
    assert result.summary.files_quarantined == 1


def test_crd_converter_required_without_converter_is_fatal(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "converter_required"
    config["crd"]["converter_command"] = None

    result = run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "out-required",
        config=config,
        run_id="required-mode",
    )
    assert result.exit_code == 3
    assert any(f.code == "processing_error" for f in result.findings)


def test_crd_auto_mode_with_working_converter_parses_points(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")
    _write_field_code_csv(input_dir / "field_codes.csv")

    converter = tmp_path / "converter.sh"
    converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "out=\"$2\"\n"
        "cat > \"$out\" <<'CSV'\n"
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "9001,1,2,3,CP GENERATED,CP GENERATED,PNTS,No,,Converted,\n"
        "CSV\n",
        encoding="utf-8",
    )
    converter.chmod(0o755)

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = f"{converter} {{input}} {{output}}"

    result = run_pipeline(input_dir=input_dir, output_dir=tmp_path / "out", config=config, run_id="auto-convert")

    assert result.summary.files_processed >= 1
    assert not any(f.code == "binary_crd_converter_missing" for f in result.findings)
    points_csv = tmp_path / "out/auto-convert/normalized/points.csv"
    assert points_csv.exists()
    assert "9001" in points_csv.read_text(encoding="utf-8")


def test_text_points_extension_is_processed_without_converter(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_text_points(input_dir / "points.pts")
    _write_field_code_csv(input_dir / "field_codes.csv")

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = None

    result = run_pipeline(input_dir=input_dir, output_dir=tmp_path / "out", config=config, run_id="text-points")

    assert result.exit_code == 0
    assert result.summary.files_processed == 2
    points_csv = tmp_path / "out/text-points/normalized/points.csv"
    assert points_csv.exists()
    assert "7001" in points_csv.read_text(encoding="utf-8")


def test_crd_auto_mode_with_broken_converter_quarantines_and_continues(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")
    _write_point_csv(input_dir / "points.csv")

    broken_converter = tmp_path / "broken.sh"
    broken_converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "echo converter failed >&2\n"
        "exit 7\n",
        encoding="utf-8",
    )
    broken_converter.chmod(0o755)

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = f"{broken_converter} {{input}} {{output}}"

    result = run_pipeline(input_dir=input_dir, output_dir=tmp_path / "out", config=config, run_id="auto-broken")

    assert result.exit_code == 2
    assert result.summary.files_processed >= 1
    assert any(f.code == "binary_crd_converter_failed" for f in result.findings)


def test_crd_auto_mode_with_unsupported_converted_output_quarantines(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")
    _write_point_csv(input_dir / "points.csv")

    converter = tmp_path / "converter.sh"
    converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "out=\"$2\"\n"
        "echo \"not,a,supported,schema\" > \"$out\"\n",
        encoding="utf-8",
    )
    converter.chmod(0o755)

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = f"{converter} {{input}} {{output}}"

    result = run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "out-unsupported-converted",
        config=config,
        run_id="auto-unsupported-converted",
    )

    assert result.exit_code == 2
    assert any(f.code == "binary_crd_converted_output_unsupported" for f in result.findings)


def test_crd_converter_required_with_unsupported_converted_output_is_fatal(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    converter = tmp_path / "converter.sh"
    converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "out=\"$2\"\n"
        "echo \"not,a,supported,schema\" > \"$out\"\n",
        encoding="utf-8",
    )
    converter.chmod(0o755)

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "converter_required"
    config["crd"]["converter_command"] = f"{converter} {{input}} {{output}}"

    result = run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "out-required-unsupported-converted",
        config=config,
        run_id="required-unsupported-converted",
    )

    assert result.exit_code == 3
    assert any(f.code == "processing_error" for f in result.findings)


def test_parquet_failure_does_not_create_placeholder_file(tmp_path, monkeypatch) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_point_csv(input_dir / "points.csv")

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["outputs"]["formats"] = ["csv", "parquet"]

    def _fake_parquet_writer(path: Path, points):
        return False, "forced parquet failure"

    monkeypatch.setattr(pipeline_module, "_write_points_parquet", _fake_parquet_writer)
    result = run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "out-parquet-failure",
        config=config,
        run_id="parquet-failure",
    )

    assert any(f.code == "parquet_write_failed" for f in result.findings)
    parquet_path = tmp_path / "out-parquet-failure/parquet-failure/normalized/points.parquet"
    assert not parquet_path.exists()


def test_crd_converter_required_with_broken_converter_is_fatal(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    broken_converter = tmp_path / "broken.sh"
    broken_converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "echo converter failed >&2\n"
        "exit 7\n",
        encoding="utf-8",
    )
    broken_converter.chmod(0o755)

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "converter_required"
    config["crd"]["converter_command"] = f"{broken_converter} {{input}} {{output}}"

    result = run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "out-required-broken",
        config=config,
        run_id="required-broken",
    )

    assert result.exit_code == 3
    assert any(f.code == "processing_error" for f in result.findings)


def test_crd_converter_required_with_quarantine_mode_downgrades_converter_failure(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")
    _write_point_csv(input_dir / "points.csv")

    broken_converter = tmp_path / "broken.sh"
    broken_converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "echo converter failed >&2\n"
        "exit 7\n",
        encoding="utf-8",
    )
    broken_converter.chmod(0o755)

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "converter_required"
    config["crd"]["converter_command"] = f"{broken_converter} {{input}} {{output}}"
    config["crd"]["converter_failure_mode"] = "quarantine"

    result = run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "out-required-quarantine",
        config=config,
        run_id="required-quarantine",
    )

    assert result.exit_code == 2
    assert any(f.code == "binary_crd_converter_failed" for f in result.findings)
    assert not any(f.code == "processing_error" for f in result.findings)


def test_crd_converter_command_can_expand_environment_template(tmp_path, monkeypatch) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")
    _write_field_code_csv(input_dir / "field_codes.csv")

    converter = tmp_path / "converter.sh"
    converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "out=\"$2\"\n"
        "cat > \"$out\" <<'CSV'\n"
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "9001,1,2,3,CP GENERATED,CP GENERATED,PNTS,No,,Converted,\n"
        "CSV\n",
        encoding="utf-8",
    )
    converter.chmod(0o755)
    monkeypatch.setenv("CRD_CONVERTER_COMMAND", f"{converter} {{input}} {{output}}")

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = "${CRD_CONVERTER_COMMAND}"

    result = run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "out",
        config=config,
        run_id="env-template",
    )

    assert result.exit_code == 0
    assert not any(f.code == "binary_crd_converter_missing" for f in result.findings)
    points_csv = tmp_path / "out/env-template/normalized/points.csv"
    assert points_csv.exists()
    assert "9001" in points_csv.read_text(encoding="utf-8")
