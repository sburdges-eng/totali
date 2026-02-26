import json
from pathlib import Path

from survey_automation.cli import main


def _write_config(path: Path, converter_command: str) -> None:
    path.write_text(
        "version: '1'\n"
        "crd:\n"
        "  mode: 'converter_required'\n"
        f"  converter_command: '{converter_command}'\n",
        encoding="utf-8",
    )


def _find_check(payload: dict, name: str) -> dict:
    return next(check for check in payload["checks"] if check["name"] == name)


def test_check_converter_static_pass(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path, "${CRD_CONVERTER_COMMAND}")
    monkeypatch.setenv("CRD_CONVERTER_COMMAND", "/bin/echo {input} {output}")

    exit_code = main(["check-converter", "--config", str(config_path)])
    assert exit_code == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["crd_mode"] == "converter_required"
    assert _find_check(payload, "unresolved_env_vars")["ok"] is True


def test_check_converter_static_fail_for_missing_env_var(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path, "${CRD_CONVERTER_COMMAND}")
    monkeypatch.delenv("CRD_CONVERTER_COMMAND", raising=False)

    exit_code = main(["check-converter", "--config", str(config_path)])
    assert exit_code == 3

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert _find_check(payload, "unresolved_env_vars")["ok"] is False


def test_check_converter_smoke_pass(tmp_path: Path, monkeypatch, capsys) -> None:
    converter = tmp_path / "converter.sh"
    converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "out=\"$2\"\n"
        "cat > \"$out\" <<'CSV'\n"
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1001,1,2,3,CP,CP,PNTS,No,,Converted,\n"
        "CSV\n",
        encoding="utf-8",
    )
    converter.chmod(0o755)

    sample_crd = tmp_path / "sample.crd"
    sample_crd.write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    config_path = tmp_path / "config.yaml"
    _write_config(config_path, "${CRD_CONVERTER_COMMAND}")
    monkeypatch.setenv("CRD_CONVERTER_COMMAND", f"{converter} {{input}} {{output}}")

    exit_code = main(
        [
            "check-converter",
            "--config",
            str(config_path),
            "--sample-crd",
            str(sample_crd),
        ]
    )
    assert exit_code == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert _find_check(payload, "smoke_conversion")["ok"] is True


def test_check_converter_smoke_fail_when_converter_writes_no_output(tmp_path: Path, monkeypatch, capsys) -> None:
    converter = tmp_path / "converter.sh"
    converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "exit 0\n",
        encoding="utf-8",
    )
    converter.chmod(0o755)

    sample_crd = tmp_path / "sample.crd"
    sample_crd.write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    config_path = tmp_path / "config.yaml"
    _write_config(config_path, "${CRD_CONVERTER_COMMAND}")
    monkeypatch.setenv("CRD_CONVERTER_COMMAND", f"{converter} {{input}} {{output}}")

    exit_code = main(
        [
            "check-converter",
            "--config",
            str(config_path),
            "--sample-crd",
            str(sample_crd),
        ]
    )
    assert exit_code == 3

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    smoke_check = _find_check(payload, "smoke_conversion")
    assert smoke_check["ok"] is False
    assert "not produced" in smoke_check["message"]


def test_check_converter_smoke_fail_for_invalid_header(tmp_path: Path, monkeypatch, capsys) -> None:
    converter = tmp_path / "converter.sh"
    converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "out=\"$2\"\n"
        "cat > \"$out\" <<'CSV'\n"
        "Field Code,Layer,Symbol,Linework\n"
        "CP,PNTS,DOT1,YES\n"
        "CSV\n",
        encoding="utf-8",
    )
    converter.chmod(0o755)

    sample_crd = tmp_path / "sample.crd"
    sample_crd.write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    config_path = tmp_path / "config.yaml"
    _write_config(config_path, "${CRD_CONVERTER_COMMAND}")
    monkeypatch.setenv("CRD_CONVERTER_COMMAND", f"{converter} {{input}} {{output}}")

    exit_code = main(
        [
            "check-converter",
            "--config",
            str(config_path),
            "--sample-crd",
            str(sample_crd),
        ]
    )
    assert exit_code == 3

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    smoke_check = _find_check(payload, "smoke_conversion")
    assert smoke_check["ok"] is False
    assert "supported point CSV header" in smoke_check["message"]
