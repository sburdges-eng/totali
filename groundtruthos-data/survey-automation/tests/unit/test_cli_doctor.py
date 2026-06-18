import json
from pathlib import Path

from survey_automation.cli import main


def _write_config(path: Path) -> None:
    path.write_text(
        "version: '1'\n"
        "project:\n"
        "  qc_profile: strict\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*.csv'\n"
        "  exclude_globs: []\n"
        "crd:\n"
        "  mode: auto\n"
        "  converter_command: '${CRD_CONVERTER_COMMAND}'\n",
        encoding="utf-8",
    )


def test_doctor_command_reports_ok_for_healthy_setup(tmp_path: Path, monkeypatch, capsys) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "points.csv").write_text(
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,1,2,3,CP,CP,PNTS,No,,Control,\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    monkeypatch.setenv("CRD_CONVERTER_COMMAND", "/bin/echo {input} {output}")

    exit_code = main(
        [
            "doctor",
            "--config",
            str(config_path),
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(tmp_path / "out"),
        ]
    )
    assert exit_code == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert any(check["name"] == "input_discovery_non_empty" and check["ok"] for check in payload["checks"])
    assert "presentation" in payload
    category_colors = payload["presentation"]["category_colors"]
    assert category_colors["config"] == "#0072B2"
    for check in payload["checks"]:
        assert check["color"] == category_colors[check["category"]]


def test_doctor_command_fails_when_config_is_missing(tmp_path: Path, capsys) -> None:
    missing_config = tmp_path / "missing.yaml"
    exit_code = main(["doctor", "--config", str(missing_config), "--input-dir", str(tmp_path)])
    assert exit_code == 3

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert any(check["name"] == "config_exists" and not check["ok"] for check in payload["checks"])
    category_colors = payload["presentation"]["category_colors"]
    for check in payload["checks"]:
        assert check["color"] == category_colors[check["category"]]
