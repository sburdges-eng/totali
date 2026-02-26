import json

from survey_automation.cli import main


def test_profile_command_without_config_remains_compatible(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "a.csv").write_text("Field Code,Layer,Symbol,Linework\nCP,PNTS,DOT1,YES\n", encoding="utf-8")
    (input_dir / "b.txt").write_text("hello\n", encoding="utf-8")
    output = tmp_path / "profile.json"

    exit_code = main(["profile", "--input-dir", str(input_dir), "--output", str(output)])
    assert exit_code == 0

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["files_total"] == 2
    assert "field_code_csv" in payload["files_by_type"]


def test_profile_command_with_config_applies_globs(tmp_path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "keep.csv").write_text("Field Code,Layer,Symbol,Linework\nCP,PNTS,DOT1,YES\n", encoding="utf-8")
    (input_dir / "ignore.txt").write_text("ignore\n", encoding="utf-8")

    config_path = tmp_path / "profile.yaml"
    config_path.write_text(
        "version: '1'\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*.csv'\n"
        "  exclude_globs: []\n"
        "validation:\n"
        "  required_point_columns:\n"
        "    - 'Point#'\n"
        "    - 'Northing'\n"
        "    - 'Easting'\n"
        "    - 'Elevation'\n"
        "    - 'Description'\n"
        "    - 'DWG Description'\n"
        "    - 'DWG Layer'\n"
        "    - 'Locked'\n"
        "    - 'Group'\n"
        "    - 'Category'\n"
        "    - 'LS Number'\n",
        encoding="utf-8",
    )

    output = tmp_path / "profile.json"
    exit_code = main(
        [
            "profile",
            "--input-dir",
            str(input_dir),
            "--output",
            str(output),
            "--config",
            str(config_path),
        ]
    )
    assert exit_code == 0

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["files_total"] == 1
    assert payload["files"][0]["file_path"].endswith("keep.csv")


def test_profile_command_quiet_suppresses_stdout(tmp_path, capsys) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "a.csv").write_text("Field Code,Layer,Symbol,Linework\nCP,PNTS,DOT1,YES\n", encoding="utf-8")
    output = tmp_path / "profile.json"

    exit_code = main(
        [
            "profile",
            "--input-dir",
            str(input_dir),
            "--output",
            str(output),
            "--quiet",
        ]
    )
    assert exit_code == 0
    assert output.exists()
    assert capsys.readouterr().out.strip() == ""
