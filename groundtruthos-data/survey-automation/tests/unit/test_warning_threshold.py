import json
from copy import deepcopy

from survey_automation.config import DEFAULT_CONFIG
from survey_automation.pipeline import run_pipeline


def _write_point_csv(path) -> None:
    path.write_text(
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,1000,2000,300,CP TEST,CP TEST,PNTS,No,,Control,\n",
        encoding="utf-8",
    )


def _run_with_warnings(tmp_path, max_warning_count):
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_point_csv(input_dir / "points.csv")
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = None
    config["validation"]["max_warning_count"] = max_warning_count

    result = run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "out",
        config=config,
        run_id="threshold-run",
    )

    manifest = json.loads((tmp_path / "out/threshold-run/manifest/run_manifest.json").read_text(encoding="utf-8"))
    return result, manifest


def test_warning_threshold_unset_keeps_behavior(tmp_path) -> None:
    result, manifest = _run_with_warnings(tmp_path, None)
    assert result.exit_code == 2
    assert manifest["data"]["warning_threshold"] is None
    assert manifest["data"]["warning_threshold_exceeded"] is False


def test_warning_threshold_zero_marks_exceeded(tmp_path) -> None:
    result, manifest = _run_with_warnings(tmp_path, 0)
    assert result.exit_code == 2
    assert manifest["data"]["warning_threshold"] == 0
    assert manifest["data"]["warning_threshold_exceeded"] is True


def test_warning_threshold_high_not_exceeded(tmp_path) -> None:
    result, manifest = _run_with_warnings(tmp_path, 100)
    assert result.exit_code == 2
    assert manifest["data"]["warning_threshold"] == 100
    assert manifest["data"]["warning_threshold_exceeded"] is False
