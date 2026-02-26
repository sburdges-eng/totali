from pathlib import Path

from laser_suite.cli import main


def test_export_civil3d_payload() -> None:
    root = Path(__file__).resolve().parents[3]
    bundle = root / "samples/pass_case"
    config = root / "config/pipeline.example.yaml"
    out = root / "artifacts-integration"

    run_code = main([
        "run",
        "--bundle-dir",
        str(bundle),
        "--config",
        str(config),
        "--out",
        str(out),
        "--run-id",
        "integration-export",
    ])
    assert run_code == 0

    export_code = main([
        "export-civil3d",
        "--run-root",
        str(out / "integration-export"),
    ])
    assert export_code == 0
    assert (out / "integration-export/civil3d/civil3d_payload.json").exists()
