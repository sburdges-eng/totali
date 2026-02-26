from pathlib import Path

from laser_suite.cli import main


def test_pass_case_sample_bundle() -> None:
    root = Path(__file__).resolve().parents[3]
    bundle = root / "samples/pass_case"
    config = root / "config/pipeline.example.yaml"
    out_dir = root / "artifacts-integration"
    code = main([
        "run",
        "--bundle-dir",
        str(bundle),
        "--config",
        str(config),
        "--out",
        str(out_dir),
        "--run-id",
        "integration-pass",
    ])
    assert code == 0
