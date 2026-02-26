from pathlib import Path

from laser_suite.cli import main


def test_fail_case_rpp() -> None:
    root = Path(__file__).resolve().parents[3]
    bundle = root / "samples/fail_case_rpp"
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
        "integration-fail-rpp",
    ])
    assert code == 2
