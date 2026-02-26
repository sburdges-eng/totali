from pathlib import Path

from laser_suite.cli import main


def test_reproducible_manifest_for_fixed_run_id(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[3]
    bundle = root / "samples/pass_case"
    config = root / "config/pipeline.example.yaml"

    out_a = tmp_path / "a"
    out_b = tmp_path / "b"

    code_a = main(["run", "--bundle-dir", str(bundle), "--config", str(config), "--out", str(out_a), "--run-id", "same"])
    code_b = main(["run", "--bundle-dir", str(bundle), "--config", str(config), "--out", str(out_b), "--run-id", "same"])
    assert code_a == 0
    assert code_b == 0

    m_a = (out_a / "same/manifest/run_manifest.json").read_text(encoding="utf-8")
    m_b = (out_b / "same/manifest/run_manifest.json").read_text(encoding="utf-8")
    assert m_a == m_b
