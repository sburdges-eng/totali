import json
import os
import subprocess
import sys
from pathlib import Path


def test_golden_verification_script(repo_root: Path, tmp_path: Path) -> None:
    env = os.environ.copy()
    env["GOLDEN_RUN_ROOT"] = str(tmp_path / "runs")
    env["GOLDEN_RESULT_JSON"] = str(tmp_path / "golden_results.json")
    env["GOLDEN_RESULT_MD"] = str(tmp_path / "last_validation.md")

    verify = subprocess.run(
        [sys.executable, str(repo_root / "validation/verify_golden.py")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert verify.returncode == 0, verify.stdout + verify.stderr

    write_md = subprocess.run(
        [sys.executable, str(repo_root / "validation/write_last_validation.py")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert write_md.returncode == 0, write_md.stdout + write_md.stderr

    result_json = Path(env["GOLDEN_RESULT_JSON"])
    payload = json.loads(result_json.read_text(encoding="utf-8"))
    assert payload["overall_status"] == "PASS"
    assert len(payload["projects"]) == 3

    result_md = Path(env["GOLDEN_RESULT_MD"])
    assert result_md.exists()
    content = result_md.read_text(encoding="utf-8")
    assert "# Last Golden Validation" in content
