#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from survey_automation.config import load_config  # noqa: E402
from survey_automation.pipeline import run_pipeline  # noqa: E402

GOLDEN_ROOT = REPO_ROOT / "validation" / "golden"
RUN_ROOT = Path(os.environ.get("GOLDEN_RUN_ROOT", REPO_ROOT / "validation" / "runs"))
RESULT_JSON = Path(
    os.environ.get("GOLDEN_RESULT_JSON", REPO_ROOT / "validation" / "golden_results.json")
)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_bool(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def main() -> int:
    RUN_ROOT.mkdir(parents=True, exist_ok=True)

    projects = sorted(p for p in GOLDEN_ROOT.iterdir() if p.is_dir())
    if not projects:
        print("No golden projects found", file=sys.stderr)
        return 2

    summary_rows: list[dict] = []
    failures: list[str] = []

    for project in projects:
        config_path = project / "config.yaml"
        checkpoint_path = project / "checkpoint.json"
        if not config_path.exists() or not checkpoint_path.exists():
            failures.append(f"{project.name}: missing config.yaml or checkpoint.json")
            continue

        config = load_config(config_path)
        checkpoint = _load_json(checkpoint_path)
        run_id = f"golden-{project.name}"

        result = run_pipeline(
            input_dir=REPO_ROOT,
            output_dir=RUN_ROOT,
            config=config,
            run_id=run_id,
        )

        run_dir = RUN_ROOT / run_id
        qc_summary = _load_json(run_dir / "reports" / "qc_summary.json")
        quarantined_files = _load_json(run_dir / "quarantine" / "quarantined_files.json")

        finding_codes = {finding.code for finding in result.findings}
        quarantine_reasons = {item["reason"] for item in quarantined_files["files"]}

        checks = {
            "exit_code": result.exit_code == checkpoint["expected_exit_code"],
            "point_count": qc_summary["point_count"] == checkpoint["expected_point_count"],
            "field_code_rule_count": qc_summary["field_code_rule_count"] == checkpoint["expected_field_code_rule_count"],
            "dxf_entity_count": qc_summary["dxf_entity_count"] == checkpoint["expected_dxf_entity_count"],
            "quarantined_row_count": qc_summary["quarantined_row_count"] == checkpoint["expected_quarantined_row_count"],
            "quarantined_file_reasons": checkpoint["expected_quarantined_file_reasons"] == sorted(quarantine_reasons),
        }

        required_codes = set(checkpoint.get("required_finding_codes", []))
        forbidden_codes = set(checkpoint.get("forbidden_finding_codes", []))

        checks["required_finding_codes"] = required_codes.issubset(finding_codes)
        checks["forbidden_finding_codes"] = finding_codes.isdisjoint(forbidden_codes)

        project_ok = all(checks.values())
        if not project_ok:
            failed_checks = [name for name, ok in checks.items() if not ok]
            failures.append(f"{project.name}: {', '.join(failed_checks)}")

        summary_rows.append(
            {
                "project": project.name,
                "status": _fmt_bool(project_ok),
                "exit_code": result.exit_code,
                "point_count": qc_summary["point_count"],
                "field_code_rule_count": qc_summary["field_code_rule_count"],
                "dxf_entity_count": qc_summary["dxf_entity_count"],
                "quarantined_row_count": qc_summary["quarantined_row_count"],
                "quarantine_reasons": sorted(quarantine_reasons),
                "finding_codes": sorted(finding_codes),
                "checks": checks,
                "run_dir": str(run_dir),
            }
        )

    payload = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "overall_status": "PASS" if not failures else "FAIL",
        "projects": summary_rows,
        "failures": failures,
    }
    RESULT_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESULT_JSON.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
