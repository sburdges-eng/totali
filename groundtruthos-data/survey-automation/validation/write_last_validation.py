#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULT_JSON = Path(
    os.environ.get("GOLDEN_RESULT_JSON", REPO_ROOT / "validation" / "golden_results.json")
)
OUTPUT_MD = Path(
    os.environ.get("GOLDEN_RESULT_MD", REPO_ROOT / "validation" / "last_validation.md")
)


def main() -> int:
    if not RESULT_JSON.exists():
        raise SystemExit(f"Missing result json: {RESULT_JSON}")

    payload = json.loads(RESULT_JSON.read_text(encoding="utf-8"))

    lines = [
        "# Last Golden Validation",
        "",
        f"- Generated at (UTC): `{payload['generated_at_utc']}`",
        f"- Overall status: `{payload['overall_status']}`",
        "",
        "## Project Results",
        "",
        "| Project | Status | Exit | Points | Rules | DXF Entities | Quarantine Rows |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]

    for item in payload["projects"]:
        lines.append(
            "| "
            f"{item['project']} | {item['status']} | {item['exit_code']} | {item['point_count']} | "
            f"{item['field_code_rule_count']} | {item['dxf_entity_count']} | {item['quarantined_row_count']} |"
        )

    lines.extend(["", "## Failures", ""])
    failures = payload.get("failures", [])
    if failures:
        for failure in failures:
            lines.append(f"- {failure}")
    else:
        lines.append("- None")

    OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUTPUT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
