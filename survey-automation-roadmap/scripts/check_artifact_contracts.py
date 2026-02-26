#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from survey_automation.arbitrator import arbitrate_run  # noqa: E402


def _parse_bool(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean: {value}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run strict arbitration over canonical artifacts",
    )
    parser.add_argument(
        "--run-root",
        required=True,
        help="Artifact run root (artifacts/<run-id>)",
    )
    parser.add_argument(
        "--eval-report",
        required=True,
        help="Path to eval gate report JSON",
    )
    parser.add_argument(
        "--require-bridge",
        required=False,
        default="true",
        help="Require bridge artifacts (true|false), default true",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        require_bridge = _parse_bool(args.require_bridge)
    except ValueError as exc:
        print(f"[artifact-contract] {exc}", file=sys.stderr)
        return 1

    result = arbitrate_run(
        run_root=Path(args.run_root).resolve(),
        eval_report_path=Path(args.eval_report).resolve(),
        require_bridge=require_bridge,
    )

    if result.ok:
        print(f"[artifact-contract] PASS ({result.report_path})")
        return 0

    violations = result.report.get("data", {}).get("violations", [])
    for violation in violations:
        code = violation.get("code", "unknown")
        artifact = violation.get("artifact", "unknown")
        message = violation.get("message", "")
        print(f"[artifact-contract] {artifact} {code}: {message}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
