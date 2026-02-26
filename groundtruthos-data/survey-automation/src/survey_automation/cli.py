from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .config import DEFAULT_CONFIG, ConfigError, load_config
from .converter import ConverterCheck, run_converter_smoke_check, run_static_converter_checks
from .models import SCHEMA_VERSION
from .pipeline import profile_input, run_pipeline


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="survey-automation")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the survey automation pipeline")
    run_parser.add_argument("--input-dir", required=True, help="Directory with input files")
    run_parser.add_argument("--config", required=True, help="YAML config path")
    run_parser.add_argument("--output-dir", required=True, help="Base output directory")
    run_parser.add_argument("--run-id", required=False, help="Deterministic run id")

    profile_parser = subparsers.add_parser("profile", help="Profile input files and detected types")
    profile_parser.add_argument("--input-dir", required=True, help="Directory with input files")
    profile_parser.add_argument("--output", required=True, help="JSON output path")
    profile_parser.add_argument(
        "--config",
        required=False,
        help="Optional YAML config path to apply include/exclude globs",
    )
    profile_parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress stdout JSON output and write only to --output",
    )

    validate_parser = subparsers.add_parser("validate", help="Validate config and inspect input discovery")
    validate_parser.add_argument("--input-dir", required=True, help="Directory with input files")
    validate_parser.add_argument("--config", required=True, help="YAML config path")

    check_parser = subparsers.add_parser(
        "check-converter",
        help="Validate CRD converter readiness for production execution",
    )
    check_parser.add_argument("--config", required=True, help="YAML config path")
    check_parser.add_argument(
        "--sample-crd",
        required=False,
        help="Optional sample binary CRD path for conversion smoke testing",
    )

    return parser


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def cmd_profile(args: argparse.Namespace) -> int:
    config = DEFAULT_CONFIG
    if args.config:
        try:
            config = load_config(args.config)
        except ConfigError as exc:
            print(f"Config validation failed: {exc}", file=sys.stderr)
            return 3

    input_dir = Path(args.input_dir).resolve()
    output_path = Path(args.output).resolve()
    profile = profile_input(input_dir=input_dir, config=config)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(profile, indent=2, sort_keys=True), encoding="utf-8")
    if not args.quiet:
        _print_json(profile)
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"Config validation failed: {exc}", file=sys.stderr)
        return 3

    input_dir = Path(args.input_dir).resolve()
    profile = profile_input(input_dir=input_dir, config=config)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "config_valid": True,
        "files_total": profile["files_total"],
        "files_by_type": profile["files_by_type"],
    }
    _print_json(payload)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"Config validation failed: {exc}", file=sys.stderr)
        return 3

    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    try:
        result = run_pipeline(
            input_dir=input_dir,
            output_dir=output_dir,
            config=config,
            run_id=args.run_id,
        )
    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 3

    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": result.run_id,
        "exit_code": result.exit_code,
        "run_root": str(result.run_root),
        "summary": {
            "files_total": result.summary.files_total,
            "files_processed": result.summary.files_processed,
            "files_quarantined": result.summary.files_quarantined,
            "findings_by_severity": result.summary.findings_by_severity,
        },
    }
    _print_json(payload)
    return result.exit_code


def cmd_check_converter(args: argparse.Namespace) -> int:
    config_path = Path(args.config).resolve()
    try:
        config = load_config(config_path)
    except ConfigError as exc:
        print(f"Config validation failed: {exc}", file=sys.stderr)
        return 3

    converter_command = config["crd"].get("converter_command")
    check_env = dict(os.environ)
    resolved_command, checks = run_static_converter_checks(converter_command, env=check_env)

    sample_crd: Path | None = None
    if args.sample_crd:
        sample_crd = Path(args.sample_crd).resolve()
        if all(check.ok for check in checks):
            checks.append(run_converter_smoke_check(resolved_command, sample_crd, env=check_env))
        else:
            checks.append(
                ConverterCheck(
                    name="smoke_conversion",
                    ok=False,
                    message="smoke conversion skipped because static checks failed",
                )
            )

    ok = all(check.ok for check in checks)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "ok": ok,
        "config_path": str(config_path),
        "crd_mode": config["crd"]["mode"],
        "sample_crd": str(sample_crd) if sample_crd else None,
        "checks": [check.to_dict() for check in checks],
    }
    _print_json(payload)
    return 0 if ok else 3


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "profile":
        return cmd_profile(args)
    if args.command == "validate":
        return cmd_validate(args)
    if args.command == "run":
        return cmd_run(args)
    if args.command == "check-converter":
        return cmd_check_converter(args)

    parser.print_help()
    return 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
