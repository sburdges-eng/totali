from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from .arbitrator import arbitrate_run
from .bridge import BridgeConfigError, run_bridge
from .config import DEFAULT_CONFIG, ConfigError, load_config
from .converter import ConverterCheck, run_converter_smoke_check, run_static_converter_checks
from .detection import discover_files
from .json_contract import to_stable_relative_path
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

    bridge_parser = subparsers.add_parser(
        "bridge",
        help="Run deterministic rule-based intent/geometry bridge on an existing run root",
    )
    bridge_parser.add_argument(
        "--run-root",
        required=True,
        help="Path to artifacts/<run-id> containing normalized/points.csv and manifest/dataset_snapshot.json",
    )
    bridge_parser.add_argument(
        "--rules",
        required=True,
        help="YAML rule-pack path for deterministic point-to-feature mapping",
    )
    bridge_parser.add_argument(
        "--output-manifest-dir",
        required=False,
        help="Optional output directory for bridge artifacts (defaults to <run-root>/manifest)",
    )

    arbitrate_parser = subparsers.add_parser(
        "arbitrate",
        help="Run strict final validation across canonical artifacts and fail on any violation",
    )
    arbitrate_parser.add_argument(
        "--run-root",
        required=True,
        help="Path to artifacts/<run-id> root",
    )
    arbitrate_parser.add_argument(
        "--eval-report",
        required=True,
        help="Path to eval_gate_report.json",
    )
    arbitrate_parser.add_argument(
        "--require-bridge",
        required=False,
        default="true",
        help="Require bridge artifacts (true|false). Default: true",
    )

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

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Run environment/config/input diagnostics and print actionable fixes",
    )
    doctor_parser.add_argument(
        "--config",
        required=False,
        help="Optional YAML config path (defaults to config/pipeline.prod.yaml when present)",
    )
    doctor_parser.add_argument(
        "--input-dir",
        required=False,
        default=".",
        help="Input directory root used for discovery checks",
    )
    doctor_parser.add_argument(
        "--output-dir",
        required=False,
        default="artifacts",
        help="Output directory used for writeability checks",
    )
    doctor_parser.add_argument(
        "--sample-crd",
        required=False,
        help="Optional sample binary CRD path for converter smoke testing",
    )

    return parser


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _build_doctor_presentation(config: dict[str, object]) -> dict[str, object]:
    default_presentation = DEFAULT_CONFIG["presentation"]
    presentation_cfg = config.get("presentation", default_presentation)
    if not isinstance(presentation_cfg, dict):
        presentation_cfg = default_presentation

    category_colors = presentation_cfg.get("category_colors", default_presentation["category_colors"])
    if not isinstance(category_colors, dict):
        category_colors = default_presentation["category_colors"]

    config_colors = presentation_cfg.get("config_colors", default_presentation["config_colors"])
    if not isinstance(config_colors, dict):
        config_colors = default_presentation["config_colors"]

    qc_profile_colors = config_colors.get("qc_profile", default_presentation["config_colors"]["qc_profile"])
    if not isinstance(qc_profile_colors, dict):
        qc_profile_colors = default_presentation["config_colors"]["qc_profile"]

    crd_mode_colors = config_colors.get("crd_mode", default_presentation["config_colors"]["crd_mode"])
    if not isinstance(crd_mode_colors, dict):
        crd_mode_colors = default_presentation["config_colors"]["crd_mode"]

    return {
        "enabled": bool(presentation_cfg.get("enabled", default_presentation["enabled"])),
        "color_basis": presentation_cfg.get("color_basis", default_presentation["color_basis"]),
        "category_colors": dict(category_colors),
        "config_colors": {
            "qc_profile": dict(qc_profile_colors),
            "crd_mode": dict(crd_mode_colors),
        },
    }


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
    config_path = Path(args.config).resolve()
    try:
        config = load_config(config_path)
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
            config_anchor_dir=config_path.parent,
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


def cmd_bridge(args: argparse.Namespace) -> int:
    run_root = Path(args.run_root).resolve()
    rules_path = Path(args.rules).resolve()
    output_manifest_dir = Path(args.output_manifest_dir).resolve() if args.output_manifest_dir else None

    try:
        result = run_bridge(
            run_root=run_root,
            rules_path=rules_path,
            output_manifest_dir=output_manifest_dir,
        )
    except BridgeConfigError as exc:
        print(f"Bridge failed: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:
        print(f"Bridge failed: {exc}", file=sys.stderr)
        return 3

    payload = {
        "schema_version": SCHEMA_VERSION,
        "ok": True,
        "run_id": result.run_id,
        "run_root": ".",
        "artifact_paths": {
            "intent_ir": to_stable_relative_path(result.intent_path, base=run_root),
            "geometry_ir": to_stable_relative_path(result.geometry_path, base=run_root),
            "bridge_manifest": to_stable_relative_path(result.bridge_manifest_path, base=run_root),
        },
        "counts": {
            "mapped_points": result.mapped_points,
            "unmapped_points": result.unmapped_points,
            "quarantined_points": result.quarantined_points,
            "intent_features": result.intent_features,
            "geometry_features": result.geometry_features,
        },
    }
    _print_json(payload)
    return 0


def _parse_bool_arg(value: str, *, field_name: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean value for `{field_name}`: {value}")


def cmd_arbitrate(args: argparse.Namespace) -> int:
    run_root = Path(args.run_root).resolve()
    eval_report = Path(args.eval_report).resolve()
    try:
        require_bridge = _parse_bool_arg(args.require_bridge, field_name="require_bridge")
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 3

    try:
        result = arbitrate_run(
            run_root=run_root,
            eval_report_path=eval_report,
            require_bridge=require_bridge,
        )
    except Exception as exc:
        print(f"Arbitration failed: {exc}", file=sys.stderr)
        return 3

    payload = {
        "schema_version": SCHEMA_VERSION,
        "ok": result.ok,
        "run_root": str(run_root),
        "report_path": str(result.report_path),
        "violation_count": result.report.get("metadata", {}).get("violation_count"),
    }
    _print_json(payload)
    return 0 if result.ok else 3


def _doctor_check(
    *,
    name: str,
    ok: bool,
    message: str,
    fix: str,
    category: str,
    category_colors: dict[str, str],
) -> dict[str, object]:
    return {
        "name": name,
        "ok": ok,
        "category": category,
        "color": category_colors.get(category, "#999999"),
        "message": message,
        "fix": fix,
    }


def cmd_doctor(args: argparse.Namespace) -> int:
    checks: list[dict[str, object]] = []
    config_path: Path | None = None
    config = DEFAULT_CONFIG
    presentation = _build_doctor_presentation(config)
    category_colors = presentation["category_colors"]

    if args.config:
        config_path = Path(args.config).resolve()
    else:
        default_prod_config = Path("config/pipeline.prod.yaml").resolve()
        if default_prod_config.exists():
            config_path = default_prod_config

    if config_path is not None:
        if not config_path.exists():
            checks.append(
                _doctor_check(
                    name="config_exists",
                    ok=False,
                    category="config",
                    category_colors=category_colors,
                    message=f"Config file not found: {config_path}",
                    fix="Provide `--config /abs/path/to/config.yaml` or create the missing config file.",
                )
            )
        else:
            checks.append(
                _doctor_check(
                    name="config_exists",
                    ok=True,
                    category="config",
                    category_colors=category_colors,
                    message=f"Config file found: {config_path}",
                    fix="No action required.",
                )
            )
            try:
                config = load_config(config_path)
                presentation = _build_doctor_presentation(config)
                category_colors = presentation["category_colors"]
                checks.append(
                    _doctor_check(
                        name="config_valid",
                        ok=True,
                        category="config",
                        category_colors=category_colors,
                        message=f"Config validation passed ({config_path})",
                        fix="No action required.",
                    )
                )
            except ConfigError as exc:
                checks.append(
                    _doctor_check(
                        name="config_valid",
                        ok=False,
                        category="config",
                        category_colors=category_colors,
                        message=f"Config validation failed: {exc}",
                        fix="Fix config schema errors, then rerun `survey-automation doctor --config ...`.",
                    )
                )
                payload = {
                    "schema_version": SCHEMA_VERSION,
                    "ok": False,
                    "config_path": str(config_path),
                    "presentation": presentation,
                    "checks": checks,
                }
                _print_json(payload)
                return 3

    input_dir = Path(args.input_dir).resolve()
    if not input_dir.exists() or not input_dir.is_dir():
        checks.append(
            _doctor_check(
                name="input_dir_accessible",
                ok=False,
                category="data",
                category_colors=category_colors,
                message=f"Input directory is missing or not a directory: {input_dir}",
                fix="Create/fix the input directory path, or pass `--input-dir` to the correct root.",
            )
        )
    else:
        checks.append(
            _doctor_check(
                name="input_dir_accessible",
                ok=True,
                category="data",
                category_colors=category_colors,
                message=f"Input directory is accessible: {input_dir}",
                fix="No action required.",
            )
        )
        discovered_files = discover_files(
            input_dir=input_dir,
            include_globs=config["input"]["include_globs"],
            exclude_globs=config["input"]["exclude_globs"],
        )
        checks.append(
            _doctor_check(
                name="input_discovery_non_empty",
                ok=bool(discovered_files),
                category="data",
                category_colors=category_colors,
                message=(
                    f"Discovered {len(discovered_files)} files with include/exclude globs"
                    if discovered_files
                    else "No input files discovered with current include/exclude globs"
                ),
                fix=(
                    "Adjust `input.include_globs`/`input.exclude_globs` or validate dataset mount/symlinks."
                    if not discovered_files
                    else "No action required."
                ),
            )
        )

    output_dir = Path(args.output_dir).resolve()
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix="survey-automation-doctor-", dir=output_dir, delete=True):
            pass
        checks.append(
            _doctor_check(
                name="output_dir_writable",
                ok=True,
                category="environment",
                category_colors=category_colors,
                message=f"Output directory is writable: {output_dir}",
                fix="No action required.",
            )
        )
    except Exception as exc:
        checks.append(
            _doctor_check(
                name="output_dir_writable",
                ok=False,
                category="environment",
                category_colors=category_colors,
                message=f"Output directory is not writable: {exc}",
                fix="Fix directory permissions or pass a writable `--output-dir`.",
            )
        )

    converter_command = config["crd"].get("converter_command")
    check_env = dict(os.environ)
    resolved_command, converter_checks = run_static_converter_checks(converter_command, env=check_env)
    checks.extend(
        _doctor_check(
            name=f"converter_{item.name}",
            ok=item.ok,
            category="converter",
            category_colors=category_colors,
            message=item.message,
            fix=(
                "Set `CRD_CONVERTER_COMMAND` (or `crd.converter_command`) with `{input}` and `{output}` and ensure the executable is installed."
                if not item.ok
                else "No action required."
            ),
        )
        for item in converter_checks
    )

    sample_crd: Path | None = None
    if args.sample_crd:
        sample_crd = Path(args.sample_crd).resolve()
        if all(check.ok for check in converter_checks):
            smoke = run_converter_smoke_check(resolved_command, sample_crd, env=check_env)
            checks.append(
                _doctor_check(
                    name=f"converter_{smoke.name}",
                    ok=smoke.ok,
                    category="converter",
                    category_colors=category_colors,
                    message=smoke.message,
                    fix=(
                        "Run the converter manually with the sample CRD and ensure it outputs a supported point CSV header."
                        if not smoke.ok
                        else "No action required."
                    ),
                )
            )
        else:
            checks.append(
                _doctor_check(
                    name="converter_smoke_conversion",
                    ok=False,
                    category="converter",
                    category_colors=category_colors,
                    message="Smoke conversion skipped because static converter checks failed",
                    fix="Fix static converter checks first, then rerun with `--sample-crd`.",
                )
            )

    if "parquet" in config["outputs"]["formats"]:
        try:
            import pyarrow  # noqa: F401

            checks.append(
                _doctor_check(
                    name="parquet_dependency",
                    ok=True,
                    category="environment",
                    category_colors=category_colors,
                    message="PyArrow dependency is available for parquet output",
                    fix="No action required.",
                )
            )
        except Exception:
            checks.append(
                _doctor_check(
                    name="parquet_dependency",
                    ok=False,
                    category="environment",
                    category_colors=category_colors,
                    message="PyArrow is missing; parquet output will fail",
                    fix="Install dependencies (for example `pip install -e .`) before running parquet-enabled outputs.",
                )
            )

    ok = all(bool(check["ok"]) for check in checks)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "ok": ok,
        "config_path": str(config_path) if config_path else None,
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "sample_crd": str(sample_crd) if sample_crd else None,
        "presentation": presentation,
        "checks": checks,
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
    if args.command == "bridge":
        return cmd_bridge(args)
    if args.command == "arbitrate":
        return cmd_arbitrate(args)
    if args.command == "check-converter":
        return cmd_check_converter(args)
    if args.command == "doctor":
        return cmd_doctor(args)

    parser.print_help()
    return 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
