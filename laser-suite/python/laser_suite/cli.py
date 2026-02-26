from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .adjustment import AdjustmentError, run_adjustment
from .config import ConfigError, load_config
from .contracts import build_contract_payload, write_contract_json
from .encroachment import analyze_encroachments
from .io_csv import BundleError, load_bundle
from .rpp import compute_rpp_rows
from .schemas import SCHEMA_VERSION


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _mk_run_root(out_dir: Path, run_id: str | None) -> tuple[str, Path]:
    rid = run_id or datetime.now(UTC).strftime("run-%Y%m%dT%H%M%SZ")
    root = out_dir / rid
    return rid, root


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _run_laser_core(bundle_dir: Path, config: dict[str, Any], run_root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    bundle = load_bundle(bundle_dir)
    result = run_adjustment(bundle, config)

    rpp_cfg = config["laser"]["rpp"]
    rpp_rows = compute_rpp_rows(
        bundle=bundle,
        adjusted_xy=result.adjusted_xy,
        covariance_xy_full=result.covariance_xy_full,
        k95=float(rpp_cfg["k95"]),
        allowable_base_m=float(rpp_cfg["allowable_base_m"]),
        allowable_ppm=float(rpp_cfg["allowable_ppm"]),
    )

    adjusted_rows = [
        {
            "station_id": sid,
            "x": xy[0],
            "y": xy[1],
        }
        for sid, xy in sorted(result.adjusted_xy.items())
    ]
    _write_csv(run_root / "laser/stations_adjusted.csv", ["station_id", "x", "y"], adjusted_rows)

    cov_rows = []
    cov = result.covariance_xy_full
    for i in range(cov.shape[0]):
        for j in range(cov.shape[1]):
            if abs(float(cov[i, j])) > 0.0:
                cov_rows.append({"row": i, "col": j, "value": float(cov[i, j])})
    _write_csv(run_root / "laser/covariance_blocks.csv", ["row", "col", "value"], cov_rows)

    rpp_rows_dict = [asdict(row) for row in rpp_rows]
    _write_csv(
        run_root / "laser/rpp_adjacency.csv",
        ["pair_id", "station_i", "station_j", "distance_m", "rpp_actual_m", "rpp_allowable_m", "margin_m", "compliant"],
        rpp_rows_dict,
    )

    adjustment_payload = build_contract_payload(
        artifact_type="adjustment_report",
        invariants=[
            "schema_version_required",
            "deterministic_key_order",
            "relative_paths_only",
            "pair_covariance_propagation_used",
        ],
        metadata={
            "generated_at": _now_iso(),
            "solver_path": result.solver_path,
            "iterations": result.iterations,
            "condition_number": result.condition_number,
        },
        paths={
            "stations_adjusted": "laser/stations_adjusted.csv",
            "covariance_blocks": "laser/covariance_blocks.csv",
            "rpp_adjacency": "laser/rpp_adjacency.csv",
        },
        data={
            "converged": result.converged,
            "posterior_variance_factor": result.posterior_variance_factor,
            "residual_norm": result.residual_norm,
            "rpp_fail_count": sum(1 for row in rpp_rows if not row.compliant),
        },
        schema_version=SCHEMA_VERSION,
    )
    write_contract_json(run_root / "laser/adjustment_report.json", adjustment_payload)

    return adjustment_payload, rpp_rows_dict


def _run_encroachment_core(bundle_dir: Path, config: dict[str, Any], run_root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    bundle = load_bundle(bundle_dir)
    analysis = analyze_encroachments(bundle, float(config["encroachment"]["snap_tolerance_m"]))
    rows = [asdict(row) for row in analysis["rows"]]

    _write_csv(
        run_root / "encroachment/table_a_item20.csv",
        ["item_id", "condition_type", "location_reference", "magnitude", "units", "status"],
        rows,
    )

    report_payload = build_contract_payload(
        artifact_type="encroachment_report",
        invariants=[
            "schema_version_required",
            "deterministic_key_order",
            "relative_paths_only",
            "deterministic_topology_normalization",
        ],
        metadata={"generated_at": _now_iso()},
        paths={"table_a_item20": "encroachment/table_a_item20.csv"},
        data={
            "compliant": analysis["compliant"],
            "row_count": analysis["row_count"],
            "rows": rows,
        },
        schema_version=SCHEMA_VERSION,
    )
    write_contract_json(run_root / "encroachment/encroachment_report.json", report_payload)

    return report_payload, rows


def _export_civil3d(run_root: Path, out_dir: Path | None = None) -> dict[str, Any]:
    target_dir = out_dir or (run_root / "civil3d")
    target_dir.mkdir(parents=True, exist_ok=True)

    rpp_rows = []
    with (run_root / "laser/rpp_adjacency.csv").open("r", encoding="utf-8", newline="") as handle:
        rpp_rows = list(csv.DictReader(handle))

    table_rows = []
    with (run_root / "encroachment/table_a_item20.csv").open("r", encoding="utf-8", newline="") as handle:
        table_rows = list(csv.DictReader(handle))

    payload = build_contract_payload(
        artifact_type="civil3d_payload",
        invariants=["schema_version_required", "deterministic_key_order", "relative_paths_only"],
        metadata={"generated_at": _now_iso()},
        paths={
            "source_rpp": "../laser/rpp_adjacency.csv",
            "source_item20": "../encroachment/table_a_item20.csv",
        },
        data={
            "rpp_rows": rpp_rows,
            "item20_rows": table_rows,
            "notification_required": any((str(row.get("compliant", "")).lower() == "false") for row in rpp_rows),
        },
        schema_version=SCHEMA_VERSION,
    )
    write_contract_json(target_dir / "civil3d_payload.json", payload)

    xdata_payload = build_contract_payload(
        artifact_type="dxf_xdata_payload",
        invariants=["schema_version_required", "deterministic_key_order", "relative_paths_only"],
        metadata={"generated_at": _now_iso(), "appid": "GEO_AI_ENGINE_2026"},
        paths={"source_payload": "civil3d_payload.json"},
        data={
            "xdata_fields": [
                {"code": 1001, "name": "AppId"},
                {"code": 1000, "name": "Datum"},
                {"code": 1000, "name": "Epoch_MJD"},
                {"code": 1040, "name": "RPP_Actual"},
                {"code": 1000, "name": "ALTA_2026_COMPLIANT"},
            ]
        },
        schema_version=SCHEMA_VERSION,
    )
    write_contract_json(target_dir / "dxf_xdata_payload.json", xdata_payload)

    return payload


def _run_full(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config).resolve())
    bundle_dir = Path(args.bundle_dir).resolve()
    out_dir = Path(args.out).resolve()
    run_id, run_root = _mk_run_root(out_dir, args.run_id)

    run_root.mkdir(parents=True, exist_ok=True)

    adjustment_payload, rpp_rows = _run_laser_core(bundle_dir, config, run_root)
    enc_payload, _enc_rows = _run_encroachment_core(bundle_dir, config, run_root)
    _export_civil3d(run_root)

    manifest = build_contract_payload(
        artifact_type="run_manifest",
        invariants=["schema_version_required", "deterministic_key_order", "relative_paths_only"],
        metadata={"run_id": run_id, "generated_at": _now_iso()},
        paths={
            "adjustment_report": "laser/adjustment_report.json",
            "encroachment_report": "encroachment/encroachment_report.json",
            "civil3d_payload": "civil3d/civil3d_payload.json",
        },
        data={
            "summary": {
                "rpp_fail_count": adjustment_payload["data"]["rpp_fail_count"],
                "encroachment_row_count": enc_payload["data"]["row_count"],
            },
            "rpp_all_pass": all(str(row["compliant"]).lower() == "true" for row in rpp_rows),
            "schemaVersion": SCHEMA_VERSION,
        },
        schema_version=SCHEMA_VERSION,
    )
    write_contract_json(run_root / "manifest/run_manifest.json", manifest)

    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "runId": run_id,
        "runRoot": str(run_root),
        "rppFailCount": adjustment_payload["data"]["rpp_fail_count"],
        "encroachmentRowCount": enc_payload["data"]["row_count"],
    }
    print(json.dumps(payload, indent=2))

    return 0 if adjustment_payload["data"]["rpp_fail_count"] == 0 else 2


def _run_laser(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config).resolve())
    bundle_dir = Path(args.bundle_dir).resolve()
    out_dir = Path(args.out).resolve()
    run_id, run_root = _mk_run_root(out_dir, args.run_id)
    run_root.mkdir(parents=True, exist_ok=True)

    adjustment_payload, _ = _run_laser_core(bundle_dir, config, run_root)
    print(json.dumps({"schemaVersion": SCHEMA_VERSION, "runId": run_id, "rppFailCount": adjustment_payload["data"]["rpp_fail_count"]}, indent=2))
    return 0 if adjustment_payload["data"]["rpp_fail_count"] == 0 else 2


def _run_enc(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config).resolve())
    bundle_dir = Path(args.bundle_dir).resolve()
    out_dir = Path(args.out).resolve()
    run_id, run_root = _mk_run_root(out_dir, args.run_id)
    run_root.mkdir(parents=True, exist_ok=True)

    enc_payload, _ = _run_encroachment_core(bundle_dir, config, run_root)
    print(json.dumps({"schemaVersion": SCHEMA_VERSION, "runId": run_id, "encroachmentRowCount": enc_payload["data"]["row_count"]}, indent=2))
    return 0 if enc_payload["data"]["compliant"] else 2


def _run_export(args: argparse.Namespace) -> int:
    run_root = Path(args.run_root).resolve()
    out_dir = Path(args.out).resolve() if args.out else None
    payload = _export_civil3d(run_root, out_dir)
    print(json.dumps({"schemaVersion": SCHEMA_VERSION, "artifactType": payload["artifactType"]}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="laser-suite")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run")
    p_run.add_argument("--bundle-dir", required=True)
    p_run.add_argument("--config", required=True)
    p_run.add_argument("--out", required=True)
    p_run.add_argument("--run-id")
    p_run.set_defaults(func=_run_full)

    p_laser = sub.add_parser("laser")
    p_laser.add_argument("--bundle-dir", required=True)
    p_laser.add_argument("--config", required=True)
    p_laser.add_argument("--out", required=True)
    p_laser.add_argument("--run-id")
    p_laser.set_defaults(func=_run_laser)

    p_enc = sub.add_parser("encroachment")
    p_enc.add_argument("--bundle-dir", required=True)
    p_enc.add_argument("--config", required=True)
    p_enc.add_argument("--out", required=True)
    p_enc.add_argument("--run-id")
    p_enc.set_defaults(func=_run_enc)

    p_export = sub.add_parser("export-civil3d")
    p_export.add_argument("--run-root", required=True)
    p_export.add_argument("--out")
    p_export.set_defaults(func=_run_export)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (ConfigError, BundleError, AdjustmentError, ValueError) as exc:
        print(json.dumps({"schemaVersion": SCHEMA_VERSION, "error": str(exc)}, indent=2))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
