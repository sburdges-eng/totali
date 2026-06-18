#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
import os
from pathlib import Path
import sys
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from survey_automation.json_contract import (  # noqa: E402
    build_contract_payload,
    build_invariant,
    to_stable_relative_path,
    write_contract_json,
)

DEFAULT_THRESHOLDS: dict[str, Any] = {
    "quality": {
        "min_heldout_score": 0.85,
        "min_bucket_score": 0.75,
        "max_failed_buckets": 0,
    },
    "stability": {
        "max_score_regression": 0.02,
    },
    "cost": {
        "max_cost_per_run_usd": 50.0,
    },
    "latency": {
        "max_p95_ms": 500.0,
    },
    "curation": {
        "min_hard_negative_share": 0.1,
    },
}


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _load_thresholds(path: Path | None) -> dict[str, Any]:
    if path is None:
        return DEFAULT_THRESHOLDS
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected YAML mapping at {path}")
    merged = json.loads(json.dumps(DEFAULT_THRESHOLDS))
    for section, value in payload.items():
        if isinstance(value, dict) and isinstance(merged.get(section), dict):
            merged[section].update(value)
        else:
            merged[section] = value
    return merged


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def evaluate(
    *,
    metrics: dict[str, Any],
    thresholds: dict[str, Any],
    baseline: dict[str, Any] | None,
) -> dict[str, Any]:
    quality = metrics.get("quality", {})
    stability = metrics.get("stability", {})
    cost = metrics.get("cost", {})
    latency = metrics.get("latency", {})
    curation = metrics.get("curation", {})

    heldout_score = _float(quality.get("heldout_score"))
    bucket_scores = quality.get("failure_buckets", {})
    if not isinstance(bucket_scores, dict):
        bucket_scores = {}

    min_heldout_score = _float(thresholds["quality"].get("min_heldout_score"), 0.85)
    min_bucket_score = _float(thresholds["quality"].get("min_bucket_score"), 0.75)
    max_failed_buckets = int(thresholds["quality"].get("max_failed_buckets", 0))

    failed_buckets = sorted(
        name for name, score in bucket_scores.items() if _float(score, 1.0) < min_bucket_score
    )
    quality_ok = heldout_score >= min_heldout_score and len(failed_buckets) <= max_failed_buckets

    baseline_heldout = _float((baseline or {}).get("quality", {}).get("heldout_score"), heldout_score)
    score_regression = baseline_heldout - heldout_score
    max_score_regression = _float(thresholds["stability"].get("max_score_regression"), 0.02)
    stability_ok = score_regression <= (max_score_regression + 1e-9)

    run_cost = _float(cost.get("cost_per_run_usd"))
    max_cost = _float(thresholds["cost"].get("max_cost_per_run_usd"), 50.0)
    cost_ok = run_cost <= max_cost

    p95_latency = _float(latency.get("p95_ms"))
    max_p95 = _float(thresholds["latency"].get("max_p95_ms"), 500.0)
    latency_ok = p95_latency <= max_p95

    hard_negative_share = _float(curation.get("hard_negative_share"))
    min_hard_negative_share = _float(thresholds["curation"].get("min_hard_negative_share"), 0.1)
    curation_ok = hard_negative_share >= min_hard_negative_share

    gates = {
        "quality": {
            "ok": quality_ok,
            "heldout_score": heldout_score,
            "min_heldout_score": min_heldout_score,
            "failed_buckets": failed_buckets,
            "max_failed_buckets": max_failed_buckets,
            "min_bucket_score": min_bucket_score,
        },
        "stability": {
            "ok": stability_ok,
            "score_regression": score_regression,
            "max_score_regression": max_score_regression,
            "baseline_heldout_score": baseline_heldout,
        },
        "cost": {
            "ok": cost_ok,
            "cost_per_run_usd": run_cost,
            "max_cost_per_run_usd": max_cost,
        },
        "latency": {
            "ok": latency_ok,
            "p95_ms": p95_latency,
            "max_p95_ms": max_p95,
        },
        "curation": {
            "ok": curation_ok,
            "hard_negative_share": hard_negative_share,
            "min_hard_negative_share": min_hard_negative_share,
        },
    }
    overall_ok = all(section["ok"] for section in gates.values())

    return {
        "ok": overall_ok,
        "gates": gates,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate training-run metrics against continuous gates")
    parser.add_argument("--metrics", required=True, help="JSON file with current run metrics")
    parser.add_argument("--thresholds", required=False, help="Optional YAML thresholds override")
    parser.add_argument("--baseline", required=False, help="Optional JSON baseline metrics")
    parser.add_argument("--output", required=False, help="Optional JSON report output path")
    return parser


def _relative_paths_without_traversal(paths: list[Path]) -> dict[str, str]:
    if not paths:
        return {}
    common_root = Path(os.path.commonpath([str(path.resolve()) for path in paths]))
    result: dict[str, str] = {}
    for path in paths:
        result[str(path)] = to_stable_relative_path(path, base=common_root)
    return result


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    metrics_path = Path(args.metrics).resolve()
    thresholds_path = Path(args.thresholds).resolve() if args.thresholds else None
    baseline_path = Path(args.baseline).resolve() if args.baseline else None

    metrics = _load_json(metrics_path)
    thresholds = _load_thresholds(thresholds_path)
    baseline = _load_json(baseline_path) if baseline_path else None
    evaluation = evaluate(metrics=metrics, thresholds=thresholds, baseline=baseline)
    output_path: Path | None = Path(args.output).resolve() if args.output else None
    path_inputs = [metrics_path]
    if thresholds_path is not None:
        path_inputs.append(thresholds_path)
    if baseline_path is not None:
        path_inputs.append(baseline_path)
    if output_path is not None:
        path_inputs.append(output_path)
    rendered_paths = _relative_paths_without_traversal(path_inputs)
    paths: dict[str, str] = {
        "metrics": rendered_paths[str(metrics_path)],
    }
    if thresholds_path is not None:
        paths["thresholds"] = rendered_paths[str(thresholds_path)]
    if baseline_path is not None:
        paths["baseline"] = rendered_paths[str(baseline_path)]
    if output_path is not None:
        paths["report_output"] = rendered_paths[str(output_path)]

    report = build_contract_payload(
        artifact_type="eval_gate_report",
        invariants=[
            build_invariant("threshold_merge_is_deterministic"),
            build_invariant("gate_evaluation_is_deterministic"),
        ],
        metadata={
            "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "ok": evaluation["ok"],
        },
        paths=paths,
        data={
            "evaluation": evaluation,
        },
    )

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_contract_json(output_path, report)

    print(json.dumps(report, indent=2))
    return 0 if evaluation["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
