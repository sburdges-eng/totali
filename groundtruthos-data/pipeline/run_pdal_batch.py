#!/usr/bin/env python3
"""Deterministic PDAL batch runner for COPC + pgPointCloud pipeline.

Renders a shared PDAL template for each LAS/LAZ file, executes with retries, logs
each attempt, and writes a deterministic manifest of batch outcomes.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", required=True)
    parser.add_argument("--runner-pdal", default="pdal")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--input-pattern", default="*.laz")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--manifest", default="")
    parser.add_argument("--input-srid", required=True)
    parser.add_argument("--outlier-mean-k", type=int, required=True)
    parser.add_argument("--outlier-multiplier", type=float, required=True)
    parser.add_argument("--decimation-step", type=int, required=True)
    parser.add_argument("--chipper-capacity", type=int, required=True)
    parser.add_argument("--pg-connection", required=True)
    parser.add_argument("--pg-table", required=True)
    parser.add_argument("--pg-srid", type=int, required=True)
    parser.add_argument("--copc-threads", type=int, default=8)
    parser.add_argument("--retries", type=int, default=2)
    return parser.parse_args()


def sha256_of_file(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def render_template(template: Path, context: dict[str, Any]) -> dict[str, Any]:
    template_text = template.read_text()
    raw = json.loads(template_text)

    def _replace(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _replace(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_replace(v) for v in value]
        if isinstance(value, str) and value.startswith("{{") and value.endswith("}}"):
            key = value[2:-2].strip()
            if key not in context:
                raise KeyError(f"Missing template variable: {key}")
            return context[key]
        return value

    return _replace(raw)


def run_with_retries(command: list[str], log_path: Path, retries: int) -> tuple[int, int]:
    attempts = 0
    last_code = 0
    while True:
        attempts += 1
        with log_path.open("a", encoding="utf-8") as log:
            log.write(f"\n[{datetime.now(timezone.utc).isoformat()}Z] attempt={attempts}\n")
            log.write(" ".join(command) + "\n")
            log.flush()
            result = subprocess.run(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            log.write(f"\nreturn_code={result.returncode}\n")
        last_code = result.returncode
        if last_code == 0:
            return last_code, attempts
        if attempts > retries:
            return last_code, attempts


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    manifest_path = Path(args.manifest or output_dir / "manifest.json")

    input_files = sorted(
        map(Path, glob.glob(str(input_dir / args.input_pattern)))
    )
    if not input_files:
        print(f"No input files found in {input_dir} matching {args.input_pattern}", file=sys.stderr)
        return 1

    template = Path(args.template)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "copc").mkdir(parents=True, exist_ok=True)
    (output_dir / "pipeline-json").mkdir(parents=True, exist_ok=True)
    (output_dir / "logs").mkdir(parents=True, exist_ok=True)

    start = datetime.now(timezone.utc)
    jobs = []

    for input_file in input_files:
        stem = input_file.stem
        pipeline_json_path = output_dir / "pipeline-json" / f"{stem}.pipeline.json"
        log_path = output_dir / "logs" / f"{stem}.log"
        copc_path = output_dir / "copc" / f"{stem}.copc.laz"

        log_path.write_text("")

        context = {
            "input_file": str(input_file),
            "input_srid": args.input_srid,
            "outlier_mean_k": args.outlier_mean_k,
            "outlier_multiplier": args.outlier_multiplier,
            "decimation_step": args.decimation_step,
            "chipper_capacity": args.chipper_capacity,
            "pg_connection": args.pg_connection,
            "pg_table": args.pg_table,
            "pg_srid": args.pg_srid,
            "output_copc_file": str(copc_path),
            "copc_threads": args.copc_threads,
        }

        rendered = render_template(template, context)
        pipeline_json_path.write_text(
            json.dumps(rendered, indent=2) + "\n",
            encoding="utf-8",
        )

        command = [args.runner_pdal, "pipeline", str(pipeline_json_path)]
        return_code, attempts = run_with_retries(command, log_path, args.retries)

        jobs.append(
            {
                "input_file": str(input_file),
                "pipeline_file": str(pipeline_json_path),
                "copc_file": str(copc_path),
                "log_file": str(log_path),
                "status": "success" if return_code == 0 else "failed",
                "attempts": attempts,
                "return_code": return_code,
                "copc_sha256": sha256_of_file(copc_path),
            }
        )

        if return_code != 0:
            print(f"FAILED: {input_file} after {attempts} attempts", file=sys.stderr)

    manifest = {
        "schemaVersion": "1",
        "seed": str(start.isoformat()),
        "text": str(args.input_pattern),
        "intent": {
            "harmonic": {
                "jobs": jobs,
                "summary": {
                    "count": len(jobs),
                    "failed": len([job for job in jobs if job["status"] != "success"]),
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
            "rhythmic": {},
            "dynamic": {},
            "tempo": {},
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    failures = [job for job in jobs if job["status"] != "success"]
    if failures:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
