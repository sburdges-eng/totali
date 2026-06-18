"""
TOTaLi Drafting Pipeline – Main Entry Point
=============================================
Usage:
    python -m totali.main --input pointcloud.las --config config/pipeline.yaml
    python -m totali.main --input pointcloud.las --phase geodetic
"""

import click
import yaml
import sys
from pathlib import Path
from datetime import datetime

from totali.pipeline.orchestrator import PipelineOrchestrator
from totali.pipeline.context import PipelineConfig
from totali.audit.logger import AuditLogger


@click.command()
@click.option(
    "--input",
    "input_path",
    required=True,
    type=click.Path(exists=True),
    help="Input point cloud file (.las/.laz/.copc.laz)",
)
@click.option(
    "--config",
    "config_path",
    default="config/pipeline.yaml",
    type=click.Path(exists=True),
    help="Pipeline config YAML",
)
@click.option(
    "--phase",
    type=click.Choice(["all", "geodetic", "segment", "extract", "shield", "lint"]),
    default="all",
    help="Run specific phase or all",
)
@click.option("--output", "output_dir", default="output", help="Output directory")
@click.option("--project-id", default=None, help="Project identifier for audit trail")
@click.option(
    "--manual-baseline-sec",
    type=float,
    default=None,
    help="U5: wall-clock seconds for the equivalent manual drafting effort, "
    "captured into the timing metrics for the pilot baseline (SC2). "
    "No time-savings percentage is computed.",
)
@click.option("--dry-run", is_flag=True, help="Validate config and inputs without processing")
def main(input_path, config_path, phase, output_dir, project_id, manual_baseline_sec, dry_run):
    """TOTaLi-Assisted Drafting Pipeline"""

    # Load config
    with open(config_path, "r") as f:
        config = PipelineConfig.model_validate(yaml.safe_load(f))

    # Setup
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    project_id = project_id or f"TOTaLi_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Init audit logger
    audit = AuditLogger(
        log_dir=config.audit.log_dir,
        project_id=project_id,
        hash_algo=config.audit.hash_algorithm,
    )

    audit.log(
        "pipeline_start",
        {
            "input": str(input_path),
            "config": str(config_path),
            "phase": phase,
            "project_id": project_id,
            "dry_run": dry_run,
        },
    )

    if dry_run:
        click.echo(f"[DRY RUN] Config valid. Input: {input_path}")
        click.echo(f"[DRY RUN] Project ID: {project_id}")
        click.echo(f"[DRY RUN] Phase: {phase}")
        audit.log("dry_run_complete", {"status": "ok"})
        return

    # Run pipeline
    pipeline = PipelineOrchestrator(config.model_dump(), audit, output_path)

    try:
        result = pipeline.run(input_path, phase=phase, manual_baseline_sec=manual_baseline_sec)
    except Exception as e:
        audit.log("pipeline_error", {"error": str(e), "phase": phase})
        click.echo(f"\n✗ Pipeline failed: {e}", err=True)
        sys.exit(1)

    # A returned result with success=False means a phase gate halted the run
    # (e.g. the geodetic gatekeeper rejecting an out-of-jurisdiction input).
    # That is a failure, not a success — surface it with a non-zero exit so
    # callers/CI don't read an empty output set as OK.
    if not result.success:
        failed = next((p for p in result.phases if not p.success), None)
        audit.log(
            "pipeline_complete",
            {
                "status": "failed",
                "failed_phase": failed.phase if failed else None,
                "message": failed.message if failed else None,
                "outputs": [str(p) for p in result.output_files],
                "duration_sec": result.duration_sec,
            },
        )
        phase_label = failed.phase if failed else "?"
        reason = failed.message if failed else "unknown error"
        click.echo(f"\n✗ Pipeline failed at phase '{phase_label}': {reason}", err=True)
        click.echo(f"  Audit log: {audit.log_path}", err=True)
        sys.exit(1)

    audit.log(
        "pipeline_complete",
        {
            "status": "success",
            "outputs": [str(p) for p in result.output_files],
            "duration_sec": result.duration_sec,
        },
    )
    click.echo(f"\n✓ Pipeline complete. Outputs in: {output_dir}/")
    click.echo(f"  Audit log: {audit.log_path}")


if __name__ == "__main__":
    main()
