import argparse
import sys
import subprocess
import json
import logging
import yaml
from pathlib import Path
from training.common.config import load_config
from training.common.logger import setup_logger

logger = setup_logger("pipeline", "logs")

def run_jepa(config_path: str, output_dir: str):
    logger.info(f"Starting JEPA training pipeline with {config_path}")

    # 1. Train
    cmd = [sys.executable, "training/jepa/train.py", "--config", config_path, "--output-dir", output_dir]
    subprocess.check_call(cmd)

    # 2. Export (JEPA encoder to ONNX for embedding generation)
    # Using a separate export script or call would be better, but doing inline for now
    try:
        from training.jepa.model import JEPAModel
        from training.common.export import export_to_onnx
        import torch

        config = load_config(config_path)
        embed_dim = config.get("embed_dim", 128)

        # Load best model
        model = JEPAModel(in_channels=2, embed_dim=embed_dim)
        model.load_state_dict(torch.load(f"{output_dir}/jepa_model.pth", map_location="cpu"))

        # Export Context Encoder only (typically what's used for downstream tasks)
        export_to_onnx(
            model.context_encoder,
            input_shape=(1, 2, 100, 100),
            output_path=f"{output_dir}/jepa_encoder.onnx"
        )
    except Exception as e:
        logger.error(f"Failed to export JEPA model: {e}")

def run_segmentation(config_path: str, output_dir: str):
    logger.info(f"Starting Segmentation training pipeline with {config_path}")

    # 1. Train & Export (Export handled in train.py for segmentation scaffold)
    cmd = [sys.executable, "training/segmentation/train.py", "--config", config_path, "--output-dir", output_dir]
    subprocess.check_call(cmd)

def evaluate_gate(metrics_path: str, thresholds_path: str):
    logger.info(f"Evaluating gates for {metrics_path}")

    cmd = [
        sys.executable,
        "survey-automation-roadmap/scripts/eval_gate.py",
        "--metrics", metrics_path,
        "--thresholds", thresholds_path,
        "--output", str(Path(metrics_path).parent / "gate_report.json")
    ]

    try:
        subprocess.check_call(cmd)
        logger.info("Gate evaluation passed!")
    except subprocess.CalledProcessError:
        logger.error("Gate evaluation FAILED!")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="AI Training Pipeline Orchestrator")
    parser.add_argument("--pipeline", choices=["jepa", "segmentation"], required=True, help="Pipeline to run")
    parser.add_argument("--config", required=True, help="Path to training config yaml")
    parser.add_argument("--output-dir", required=True, help="Directory for artifacts")
    parser.add_argument("--gate-thresholds", default="survey-automation-roadmap/config/eval_gate.example.yaml", help="Path to gate thresholds yaml")

    args = parser.parse_args()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    if args.pipeline == "jepa":
        run_jepa(args.config, args.output_dir)
    elif args.pipeline == "segmentation":
        run_segmentation(args.config, args.output_dir)

    # Evaluate gates
    metrics_path = f"{args.output_dir}/metrics.json"
    if Path(metrics_path).exists():
        evaluate_gate(metrics_path, args.gate_thresholds)
    else:
        logger.warning(f"No metrics file found at {metrics_path}, skipping gate evaluation.")

if __name__ == "__main__":
    main()
