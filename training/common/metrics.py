import json
import time
from pathlib import Path
from typing import Dict, Any, List

class MetricsTracker:
    """
    Tracks training metrics and saves them in a format compatible with
    survey-automation-roadmap/scripts/eval_gate.py
    """
    def __init__(self):
        self.metrics = {
            "quality": {
                "heldout_score": 0.0,
                "failure_buckets": {}
            },
            "stability": {
                "score_regression": 0.0
            },
            "cost": {
                "cost_per_run_usd": 0.0
            },
            "latency": {
                "p95_ms": 0.0
            },
            "curation": {
                "hard_negative_share": 0.0
            },
            "training": {
                "epochs": [],
                "final_loss": 0.0
            }
        }
        self.start_time = time.time()

    def update_quality(self, heldout_score: float, failure_buckets: Dict[str, float] = None):
        self.metrics["quality"]["heldout_score"] = heldout_score
        if failure_buckets:
            self.metrics["quality"]["failure_buckets"] = failure_buckets

    def update_cost(self, cost_usd: float):
        self.metrics["cost"]["cost_per_run_usd"] = cost_usd

    def update_latency(self, p95_ms: float):
        self.metrics["latency"]["p95_ms"] = p95_ms

    def update_curation(self, hard_negative_share: float):
        self.metrics["curation"]["hard_negative_share"] = hard_negative_share

    def log_epoch(self, epoch: int, train_loss: float, val_loss: float, metrics: Dict[str, float] = None):
        epoch_data = {
            "epoch": epoch,
            "train_loss": train_loss,
            "val_loss": val_loss,
            "timestamp": time.time()
        }
        if metrics:
            epoch_data.update(metrics)
        self.metrics["training"]["epochs"].append(epoch_data)
        self.metrics["training"]["final_loss"] = val_loss

    def save(self, output_path: str):
        """Save metrics to JSON."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.metrics, f, indent=2)
