#!/usr/bin/env python3
"""
Classifier spike — B6 / U1
===========================
Loads a USGS LAS tile, runs the rule-based baseline (and ML path, which falls
back when the ONNX model / onnxruntime are absent), and computes accuracy vs
the ASPRS classification codes embedded in the LAS as reference.

IMPORTANT: _classify_rules() copies las.classification directly into predicted
labels for any non-zero existing classification (lines 231-235 of classifier.py).
This would artificially inflate accuracy — the spike strips the classification
attribute from the LAS wrapper before passing it to the classifier, so the
evaluation measures the pure rule-based logic (elevation-percentile binning).

Usage:
    python tools/classifier_spike.py [LAS_PATH]

Default LAS_PATH: /Users/seanburdges/Dev/data/bv_base/lidar/USGS_CO_SanLuis_8764.las

Writes JSON results to: .tmp/classifier_spike/results.json
No sklearn dependency — metrics implemented in numpy.
"""

import sys
import os
import json
import time
from pathlib import Path

# ── project root on sys.path ──────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import laspy

from totali.segmentation.classifier import PointCloudClassifier
from totali.pipeline.context import PipelineContext
from totali.audit.logger import AuditLogger

# ── ASPRS class names (superset; covers this tile) ────────────────────────────
ASPRS_NAMES = {
    0: "never_classified",
    1: "unclassified",
    2: "ground",
    3: "low_vegetation",
    4: "medium_vegetation",
    5: "high_vegetation",
    6: "building",
    7: "noise",
    8: "model_key_point",
    9: "water",
    10: "rail",
    11: "road_surface",
    12: "overlap",
    13: "wire_guard",
    14: "wire_conductor",
    15: "transmission_tower",
    17: "bridge_deck",
    18: "high_noise",
}

# ── classifier config — from pipeline_in_person.yaml segmentation block ───────
SEGMENTATION_CONFIG = {
    "model_path": "models/point_transformer_v2.onnx",
    "device": "cpu",
    "classes": {
        0: "unclassified",
        2: "ground",
        3: "low_vegetation",
        4: "medium_vegetation",
        5: "high_vegetation",
        6: "building",
        7: "noise",
        9: "water",
        10: "rail",
        11: "road",
        13: "wire_guard",
        14: "wire_conductor",
        15: "tower",
        17: "bridge",
        64: "curb",
        65: "hardscape",
    },
    "confidence_threshold": 0.75,
    "occlusion_threshold": 0.30,
    "batch_size": 65536,
    "voxel_size": 0.05,
}


class _LasNoClassification:
    """
    Thin wrapper that proxies all LAS attributes EXCEPT .classification.

    Purpose: prevent _classify_rules() from copying existing ASPRS codes
    directly into predicted labels (lines 231-235 of classifier.py), which
    would trivially inflate accuracy and make the evaluation meaningless.
    The rule-based path must predict from elevation alone.
    """

    def __init__(self, las):
        self._las = las

    def __getattr__(self, name):
        if name == "classification":
            raise AttributeError("classification intentionally hidden for spike evaluation")
        return getattr(self._las, name)

    def __hasattr__(self, name):
        if name == "classification":
            return False
        return hasattr(self._las, name)


# ── numpy-only metrics helpers ────────────────────────────────────────────────


def confusion_matrix_np(ref: np.ndarray, pred: np.ndarray, labels: list) -> np.ndarray:
    """Return confusion matrix C where C[i,j] = count(ref==labels[i], pred==labels[j])."""
    n = len(labels)
    label_to_idx = {lbl: i for i, lbl in enumerate(labels)}
    cm = np.zeros((n, n), dtype=np.int64)
    for r, p in zip(ref, pred):
        ri = label_to_idx.get(int(r), -1)
        pi = label_to_idx.get(int(p), -1)
        if ri >= 0 and pi >= 0:
            cm[ri, pi] += 1
    return cm


def per_class_metrics(cm: np.ndarray) -> dict:
    """Compute precision, recall, f1 per class from confusion matrix."""
    n = cm.shape[0]
    precision = np.zeros(n)
    recall = np.zeros(n)
    f1 = np.zeros(n)
    for i in range(n):
        tp = cm[i, i]
        fp = cm[:, i].sum() - tp
        fn = cm[i, :].sum() - tp
        precision[i] = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall[i] = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1[i] = (
            2 * precision[i] * recall[i] / (precision[i] + recall[i])
            if (precision[i] + recall[i]) > 0
            else 0.0
        )
    return {"precision": precision, "recall": recall, "f1": f1}


def overall_accuracy(ref: np.ndarray, pred: np.ndarray) -> float:
    return float(np.mean(ref == pred))


# ── main ──────────────────────────────────────────────────────────────────────


def run_spike(las_path: str) -> dict:
    print(f"\n{'=' * 70}")
    print(f"TOTaLi Classifier Spike — B6 / U1")
    print(f"{'=' * 70}")
    print(f"LAS: {las_path}")

    # ── 1. Environment facts ──────────────────────────────────────────────────
    model_file = ROOT / "models" / "point_transformer_v2.onnx"
    model_present = model_file.exists()

    try:
        import onnxruntime

        ort_present = True
        ort_version = onnxruntime.__version__
    except ImportError:
        ort_present = False
        ort_version = None

    try:
        import sklearn

        sklearn_present = True
    except ImportError:
        sklearn_present = False

    print(f"\nEnvironment:")
    print(f"  ONNX model present : {model_present}  ({model_file})")
    print(f"  onnxruntime present: {ort_present}  (version: {ort_version})")
    print(f"  sklearn present    : {sklearn_present}")
    print(f"  Effective path     : RULE-BASED ONLY (model absent → fallback)")

    # ── 2. Load LAS ───────────────────────────────────────────────────────────
    print(f"\nLoading LAS...")
    t0 = time.time()
    las = laspy.read(las_path)
    load_sec = time.time() - t0

    xyz = np.stack(
        [
            np.array(las.x, dtype=np.float64),
            np.array(las.y, dtype=np.float64),
            np.array(las.z, dtype=np.float64),
        ],
        axis=1,
    )
    n_points = len(xyz)

    # Reference: ASPRS classification field
    reference_labels = np.array(las.classification, dtype=np.int32)

    print(f"  Points loaded      : {n_points:,}")
    print(f"  Load time          : {load_sec:.2f}s")
    ref_classes, ref_counts = np.unique(reference_labels, return_counts=True)
    print(f"  Reference classes  : {sorted(ref_classes.tolist())}")
    print(f"  Distribution (ref) :")
    for cls, cnt in zip(ref_classes, ref_counts):
        name = ASPRS_NAMES.get(int(cls), f"class_{cls}")
        pct = 100.0 * cnt / n_points
        print(f"    {cls:3d}  {name:<25}  {cnt:8,}  ({pct:5.1f}%)")

    # ── 3. Run classifier ─────────────────────────────────────────────────────
    # Use AuditLogger writing to .tmp (NOT to audit_logs/) to avoid touching
    # the append-only chain with spike runs.
    tmp_audit = ROOT / ".tmp" / "classifier_spike" / "audit"
    tmp_audit.mkdir(parents=True, exist_ok=True)
    audit = AuditLogger(log_dir=str(tmp_audit), project_id="spike_b6")

    classifier = PointCloudClassifier(config=SEGMENTATION_CONFIG, audit=audit)

    # Strip classification from the las wrapper before handing to classifier
    las_no_cls = _LasNoClassification(las)

    print(f"\nRunning rule-based classifier (classification field hidden)...")
    t1 = time.time()
    cr = classifier._classify_rules(xyz, las_no_cls)
    classify_sec = time.time() - t1

    predicted_labels = cr.labels
    predicted_confs = cr.confidences

    print(f"  Classify time      : {classify_sec:.3f}s")
    pred_classes, pred_counts = np.unique(predicted_labels, return_counts=True)
    print(f"  Predicted classes  : {sorted(pred_classes.tolist())}")

    # ── 4. Compute accuracy metrics ───────────────────────────────────────────
    print(f"\nComputing accuracy metrics...")

    oa = overall_accuracy(reference_labels, predicted_labels)
    print(f"\n  Overall accuracy   : {oa:.4f}  ({oa * 100:.1f}%)")

    # Union of ref + pred classes for confusion matrix
    all_labels_set = sorted(set(ref_classes.tolist()) | set(pred_classes.tolist()))
    cm = confusion_matrix_np(reference_labels, predicted_labels, all_labels_set)
    metrics = per_class_metrics(cm)

    # Per-class table
    print(f"\n  Per-class metrics (reference classes):")
    print(f"  {'cls':>4}  {'name':<25}  {'support':>9}  {'precision':>9}  {'recall':>9}  {'f1':>9}")
    print(f"  {'-' * 4}  {'-' * 25}  {'-' * 9}  {'-' * 9}  {'-' * 9}  {'-' * 9}")

    per_class_rows = []
    for i, cls in enumerate(all_labels_set):
        name = ASPRS_NAMES.get(cls, SEGMENTATION_CONFIG["classes"].get(cls, f"class_{cls}"))
        # support = count in reference
        ref_count = int(ref_counts[np.where(ref_classes == cls)[0][0]]) if cls in ref_classes else 0
        prec = metrics["precision"][i]
        rec = metrics["recall"][i]
        f1v = metrics["f1"][i]
        in_ref = cls in ref_classes
        row = {
            "class_id": cls,
            "name": name,
            "support": ref_count,
            "precision": round(float(prec), 4),
            "recall": round(float(rec), 4),
            "f1": round(float(f1v), 4),
        }
        per_class_rows.append(row)
        if in_ref:
            print(
                f"  {cls:>4}  {name:<25}  {ref_count:>9,}  {prec:>9.4f}  {rec:>9.4f}  {f1v:>9.4f}"
            )

    # ── 5. Top confusions ─────────────────────────────────────────────────────
    print(f"\n  Top confusions (ref → pred, excluding diagonal):")
    confusions = []
    for i, ri in enumerate(all_labels_set):
        for j, pj in enumerate(all_labels_set):
            if i != j and cm[i, j] > 0:
                rname = ASPRS_NAMES.get(ri, f"class_{ri}")
                pname = ASPRS_NAMES.get(pj, f"class_{pj}")
                confusions.append(
                    {
                        "ref_class": ri,
                        "ref_name": rname,
                        "pred_class": pj,
                        "pred_name": pname,
                        "count": int(cm[i, j]),
                        "pct_of_ref": round(100.0 * cm[i, j] / max(cm[i, :].sum(), 1), 2),
                    }
                )
    confusions.sort(key=lambda x: -x["count"])
    top_confusions = confusions[:15]
    for c in top_confusions:
        print(
            f"    ref={c['ref_class']:2d} ({c['ref_name']:<25}) → pred={c['pred_class']:2d} ({c['pred_name']:<25})  "
            f"{c['count']:8,}  ({c['pct_of_ref']:5.1f}% of ref class)"
        )

    # ── 6. Assemble results dict ──────────────────────────────────────────────
    results = {
        "env": {
            "las_path": las_path,
            "n_points": n_points,
            "onnx_model_present": model_present,
            "onnxruntime_present": ort_present,
            "onnxruntime_version": ort_version,
            "sklearn_present": sklearn_present,
            "effective_path": "rule_based_only",
        },
        "overall_accuracy": round(oa, 6),
        "mean_confidence": round(float(np.mean(predicted_confs)), 4),
        "per_class": per_class_rows,
        "top_confusions": top_confusions,
        "reference_distribution": [
            {"class_id": int(c), "name": ASPRS_NAMES.get(int(c), f"class_{c}"), "count": int(n)}
            for c, n in zip(ref_classes, ref_counts)
        ],
        "predicted_distribution": [
            {"class_id": int(c), "name": ASPRS_NAMES.get(int(c), f"class_{c}"), "count": int(n)}
            for c, n in zip(pred_counts.flatten(), pred_counts.flatten())
        ],
    }

    # ── 7. Save JSON ──────────────────────────────────────────────────────────
    out_path = ROOT / ".tmp" / "classifier_spike" / "results.json"
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\n  Results saved to   : {out_path}")
    print(f"{'=' * 70}\n")

    return results


if __name__ == "__main__":
    las_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "/Users/seanburdges/Dev/data/bv_base/lidar/USGS_CO_SanLuis_8764.las"
    )
    results = run_spike(las_path)
