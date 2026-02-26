"""
Phase 2: Discriminative ML Segmentation
========================================
Non-authoritative classification. Produces probabilities + confidence, NOT geometry.
Uses ONNX runtime for model inference on point cloud batches.
"""

import time
from pathlib import Path
from typing import Optional

import numpy as np

from totali.pipeline.models import PhaseResult, ClassificationResult
from totali.audit.logger import AuditLogger


class PointCloudClassifier:
    """
    Classifies raw LiDAR into semantic classes using a trained model.

    Outputs class labels + confidence scores per point.
    Flags occlusion zones where confidence is below threshold.
    ML produces probabilities, NOT geometry – that's Phase 3.
    """

    def __init__(self, config: dict, audit: AuditLogger):
        self.config = config
        self.audit = audit
        self.model_path = config.get("model_path", "models/point_transformer_v2.onnx")
        self.device = config.get("device", "cpu")
        self.confidence_threshold = config.get("confidence_threshold", 0.75)
        self.occlusion_threshold = config.get("occlusion_threshold", 0.30)
        self.batch_size = config.get("batch_size", 65536)
        self.voxel_size = config.get("voxel_size", 0.05)
        self.classes = config.get("classes", {})
        self.session = None

    def _load_model(self):
        """Load ONNX model. Falls back to rule-based if model not found."""
        model_file = Path(self.model_path)
        if not model_file.exists():
            self.audit.log("classify", {
                "warning": f"Model not found at {self.model_path}, using rule-based fallback"
            })
            return False

        try:
            import onnxruntime as ort
            providers = ["CUDAExecutionProvider", "CPUExecutionProvider"] \
                if self.device == "cuda" else ["CPUExecutionProvider"]
            self.session = ort.InferenceSession(str(model_file), providers=providers)
            return True
        except ImportError:
            self.audit.log("classify", {"warning": "onnxruntime not available, using fallback"})
            return False

    def run(self, context: dict) -> PhaseResult:
        points_xyz = context.get("points_xyz")
        las = context.get("las")

        if points_xyz is None:
            return PhaseResult(
                phase="segment", success=False,
                message="No point data in context (run geodetic phase first)"
            )

        n_points = len(points_xyz)
        self.audit.log("classify", {
            "point_count": n_points,
            "model": self.model_path,
            "device": self.device,
        })

        # Try ML model, fall back to rule-based
        model_loaded = self._load_model()

        if model_loaded and self.session is not None:
            result = self._classify_ml(points_xyz, las)
        else:
            result = self._classify_rules(points_xyz, las)

        # Detect occlusion zones
        result.occlusion_mask = self._detect_occlusions(points_xyz, result)
        result.occluded_count = int(np.sum(result.occlusion_mask)) if result.occlusion_mask is not None else 0

        # Stats
        result.low_confidence_count = int(np.sum(result.confidences < self.confidence_threshold))
        result.mean_confidence = float(np.mean(result.confidences))

        unique, counts = np.unique(result.labels, return_counts=True)
        result.class_counts = {
            self.classes.get(int(k), f"class_{k}"): int(v)
            for k, v in zip(unique, counts)
        }

        self.audit.log("classify", {
            "method": "ml" if model_loaded else "rule_based",
            "mean_confidence": round(result.mean_confidence, 4),
            "low_confidence_points": result.low_confidence_count,
            "occluded_points": result.occluded_count,
            "class_distribution": result.class_counts,
        })

        return PhaseResult(
            phase="segment",
            success=True,
            message=f"Classified {n_points} points ({result.mean_confidence:.1%} avg confidence)",
            data={
                "points_xyz": points_xyz,
                "las": las,
                "classification": result,
                "crs": context.get("crs"),
                "stats": context.get("stats"),
                "input_hash": context.get("input_hash"),
            },
        )

    def _classify_ml(self, xyz: np.ndarray, las) -> ClassificationResult:
        """Run ONNX model inference in batches."""
        n = len(xyz)
        all_labels = np.zeros(n, dtype=np.int32)
        all_confs = np.zeros(n, dtype=np.float32)

        # Prepare features: XYZ + intensity + return number if available
        features = self._build_features(xyz, las)

        input_name = self.session.get_inputs()[0].name

        for start in range(0, n, self.batch_size):
            end = min(start + self.batch_size, n)
            batch = features[start:end].astype(np.float32)

            # Pad if model expects fixed batch size
            if len(batch) < self.batch_size:
                pad = np.zeros((self.batch_size - len(batch), batch.shape[1]), dtype=np.float32)
                batch_padded = np.vstack([batch, pad])
            else:
                batch_padded = batch

            outputs = self.session.run(None, {input_name: batch_padded[np.newaxis]})

            # outputs[0] shape: (1, batch_size, num_classes)
            probs = outputs[0][0][:end - start]
            all_labels[start:end] = np.argmax(probs, axis=1)
            all_confs[start:end] = np.max(probs, axis=1)

        return ClassificationResult(labels=all_labels, confidences=all_confs)

    def _classify_rules(self, xyz: np.ndarray, las) -> ClassificationResult:
        """
        Rule-based fallback classifier using elevation percentiles and return info.
        Not as accurate as ML but deterministic and always available.
        """
        n = len(xyz)
        labels = np.zeros(n, dtype=np.int32)  # default: unclassified
        confidences = np.full(n, 0.5, dtype=np.float32)

        z = xyz[:, 2]
        z_min, z_max = z.min(), z.max()
        z_range = z_max - z_min if z_max > z_min else 1.0
        z_norm = (z - z_min) / z_range

        # Ground: lowest 15% of elevation
        ground_mask = z_norm < 0.15
        labels[ground_mask] = 2
        confidences[ground_mask] = 0.6

        # Low vegetation: 15-30%
        low_veg = (z_norm >= 0.15) & (z_norm < 0.30)
        labels[low_veg] = 3
        confidences[low_veg] = 0.45

        # Medium vegetation: 30-50%
        med_veg = (z_norm >= 0.30) & (z_norm < 0.50)
        labels[med_veg] = 4
        confidences[med_veg] = 0.40

        # High vegetation: 50-80%
        high_veg = (z_norm >= 0.50) & (z_norm < 0.80)
        labels[high_veg] = 5
        confidences[high_veg] = 0.35

        # Building candidates: high + clustered (simplified)
        high_pts = z_norm >= 0.80
        labels[high_pts] = 6
        confidences[high_pts] = 0.35

        # Use existing classification if available
        if hasattr(las, "classification"):
            existing = np.array(las.classification)
            has_class = existing > 0
            labels[has_class] = existing[has_class]
            confidences[has_class] = 0.85  # trust existing classification more

        return ClassificationResult(labels=labels, confidences=confidences)

    def _build_features(self, xyz: np.ndarray, las) -> np.ndarray:
        """Build feature matrix for ML model: XYZ + optional intensity/returns."""
        features = [xyz]

        if hasattr(las, "intensity"):
            intensity = np.array(las.intensity, dtype=np.float32).reshape(-1, 1)
            intensity = intensity / max(intensity.max(), 1.0)
            features.append(intensity)

        if hasattr(las, "return_number"):
            ret = np.array(las.return_number, dtype=np.float32).reshape(-1, 1)
            ret = ret / max(ret.max(), 1.0)
            features.append(ret)

        if hasattr(las, "number_of_returns"):
            nret = np.array(las.number_of_returns, dtype=np.float32).reshape(-1, 1)
            nret = nret / max(nret.max(), 1.0)
            features.append(nret)

        return np.hstack(features)

    def _detect_occlusions(
        self, xyz: np.ndarray, result: ClassificationResult
    ) -> np.ndarray:
        """
        Detect occlusion zones: areas under canopy/structures with low point density
        or low classification confidence.
        """
        occlusion = np.zeros(len(xyz), dtype=bool)

        # Low confidence = potential occlusion
        occlusion |= result.confidences < self.occlusion_threshold

        # Points under high vegetation with low density (simplified)
        high_veg_mask = np.isin(result.labels, [4, 5])  # medium + high vegetation
        if high_veg_mask.any():
            # Points that are below vegetation but not ground
            ground_mask = result.labels == 2
            if ground_mask.any():
                ground_z_mean = xyz[ground_mask, 2].mean()
                veg_z_mean = xyz[high_veg_mask, 2].mean()
                under_canopy = (
                    (xyz[:, 2] > ground_z_mean)
                    & (xyz[:, 2] < veg_z_mean)
                    & ~ground_mask
                    & ~high_veg_mask
                )
                occlusion |= under_canopy

        return occlusion
