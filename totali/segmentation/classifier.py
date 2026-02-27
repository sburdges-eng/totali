"""
Phase 2: Discriminative ML Segmentation
========================================
Non-authoritative classification. Produces probabilities + confidence, NOT geometry.
Uses ONNX runtime for model inference on point cloud batches.
Also supports GNN-based classification via the unified GNN module.
"""

import time
from pathlib import Path
from typing import Optional

import numpy as np

from totali.pipeline.models import PhaseResult, ClassificationResult
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.audit.logger import AuditLogger

# Optional GNN imports
try:
    from totali.gnn.config import GNNConfig
    from totali.gnn.loader import UnifiedLoader
    from totali.gnn.graph_builder import GraphBuilder
    from totali.gnn.model import GraphNeuralNetwork
    from totali.gnn.graph_types import NodeFeatures
    GNN_AVAILABLE = True
except ImportError:
    GNN_AVAILABLE = False


class PointCloudClassifier(PipelinePhase):
    """
    Classifies raw LiDAR into semantic classes using a trained model.

    Outputs class labels + confidence scores per point.
    Flags occlusion zones where confidence is below threshold.
    ML produces probabilities, NOT geometry – that's Phase 3.
    """

    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.model_path = config.get("model_path", "models/point_transformer_v2.onnx")
        self.device = config.get("device", "cpu")
        self.confidence_threshold = config.get("confidence_threshold", 0.75)
        self.occlusion_threshold = config.get("occlusion_threshold", 0.30)
        self.batch_size = config.get("batch_size", 65536)
        self.voxel_size = config.get("voxel_size", 0.05)
        self.classes = config.get("classes", {})
        self.use_gnn = config.get("use_gnn", False)
        self.session = None

    def validate_inputs(self, context: PipelineContext) -> tuple[bool, list[str]]:
        errors: list[str] = []
        if context.points_xyz is None:
            errors.append("points_xyz missing; run geodetic phase first")
        if context.las is None:
            errors.append("las missing; run geodetic phase first")
        return len(errors) == 0, errors

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
            providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]                 if self.device == "cuda" else ["CPUExecutionProvider"]
            self.session = ort.InferenceSession(str(model_file), providers=providers)
            return True
        except ImportError:
            self.audit.log("classify", {"warning": "onnxruntime not available, using fallback"})
            return False

    def run(self, context: PipelineContext) -> PhaseResult:
        points_xyz = context.points_xyz
        las = context.las

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
            "use_gnn": self.use_gnn
        })

        # Branch for GNN or Standard ML
        if self.use_gnn and GNN_AVAILABLE:
            result = self._classify_gnn(points_xyz, las)
            method_used = "gnn"
        else:
            # Try ML model, fall back to rule-based
            model_loaded = self._load_model()
            if model_loaded and self.session is not None:
                result = self._classify_ml(points_xyz, las)
                method_used = "ml"
            else:
                result = self._classify_rules(points_xyz, las)
                method_used = "rule_based"

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
            "method": method_used,
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
                "crs": context.crs,
                "stats": context.stats,
                "input_hash": context.input_hash,
            },
        )

    def _classify_gnn(self, xyz: np.ndarray, las) -> ClassificationResult:
        """
        Run classification using the Graph Neural Network module.
        Constructs a graph from the points and propagates labels.
        """
        try:
            # Initialize GNN components
            gnn_config = GNNConfig(
                knn_k=10,
                radius_search=0.0,
                hidden_dim=32
            )

            # Loader - populate with current points
            loader = UnifiedLoader(gnn_config)

            # Add points from context directly to graph
            # This can be slow for millions of points in Python loop
            # For prototype we do a subset or simple loop
            limit = 5000 # Limit for GNN prototype stability

            n_points = len(xyz)
            step = max(1, n_points // limit)
            indices = range(0, n_points, step)

            subset_xyz = xyz[indices]

            for i, idx in enumerate(indices):
                # Extract basic features from las if available
                r = int(las.red[idx]) if hasattr(las, 'red') else 0
                g = int(las.green[idx]) if hasattr(las, 'green') else 0
                b = int(las.blue[idx]) if hasattr(las, 'blue') else 0
                intensity = int(las.intensity[idx]) if hasattr(las, 'intensity') else 0

                # Check for existing classification if we want to refine it
                cls = int(las.classification[idx]) if hasattr(las, 'classification') else 0

                loader.graph.nodes.append(NodeFeatures(
                    x=xyz[idx, 0], y=xyz[idx, 1], z=xyz[idx, 2],
                    r=r, g=g, b=b, intensity=intensity,
                    classification=cls,
                    source_format='las_context'
                ))

            # Build Graph Edges
            builder = GraphBuilder(gnn_config)
            builder.build_edges(loader.graph)
            builder.finalize_graph(loader.graph)

            # Run GNN
            model = GraphNeuralNetwork(gnn_config)
            # In a real scenario, we would load weights here
            # model.load_weights(...)

            probs = model.forward(loader.graph.x_features, loader.graph.edge_index)
            # probs is (subset_size, num_classes)

            subset_labels = np.argmax(probs, axis=1)
            subset_confs = np.max(probs, axis=1)

            # Interpolate back to full cloud (Nearest Neighbor)
            # KDTree for interpolation
            from scipy.spatial import KDTree
            tree = KDTree(subset_xyz)
            dists, nn_indices = tree.query(xyz, k=1)

            full_labels = subset_labels[nn_indices]
            full_confs = subset_confs[nn_indices]

            return ClassificationResult(labels=full_labels.astype(np.int32), confidences=full_confs.astype(np.float32))

        except Exception as e:
            self.audit.log("classify", {"error": f"GNN classification failed: {e}, falling back to rules"})
            return self._classify_rules(xyz, las)

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
