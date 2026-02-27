"""
GNN Module Data Structures (Types)
"""

from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
import numpy as np

@dataclass
class NodeFeatures:
    """Features associated with a graph node (survey point)"""
    x: float
    y: float
    z: float
    r: int = 0
    g: int = 0
    b: int = 0
    intensity: int = 0
    classification: int = 0  # 0-unclassified, or predicted class
    source_file: str = ""
    source_format: str = ""  # 'las', 'tin', 'cogo', 'dwg', etc.
    description: str = ""    # For COGO points (e.g. 'IP', 'PP')

@dataclass
class EdgeFeatures:
    """Features associated with an edge (connection between nodes)"""
    source_idx: int
    target_idx: int
    distance: float
    edge_type: str = "spatial"  # 'spatial', 'tin', 'linework' (DWG/COGO)
    weight: float = 1.0

@dataclass
class GraphData:
    """Unified Graph Representation for Classification & Visualization"""
    nodes: List[NodeFeatures] = field(default_factory=list)
    edges: List[EdgeFeatures] = field(default_factory=list)

    # NumPy arrays for efficient processing (e.g., GNN input)
    # Shapes: (N, F) for features, (2, E) for edge_index
    x_features: Optional[np.ndarray] = None
    edge_index: Optional[np.ndarray] = None
    y_labels: Optional[np.ndarray] = None

    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_numpy(self):
        """Convert list-based structure to numpy arrays for processing."""
        if not self.nodes:
            return

        # N x 3 (XYZ) + 3 (RGB) + 1 (Intensity)
        coords = [[n.x, n.y, n.z] for n in self.nodes]
        feats = [[n.r, n.g, n.b, n.intensity] for n in self.nodes]

        self.x_features = np.hstack([np.array(coords), np.array(feats)])
        self.y_labels = np.array([n.classification for n in self.nodes])

        if self.edges:
            src = [e.source_idx for e in self.edges]
            dst = [e.target_idx for e in self.edges]
            self.edge_index = np.array([src, dst], dtype=np.int64)
