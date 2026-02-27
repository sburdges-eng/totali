"""
Numpy-based Graph Neural Network Implementation.
Implements a simple Message Passing GNN for classification without heavy DL dependencies.
"""

import numpy as np
from totali.gnn.config import GNNConfig
from totali.gnn.graph_types import GraphData

class GraphNeuralNetwork:
    """
    A lightweight GNN implemented in NumPy.
    Simulates Graph Convolutional Network (GCN) layer logic:
    H' = D^-0.5 * A * D^-0.5 * H * W
    """

    def __init__(self, config: GNNConfig):
        self.config = config

        # Dimensions based on NodeFeatures structure:
        # x, y, z, r, g, b, intensity = 7 features
        self.input_dim = 7
        self.hidden_dim = config.hidden_dim

        # Output classes:
        # 0:unclassified, 2:ground, 3-5:veg, 6:building, 7:noise, 9:water,
        # 10:rail, 11:road, 13:wire, 14:wire, 15:tower, 17:bridge, 64:curb, 65:hardscape
        # Total approx 16 classes, let's say 20 max to be safe
        self.output_dim = 20

        # Initialize weights randomly
        np.random.seed(42)
        self.W1 = np.random.randn(self.input_dim, self.hidden_dim) * 0.1
        self.W2 = np.random.randn(self.hidden_dim, self.output_dim) * 0.1

    def _relu(self, x: np.ndarray) -> np.ndarray:
        return np.maximum(0, x)

    def _softmax(self, x: np.ndarray) -> np.ndarray:
        # Subtract max for numerical stability
        exp_x = np.exp(x - np.max(x, axis=1, keepdims=True))
        return exp_x / np.sum(exp_x, axis=1, keepdims=True)

    def _normalize_adj_dense(self, edge_index: np.ndarray, num_nodes: int) -> np.ndarray:
        """
        Constructs normalized adjacency matrix from edge index.
        Note: This is memory intensive for large graphs.
        For production, sparse matrix operations (scipy.sparse) should be used.
        """
        # Create adjacency matrix
        adj = np.eye(num_nodes) # Self-loops

        src, dst = edge_index
        # Assign 1.0 for unweighted, or edge weights if available
        adj[src, dst] = 1.0

        # Degree matrix
        degrees = np.sum(adj, axis=1)

        # Inverse square root of degrees
        d_inv_sqrt = np.power(degrees, -0.5)
        d_inv_sqrt[np.isinf(d_inv_sqrt)] = 0.

        # Convert to diagonal matrix
        d_mat_inv_sqrt = np.diag(d_inv_sqrt)

        # Symmetric normalization: D^-0.5 * A * D^-0.5
        return d_mat_inv_sqrt @ adj @ d_mat_inv_sqrt

    def forward(self, x: np.ndarray, edge_index: np.ndarray) -> np.ndarray:
        """
        Forward pass of the GNN.
        Returns predicted class probabilities for each node.
        """
        num_nodes = x.shape[0]

        # Feature scaling (simple min-max or standardization)
        # Here we do simple standardization on XYZ
        if num_nodes > 1:
            mean = np.mean(x, axis=0)
            std = np.std(x, axis=0) + 1e-6
            x_norm = (x - mean) / std
        else:
            x_norm = x

        # If graph is too large for dense adjacency, fall back to MLP (PointNet-like)
        # Using 2000 as a safe limit for dense matrix operations in standard memory
        if num_nodes > 2000 or edge_index is None or edge_index.shape[1] == 0:
            # print("Graph too large/empty for dense numpy GCN, falling back to pointwise MLP")
            h = self._relu(np.dot(x_norm, self.W1))
            out = np.dot(h, self.W2) # Linear output before softmax
            return self._softmax(out)

        # 1. Construct Normalized Adjacency
        norm_adj = self._normalize_adj_dense(edge_index, num_nodes)

        # 2. Layer 1: GCN
        # AXW
        h = np.dot(np.dot(norm_adj, x_norm), self.W1)
        h = self._relu(h)

        # 3. Layer 2: GCN (Output)
        out = np.dot(np.dot(norm_adj, h), self.W2)

        return self._softmax(out)

    def predict(self, graph: GraphData) -> np.ndarray:
        """Runs inference and returns predicted class labels."""
        if graph.x_features is None:
            return np.zeros(len(graph.nodes), dtype=int)

        probs = self.forward(graph.x_features, graph.edge_index)
        return np.argmax(probs, axis=1)
