"""
Test script for verifying GNN Module components.
"""
import unittest
import numpy as np
from totali.gnn.config import GNNConfig
from totali.gnn.graph_types import NodeFeatures, GraphData
from totali.gnn.graph_builder import GraphBuilder
from totali.gnn.model import GraphNeuralNetwork

class TestGNNModule(unittest.TestCase):
    def setUp(self):
        # Disable Delaunay for simple k-NN testing to be predictable
        self.config = GNNConfig(
            knn_k=2,
            radius_search=0.0,
            hidden_dim=8,
            use_tin_edges=False,
            use_dwg_linework=False
        )
        self.graph = GraphData()

        # Create a simple synthetic point cloud (tetrahedron)
        points = [
            (0, 0, 0),
            (1, 0, 0),
            (0, 1, 0),
            (0, 0, 1)
        ]

        for i, (x, y, z) in enumerate(points):
            self.graph.nodes.append(NodeFeatures(
                x=float(x), y=float(y), z=float(z),
                r=100, g=100, b=100,
                classification=2 if z == 0 else 6
            ))

    def test_graph_construction(self):
        """Test if edges are built correctly."""
        builder = GraphBuilder(self.config)
        self.graph = builder.build_edges(self.graph)
        self.graph = builder.finalize_graph(self.graph)

        # With k=2, each node connects to 2 nearest neighbors
        # Total edges = N * k = 4 * 2 = 8
        self.assertEqual(len(self.graph.edges), 8)
        self.assertIsNotNone(self.graph.edge_index)
        self.assertEqual(self.graph.edge_index.shape, (2, 8))

    def test_gnn_model_forward(self):
        """Test forward pass of the GNN model."""
        # Prepare graph
        builder = GraphBuilder(self.config)
        self.graph = builder.build_edges(self.graph)
        self.graph = builder.finalize_graph(self.graph)

        model = GraphNeuralNetwork(self.config)

        # Test input shape
        self.assertEqual(self.graph.x_features.shape, (4, 7)) # XYZ + RGB + I

        # Forward pass
        probs = model.forward(self.graph.x_features, self.graph.edge_index)

        # Check output shape (N, num_classes=20)
        self.assertEqual(probs.shape, (4, 20))

        # Check probability sum to 1
        sums = np.sum(probs, axis=1)
        np.testing.assert_allclose(sums, 1.0, atol=1e-5)

if __name__ == '__main__':
    unittest.main()
