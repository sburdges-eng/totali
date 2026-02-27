"""
Integration tests for GNN module within the pipeline context.
Ensures correct behavior even when dependencies are stubbed.
"""
import pytest
import numpy as np
from unittest.mock import MagicMock, patch

from totali.gnn.config import GNNConfig
from totali.gnn.loader import UnifiedLoader
from totali.gnn.graph_builder import GraphBuilder
from totali.gnn.model import GraphNeuralNetwork
from totali.segmentation.classifier import PointCloudClassifier
from totali.pipeline.context import PipelineContext

def test_gnn_config_defaults():
    config = GNNConfig()
    assert config.knn_k == 10
    assert config.hidden_dim == 64

def test_unified_loader_stubbed_dependencies():
    """Test that loader survives when laspy/ezdxf are fake/stubbed."""
    config = GNNConfig()
    loader = UnifiedLoader(config)

    # Try loading with non-existent files (should catch exceptions gracefully)
    loader.load_lidar("fake.las")
    loader.load_dwg_linework("fake.dwg")

    assert len(loader.graph.nodes) == 0

def test_gnn_classifier_integration(audit_logger, sample_points, sample_las):
    """Test the _classify_gnn method in PointCloudClassifier."""
    # Setup config with GNN enabled
    config = {
        "use_gnn": True,
        "classes": {0: "unclassified", 2: "ground"},
        "model_path": "dummy"
    }

    classifier = PointCloudClassifier(config, audit_logger)

    # Since we don't have real trained weights, we just want to ensure
    # the pipeline runs through without crashing and produces a result structure.

    # Mocking GNN components to avoid heavy computation during unit tests
    with patch("totali.segmentation.classifier.UnifiedLoader") as MockLoader,          patch("totali.segmentation.classifier.GraphBuilder") as MockBuilder,          patch("totali.segmentation.classifier.GraphNeuralNetwork") as MockGNN:

        # Setup mock behavior
        mock_graph = MagicMock()
        # Mock features and edge_index for the forward pass
        # 100 points passed in test
        mock_graph.x_features = np.zeros((100, 7))
        mock_graph.edge_index = np.zeros((2, 100), dtype=int)

        mock_loader_instance = MockLoader.return_value
        mock_loader_instance.graph = mock_graph

        mock_gnn_instance = MockGNN.return_value
        # Return probability matrix (N_subset, N_classes)
        # N=100 because in the test we pass 100 points, so subset logic (step=1) should yield 100
        mock_gnn_instance.forward.return_value = np.zeros((100, 20))

        # Run classification on a subset of points
        # sample_las is full size (500), points is subset (100)
        # The fix in classifier code should handle this mismatch now
        result = classifier._classify_gnn(sample_points[:100], sample_las)

        assert result is not None
        assert len(result.labels) == 100
        assert len(result.confidences) == 100

def test_graph_builder_resilience():
    """Test graph builder with empty graph."""
    config = GNNConfig()
    builder = GraphBuilder(config)

    # Empty graph
    from totali.gnn.graph_types import GraphData
    empty_graph = GraphData()

    # Should not crash
    result = builder.build_edges(empty_graph)
    assert len(result.edges) == 0

    result = builder.finalize_graph(empty_graph)
    assert result.x_features is None

if __name__ == "__main__":
    pytest.main([__file__])
