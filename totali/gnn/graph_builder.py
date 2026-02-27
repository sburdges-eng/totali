"""
Graph Construction Logic for Survey Data.
Builds spatial and topological edges between unified nodes.
"""

import numpy as np
from scipy.spatial import KDTree, Delaunay
from typing import List, Tuple

from totali.gnn.config import GNNConfig
from totali.gnn.graph_types import GraphData, EdgeFeatures, NodeFeatures

class GraphBuilder:
    def __init__(self, config: GNNConfig):
        self.config = config

    def build_edges(self, graph: GraphData) -> GraphData:
        """
        Constructs edges for the graph based on configuration.
        - k-NN (Spatial Proximity)
        - Radius Search
        - Delaunay Triangulation (if requested)
        """
        if not graph.nodes:
            return graph

        # Extract points as a numpy array for efficient spatial operations
        # Assuming points have (x, y, z) attributes
        points = np.array([[n.x, n.y, n.z] for n in graph.nodes])

        # Build KDTree for efficient nearest neighbor and radius searches
        tree = KDTree(points)

        # 1. k-Nearest Neighbors (k-NN)
        if self.config.knn_k > 0:
            k = min(self.config.knn_k + 1, len(points))
            # The query method returns distances and indices of the nearest neighbors
            dists, indices = tree.query(points, k=k)

            # Iterate over each point and its neighbors
            for i, (neighbors, neighbor_dists) in enumerate(zip(indices, dists)):
                for j_idx, dist in zip(neighbors, neighbor_dists):
                    if i == j_idx:
                        continue # Skip self-loop

                    # Add directed edge from i to j based on k-NN property
                    graph.edges.append(EdgeFeatures(
                        source_idx=i,
                        target_idx=int(j_idx),
                        distance=float(dist),
                        edge_type='spatial_knn'
                    ))

        # 2. Radius Search (connect all points within a fixed radius R)
        if self.config.radius_search > 0:
            # query_pairs returns a set of tuples (i, j) where dist(i, j) < r and i < j
            pairs = tree.query_pairs(r=self.config.radius_search)
            for i, j in pairs:
                dist = np.linalg.norm(points[i] - points[j])
                # Add edges in both directions
                graph.edges.append(EdgeFeatures(
                    source_idx=i,
                    target_idx=j,
                    distance=float(dist),
                    edge_type='spatial_radius'
                ))
                graph.edges.append(EdgeFeatures(
                    source_idx=j,
                    target_idx=i,
                    distance=float(dist),
                    edge_type='spatial_radius'
                ))

        # 3. Delaunay Triangulation (Mesh Topology)
        if self.config.use_tin_edges and len(points) >= 3:
            try:
                # 2D Delaunay on XY plane is standard for survey DTMs
                tri = Delaunay(points[:, :2])

                # Iterate over the simplices (triangles)
                for simplex in tri.simplices:
                    # Create edges for each side of the triangle: (0,1), (1,2), (2,0)
                    for k in range(3):
                        idx1 = simplex[k]
                        idx2 = simplex[(k + 1) % 3]

                        dist = np.linalg.norm(points[idx1] - points[idx2])

                        # Add edges in both directions
                        graph.edges.append(EdgeFeatures(
                            source_idx=int(idx1),
                            target_idx=int(idx2),
                            distance=float(dist),
                            edge_type='delaunay'
                        ))
                        graph.edges.append(EdgeFeatures(
                            source_idx=int(idx2),
                            target_idx=int(idx1),
                            distance=float(dist),
                            edge_type='delaunay'
                        ))
            except Exception as e:
                print(f"Delaunay triangulation failed: {e}")

        return graph

    def finalize_graph(self, graph: GraphData) -> GraphData:
        """Prepares the graph for GNN processing (converts internal lists to numpy arrays)."""
        graph.to_numpy()
        return graph
