"""
Visualization Module for Survey Graphs.
Plots point clouds, edges, and linework using matplotlib.
"""

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
from typing import Optional, List

from totali.gnn.config import GNNConfig
from totali.gnn.graph_types import GraphData, EdgeFeatures

class GraphPlotter:
    def __init__(self, config: GNNConfig):
        self.config = config

    def plot_graph_3d(self, graph: GraphData, title: str = "Survey Graph", output_path: Optional[str] = None):
        """
        Plots the 3D point cloud and graph edges.
        """
        if not graph.nodes:
            print("No nodes to plot.")
            return

        fig = plt.figure(figsize=(10, 8))
        ax = fig.add_subplot(111, projection='3d')

        # 1. Plot Nodes
        # Extract coordinates
        xs = [n.x for n in graph.nodes]
        ys = [n.y for n in graph.nodes]
        zs = [n.z for n in graph.nodes]

        # Extract classifications for coloring
        classes = [n.classification for n in graph.nodes]

        # Scatter plot for points
        # Using 'tab20' colormap which is good for categorical data
        sc = ax.scatter(xs, ys, zs, c=classes, cmap='tab20', s=self.config.plot_point_size, marker='o', depthshade=True)

        # 2. Plot Edges
        if self.config.show_edges and graph.edges:
            # For speed, only plot a subset if huge
            # Plotting millions of lines in matplotlib 3D is very slow
            max_edges = 2000

            # Prioritize 'linework' edges (from DWG/COGO) over spatial/TIN edges
            linework_edges = [e for e in graph.edges if e.edge_type == 'linework']
            other_edges = [e for e in graph.edges if e.edge_type != 'linework']

            # Combine, keeping all linework and truncating others
            edges_to_plot = linework_edges + other_edges[:max(0, max_edges - len(linework_edges))]

            # We can use Line3DCollection for faster plotting, but simple loop for prototype
            for edge in edges_to_plot:
                n1 = graph.nodes[edge.source_idx]
                n2 = graph.nodes[edge.target_idx]

                # Style edges based on type
                if edge.edge_type == 'linework':
                    color = 'black'
                    alpha = 0.8
                    width = 1.5
                elif edge.edge_type == 'tin':
                    color = 'green'
                    alpha = 0.3
                    width = 0.5
                else:
                    color = 'gray'
                    alpha = 0.2
                    width = 0.3

                ax.plot([n1.x, n2.x], [n1.y, n2.y], [n1.z, n2.z], color=color, alpha=alpha, linewidth=width)

        ax.set_xlabel('Easting (X)')
        ax.set_ylabel('Northing (Y)')
        ax.set_zlabel('Elevation (Z)')
        ax.set_title(title)

        # Add colorbar
        # Matplotlib 3D scatter returns a PathCollection
        try:
            cbar = plt.colorbar(sc, ax=ax, fraction=0.03, pad=0.04)
            cbar.set_label('Classification ID')
        except Exception:
            pass # Sometimes fails with certain backends

        if output_path:
            plt.savefig(output_path, dpi=150)
            print(f"Saved 3D plot to {output_path}")
        else:
            # Interactive show if no output path
            # Note: in headless environments this might fail or do nothing
            try:
                plt.show()
            except Exception:
                pass

        plt.close(fig)

    def plot_2d_overlay(self, graph: GraphData, title: str = "2D Overview", output_path: Optional[str] = None):
        """
        Plots 2D overlay of points and linework.
        """
        if not graph.nodes:
            return

        fig, ax = plt.subplots(figsize=(10, 10))

        xs = [n.x for n in graph.nodes]
        ys = [n.y for n in graph.nodes]
        classes = [n.classification for n in graph.nodes]

        # Plot points
        sc = ax.scatter(xs, ys, c=classes, cmap='tab20', s=self.config.plot_point_size, alpha=0.6)

        # Plot linework edges (e.g. from DWG) specifically
        # Filter for meaningful edges in 2D
        linework_edges = [e for e in graph.edges if e.edge_type in ['linework', 'tin']]

        if linework_edges:
            # Limit TIN edges if too many
            tin_edges = [e for e in linework_edges if e.edge_type == 'tin']
            dwg_edges = [e for e in linework_edges if e.edge_type == 'linework']

            # Prioritize DWG edges, sample TIN edges if too many
            edges_to_plot = dwg_edges + tin_edges[:2000]

            for edge in edges_to_plot:
                n1 = graph.nodes[edge.source_idx]
                n2 = graph.nodes[edge.target_idx]

                if edge.edge_type == 'linework':
                    color = 'black'
                    alpha = 0.8
                    width = 1.5
                else:
                    color = 'green'
                    alpha = 0.15
                    width = 0.5

                ax.plot([n1.x, n2.x], [n1.y, n2.y], color=color, alpha=alpha, linewidth=width)

        ax.set_aspect('equal')
        ax.set_xlabel('Easting (X)')
        ax.set_ylabel('Northing (Y)')
        ax.set_title(title)

        plt.colorbar(sc, label='Classification ID')

        if output_path:
            plt.savefig(output_path, dpi=150)
            print(f"Saved 2D plot to {output_path}")
        else:
            try:
                plt.show()
            except Exception:
                pass

        plt.close(fig)
