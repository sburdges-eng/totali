"""
GNN Module Configuration
"""

from typing import Dict, Any, List
from pydantic import BaseModel, Field

class GNNConfig(BaseModel):
    """Configuration for GNN Graph Construction and Classification"""

    # Input Data Sources
    input_lidar: List[str] = Field(default_factory=list, description="List of LAS/LAZ file paths")
    input_tin_xml: List[str] = Field(default_factory=list, description="List of LandXML or TIN file paths")
    input_imagery_tif: List[str] = Field(default_factory=list, description="List of TIF/GeoTIFF image paths")
    input_cogo_crd: List[str] = Field(default_factory=list, description="List of COGO/CRD/ASCII/CSV file paths")
    input_dwg: List[str] = Field(default_factory=list, description="List of DWG/DXF file paths")

    # Graph Construction Parameters
    knn_k: int = Field(default=10, description="Number of nearest neighbors for spatial edges")
    radius_search: float = Field(default=2.0, description="Radius for spatial connectivity (ft/m)")
    use_tin_edges: bool = Field(default=True, description="Use TIN topology for edges if available")
    use_dwg_linework: bool = Field(default=True, description="Use DWG/COGO linework for explicit edges")

    # GNN Model Parameters
    hidden_dim: int = Field(default=64, description="Hidden dimension size for GNN layers")
    num_layers: int = Field(default=3, description="Number of message passing layers")
    learning_rate: float = Field(default=0.01, description="Learning rate for training (if applicable)")

    # Visualization
    plot_point_size: float = Field(default=2.0, description="Point size for visualization")
    show_edges: bool = Field(default=False, description="Whether to draw graph edges in visualization")
