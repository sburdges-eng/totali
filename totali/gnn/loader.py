"""
Unified Graph Data Loader for LiDAR, TIN, TIF, COGO, DWG, and LandXML.
Constructs a unified graph representation from diverse survey sources.

This module abstracts the complexity of parsing various geospatial formats
into a common  structure consisting of  and .
"""

import math
import os
import re
from typing import List, Dict, Optional, Tuple, Any
import numpy as np

try:
    import laspy
except ImportError:
    laspy = None

try:
    import ezdxf
except ImportError:
    ezdxf = None

try:
    from PIL import Image
except ImportError:
    Image = None

from totali.gnn.config import GNNConfig
from totali.gnn.graph_types import GraphData, NodeFeatures, EdgeFeatures

class UnifiedLoader:
    """
    Orchestrates the ingestion of multiple survey data formats into a single graph.

    Attributes:
        config (GNNConfig): Configuration parameters for file paths and loading options.
        graph (GraphData): The accumulated graph structure.
    """
    def __init__(self, config: GNNConfig):
        self.config = config
        self.graph = GraphData()

    def load_lidar(self, filepath: str) -> None:
        """
        Parses LAS/LAZ files and adds points as graph nodes.

        Args:
            filepath (str): Path to the .las or .laz file.
        """
        if not laspy:
            print("WARNING: laspy not installed. Skipping LiDAR.")
            return

        if not os.path.exists(filepath):
            print(f"WARNING: LiDAR file not found: {filepath}")
            return

        try:
            las = laspy.read(filepath)

            # Simple downsampling or selection logic could go here
            points = las.points

            # Add nodes
            for i in range(len(points)):
                x, y, z = points.x[i], points.y[i], points.z[i]

                # Colors/Intensity extraction with safe fallbacks
                r = int(points.red[i]) if hasattr(points, 'red') else 0
                g = int(points.green[i]) if hasattr(points, 'green') else 0
                b = int(points.blue[i]) if hasattr(points, 'blue') else 0
                intensity = int(points.intensity[i]) if hasattr(points, 'intensity') else 0
                cls = int(points.classification[i]) if hasattr(points, 'classification') else 0

                node = NodeFeatures(
                    x=x, y=y, z=z,
                    r=r, g=g, b=b,
                    intensity=intensity,
                    classification=cls,
                    source_file=os.path.basename(filepath),
                    source_format='las'
                )
                self.graph.nodes.append(node)

        except Exception as e:
            print(f"Error loading LiDAR {filepath}: {e}")

    def load_tin_xml(self, filepath: str) -> None:
        """
        Parses LandXML or simple TIN XML formats.
        Extracts both vertices (nodes) and faces (edges).

        Args:
            filepath (str): Path to the XML file.
        """
        if not os.path.exists(filepath):
            print(f"WARNING: TIN XML file not found: {filepath}")
            return

        try:
            import xml.etree.ElementTree as ET
            tree = ET.parse(filepath)
            root = tree.getroot()

            # LandXML namespaces can be tricky, simplified search here
            # Find <P id="X"> Y X Z </P> nodes
            ns = {'lx': 'http://www.landxml.org/schema/LandXML-1.2'}

            # Try with and without namespace
            points = root.findall(".//P") or root.findall(".//lx:P", ns)

            current_idx_offset = len(self.graph.nodes)
            id_map = {}  # LandXML point ID -> graph index

            for p in points:
                pid = p.get('id')
                coords = p.text.strip().split()
                if len(coords) >= 3:
                    # LandXML is usually Y X Z (Northing Easting Elevation)
                    # We'll assume standard Y X Z for now
                    y, x, z = map(float, coords[:3])

                    node = NodeFeatures(
                        x=x, y=y, z=z,
                        source_file=os.path.basename(filepath),
                        source_format='landxml_tin'
                    )
                    self.graph.nodes.append(node)
                    if pid:
                        id_map[pid] = len(self.graph.nodes) - 1

            # Faces <F> 1 2 3 </F>
            faces = root.findall(".//F") or root.findall(".//lx:F", ns)
            for f in faces:
                p_ids = f.text.strip().split()
                if len(p_ids) == 3:
                    # Create edges for the triangle
                    idx1 = id_map.get(p_ids[0])
                    idx2 = id_map.get(p_ids[1])
                    idx3 = id_map.get(p_ids[2])

                    if idx1 is not None and idx2 is not None and idx3 is not None:
                        # 1-2
                        self.graph.edges.append(EdgeFeatures(idx1, idx2, distance=0, edge_type='tin'))
                        # 2-3
                        self.graph.edges.append(EdgeFeatures(idx2, idx3, distance=0, edge_type='tin'))
                        # 3-1
                        self.graph.edges.append(EdgeFeatures(idx3, idx1, distance=0, edge_type='tin'))

        except Exception as e:
            print(f"Error loading TIN XML {filepath}: {e}")

    def load_cogo_crd(self, filepath: str) -> None:
        """
        Parses ASCII/CSV/CRD coordinate files (P,N,E,Z,D).

        Args:
            filepath (str): Path to the coordinate text file.
        """
        if not os.path.exists(filepath):
            print(f"WARNING: COGO file not found: {filepath}")
            return

        try:
            with open(filepath, 'r') as f:
                for line in f:
                    # Skip comments or empty lines
                    if line.startswith('#') or not line.strip():
                        continue

                    # Basic CSV parsing (P,N,E,Z,D) or space delimited
                    parts = re.split(r'[,\s]+', line.strip())
                    if len(parts) >= 3:
                        # Heuristic: If 5 cols -> P, N, E, Z, D
                        # If 3 cols -> X, Y, Z

                        try:
                            if len(parts) >= 5:
                                # Standard PNEZD
                                pid, n, e, z, desc = parts[0], float(parts[1]), float(parts[2]), float(parts[3]), " ".join(parts[4:])
                                # COGO usually N=Y, E=X
                                x, y = e, n
                            elif len(parts) == 4:
                                # P, N, E, Z
                                pid, n, e, z = parts[0], float(parts[1]), float(parts[2]), float(parts[3])
                                x, y = e, n
                                desc = ""
                            else:
                                # X, Y, Z
                                x, y, z = float(parts[0]), float(parts[1]), float(parts[2])
                                desc = ""

                            node = NodeFeatures(
                                x=x, y=y, z=z,
                                description=desc,
                                source_file=os.path.basename(filepath),
                                source_format='cogo'
                            )
                            self.graph.nodes.append(node)
                        except ValueError:
                            pass # Header or bad line

        except Exception as e:
            print(f"Error loading COGO/CRD {filepath}: {e}")

    def load_dwg_linework(self, filepath: str) -> None:
        """
        Parses DWG/DXF for linework vertices and edges.

        Args:
            filepath (str): Path to the .dxf or .dwg file.
        """
        if not ezdxf:
            print("WARNING: ezdxf not installed. Skipping DWG/DXF.")
            return

        if not os.path.exists(filepath):
            print(f"WARNING: DWG file not found: {filepath}")
            return

        try:
            doc = ezdxf.readfile(filepath)
            msp = doc.modelspace()

            current_idx_offset = len(self.graph.nodes)

            # LINES
            for e in msp.query('LINE'):
                start = e.dxf.start
                end = e.dxf.end

                # Add nodes
                n1 = NodeFeatures(x=start.x, y=start.y, z=start.z, source_format='dwg_line')
                n2 = NodeFeatures(x=end.x, y=end.y, z=end.z, source_format='dwg_line')

                idx1 = len(self.graph.nodes)
                self.graph.nodes.append(n1)
                idx2 = len(self.graph.nodes) + 1
                self.graph.nodes.append(n2)

                # Add edge
                dist = math.sqrt((start.x-end.x)**2 + (start.y-end.y)**2 + (start.z-end.z)**2)
                self.graph.edges.append(EdgeFeatures(idx1, idx2, distance=dist, edge_type='linework'))

            # POLYLINES (LWPOLYLINE)
            for e in msp.query('LWPOLYLINE'):
                points = e.get_points('xy') # 2D points usually
                # LWPolylines define Z via elevation attribute
                z = e.dxf.elevation

                prev_idx = None
                first_idx = None

                for i, p in enumerate(points):
                    n = NodeFeatures(x=p[0], y=p[1], z=z, source_format='dwg_poly')
                    curr_idx = len(self.graph.nodes)
                    self.graph.nodes.append(n)

                    if i == 0:
                        first_idx = curr_idx

                    if prev_idx is not None:
                         dist = math.sqrt((p[0]-self.graph.nodes[prev_idx].x)**2 + (p[1]-self.graph.nodes[prev_idx].y)**2)
                         self.graph.edges.append(EdgeFeatures(prev_idx, curr_idx, distance=dist, edge_type='linework'))

                    prev_idx = curr_idx

                # Close loop if flag set
                if e.closed and first_idx is not None and prev_idx is not None:
                     dist = math.sqrt((self.graph.nodes[first_idx].x-self.graph.nodes[prev_idx].x)**2 +
                                      (self.graph.nodes[first_idx].y-self.graph.nodes[prev_idx].y)**2)
                     self.graph.edges.append(EdgeFeatures(prev_idx, first_idx, distance=dist, edge_type='linework'))

        except Exception as e:
            print(f"Error loading DWG {filepath}: {e}")

    def load_imagery_tif(self, filepath: str) -> None:
        """
        Enriches existing nodes with RGB values sampled from a GeoTIFF.

        Args:
            filepath (str): Path to the .tif file.

        Note:
            This method does not create new nodes. It assumes existing nodes
            (loaded from other sources) are within the bounds of the image.
        """
        if not Image:
            return

        if not os.path.exists(filepath):
            print(f"WARNING: TIF file not found: {filepath}")
            return

        try:
            # Check for world file (.tfw)
            tfw_path = os.path.splitext(filepath)[0] + '.tfw'
            pixel_size_x = 1.0
            pixel_size_y = -1.0
            upper_left_x = 0.0
            upper_left_y = 0.0

            has_tfw = False
            if os.path.exists(tfw_path):
                with open(tfw_path, 'r') as f:
                    lines = [float(l.strip()) for l in f.readlines()]
                    if len(lines) >= 6:
                        pixel_size_x = lines[0]
                        # lines[1] is rotation (assume 0)
                        # lines[2] is rotation (assume 0)
                        pixel_size_y = lines[3]
                        upper_left_x = lines[4]
                        upper_left_y = lines[5]
                        has_tfw = True

            img = Image.open(filepath)
            pixels = img.load()
            width, height = img.size

            # Iterate nodes and sample color
            for node in self.graph.nodes:
                if has_tfw:
                    # Map world X,Y to pixel col,row
                    col = int((node.x - upper_left_x) / pixel_size_x)
                    row = int((node.y - upper_left_y) / pixel_size_y)
                else:
                    # Fallback: assume 1 unit = 1 pixel (unlikely for survey, but keeps logic valid)
                    col = int(node.x)
                    row = int(node.y)

                if 0 <= col < width and 0 <= row < height:
                    try:
                        color = pixels[col, row]
                        # Handle different bands
                        if isinstance(color, int):
                            node.intensity = color # Grayscale
                        elif len(color) >= 3:
                            node.r, node.g, node.b = color[:3]
                    except Exception:
                        pass

        except Exception as e:
            print(f"Error loading TIF {filepath}: {e}")

    def load_all(self) -> GraphData:
        """
        Orchestrates loading from all configured sources in the GNNConfig.

        Returns:
            GraphData: The fully populated graph structure.
        """
        for f in self.config.input_lidar:
            self.load_lidar(f)
        for f in self.config.input_tin_xml:
            self.load_tin_xml(f)
        for f in self.config.input_cogo_crd:
            self.load_cogo_crd(f)
        for f in self.config.input_dwg:
            self.load_dwg_linework(f)

        # Enrich with imagery after nodes are created
        for f in self.config.input_imagery_tif:
            self.load_imagery_tif(f)

        return self.graph
