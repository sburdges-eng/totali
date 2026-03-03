import re

file_path = "totali/extraction/extractor.py"
with open(file_path, "r") as f:
    content = f.read()

search_pattern = r"""            try:
                hull = ConvexHull\(cluster_pts\[:, :2\]\)
                area = hull\.volume  # 2D ConvexHull\.volume = area
                if area >= min_area:
                    hull_pts = cluster_pts\[hull\.vertices, :2\]
                    footprints\.append\(hull_pts\)
            except Exception:
                continue"""

replace_pattern = r"""            try:
                from scipy.spatial.qhull import QhullError
                import logging
            except ImportError:
                QhullError = Exception

            try:
                hull = ConvexHull(cluster_pts[:, :2])
                area = hull.volume  # 2D ConvexHull.volume = area
                if area >= min_area:
                    hull_pts = cluster_pts[hull.vertices, :2]
                    footprints.append(hull_pts)
            except (QhullError, ValueError) as e:
                logging.warning("Skipping building footprint extraction for cluster due to hull computation failure: %s", e)
                continue"""

new_content = re.sub(search_pattern, replace_pattern, content, count=1)

with open(file_path, "w") as f:
    f.write(new_content)
