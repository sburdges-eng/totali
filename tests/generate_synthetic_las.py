"""
Generate synthetic LAS 1.4 test data for TOTaLi pipeline.
Simulates a small survey site in Colorado State Plane Central (EPSG:2232).

Features:
- Sloped ground surface with ridge (breakline trigger)
- Building cluster (class 6)
- Vegetation canopy (classes 3,4,5)
- Wire run (class 14)
- Curb line (class 64)
- Hardscape pad (class 65)
- Proper CRS VLR for EPSG:2232
"""

import numpy as np
import laspy
from pyproj import CRS


def generate_synthetic_las(output_path: str, n_ground=2000, seed=42):
    rng = np.random.default_rng(seed)

    # Colorado State Plane Central coords (US Survey Feet)
    # Rough center near Colorado Springs
    x_origin = 3_120_000.0
    y_origin = 1_700_000.0
    z_base = 6100.0

    all_xyz = []
    all_class = []
    all_intensity = []

    # --- Ground surface: 200x200 ft area with slope + ridge ---
    gx = rng.uniform(0, 200, n_ground) + x_origin
    gy = rng.uniform(0, 200, n_ground) + y_origin
    # Gentle slope with a ridge at x=100
    gz = z_base + 0.02 * (gx - x_origin) + 2.0 * np.exp(-((gx - x_origin - 100)**2) / 200)
    gz += rng.normal(0, 0.05, n_ground)  # noise
    all_xyz.append(np.column_stack([gx, gy, gz]))
    all_class.append(np.full(n_ground, 2, dtype=np.uint8))
    all_intensity.append(rng.integers(1000, 3000, n_ground).astype(np.uint16))

    # --- Building: 40x30 ft footprint, class 6 ---
    n_bldg = 300
    bx = rng.uniform(120, 160, n_bldg) + x_origin
    by = rng.uniform(80, 110, n_bldg) + y_origin
    bz = z_base + 4.0 + 15.0 + rng.normal(0, 0.3, n_bldg)  # roof ~15ft above ground
    all_xyz.append(np.column_stack([bx, by, bz]))
    all_class.append(np.full(n_bldg, 6, dtype=np.uint8))
    all_intensity.append(rng.integers(500, 1500, n_bldg).astype(np.uint16))

    # --- Vegetation: scattered across site ---
    # Low veg (class 3)
    n_lveg = 200
    lvx = rng.uniform(0, 200, n_lveg) + x_origin
    lvy = rng.uniform(0, 200, n_lveg) + y_origin
    lvz = z_base + 0.02 * (lvx - x_origin) + rng.uniform(0.5, 3.0, n_lveg)
    all_xyz.append(np.column_stack([lvx, lvy, lvz]))
    all_class.append(np.full(n_lveg, 3, dtype=np.uint8))
    all_intensity.append(rng.integers(800, 2000, n_lveg).astype(np.uint16))

    # High veg / canopy (class 5)
    n_hveg = 300
    hvx = rng.uniform(10, 60, n_hveg) + x_origin
    hvy = rng.uniform(130, 190, n_hveg) + y_origin
    hvz = z_base + rng.uniform(15, 35, n_hveg)
    all_xyz.append(np.column_stack([hvx, hvy, hvz]))
    all_class.append(np.full(n_hveg, 5, dtype=np.uint8))
    all_intensity.append(rng.integers(400, 1200, n_hveg).astype(np.uint16))

    # --- Wire run (class 14): line of points ---
    n_wire = 50
    wx = np.linspace(30, 180, n_wire) + x_origin
    wy = np.full(n_wire, 50.0) + y_origin + rng.normal(0, 0.3, n_wire)
    wz = z_base + 20.0 + 2.0 * np.sin(np.linspace(0, np.pi, n_wire))  # catenary-ish
    all_xyz.append(np.column_stack([wx, wy, wz]))
    all_class.append(np.full(n_wire, 14, dtype=np.uint8))
    all_intensity.append(rng.integers(200, 800, n_wire).astype(np.uint16))

    # --- Curb line (class 64): along road edge ---
    n_curb = 80
    cx = np.linspace(10, 190, n_curb) + x_origin
    cy = np.full(n_curb, 30.0) + y_origin + rng.normal(0, 0.1, n_curb)
    cz = z_base + 0.02 * (cx - x_origin) + 0.5  # curb is ~6 inches above ground
    all_xyz.append(np.column_stack([cx, cy, cz]))
    all_class.append(np.full(n_curb, 64, dtype=np.uint8))
    all_intensity.append(rng.integers(1500, 3500, n_curb).astype(np.uint16))

    # --- Hardscape pad (class 65): concrete area ---
    n_hard = 100
    hpx = rng.uniform(140, 180, n_hard) + x_origin
    hpy = rng.uniform(140, 170, n_hard) + y_origin
    hpz = z_base + 0.02 * (hpx - x_origin) + rng.normal(0, 0.02, n_hard)
    all_xyz.append(np.column_stack([hpx, hpy, hpz]))
    all_class.append(np.full(n_hard, 65, dtype=np.uint8))
    all_intensity.append(rng.integers(2000, 4000, n_hard).astype(np.uint16))

    # Stack everything
    xyz = np.vstack(all_xyz)
    classifications = np.concatenate(all_class)
    intensities = np.concatenate(all_intensity)

    # Create LAS 1.4 with point format 6 (has classification)
    header = laspy.LasHeader(point_format=6, version="1.4")
    header.offsets = xyz.min(axis=0)
    header.scales = [0.001, 0.001, 0.001]

    # Add CRS VLR for EPSG:2232 (Colorado State Plane Central, US Survey Feet)
    crs = CRS.from_epsg(2232)
    wkt = crs.to_wkt()
    wkt_bytes = wkt.encode("utf-8") + b"\x00"

    vlr = laspy.VLR(
        user_id="LASF_Projection",
        record_id=2112,
        description="OGC Coordinate System WKT",
        record_data=wkt_bytes,
    )
    header.vlrs.append(vlr)

    las = laspy.LasData(header)
    las.x = xyz[:, 0]
    las.y = xyz[:, 1]
    las.z = xyz[:, 2]
    las.classification = classifications
    las.intensity = intensities

    las.write(output_path)

    print(f"Generated {len(xyz)} points → {output_path}")
    print(f"  Ground:     {np.sum(classifications == 2)}")
    print(f"  Building:   {np.sum(classifications == 6)}")
    print(f"  Low veg:    {np.sum(classifications == 3)}")
    print(f"  High veg:   {np.sum(classifications == 5)}")
    print(f"  Wire:       {np.sum(classifications == 14)}")
    print(f"  Curb:       {np.sum(classifications == 64)}")
    print(f"  Hardscape:  {np.sum(classifications == 65)}")
    print(f"  CRS VLR:    EPSG:2232")

    return output_path


if __name__ == "__main__":
    generate_synthetic_las("tests/synthetic_site.las")
