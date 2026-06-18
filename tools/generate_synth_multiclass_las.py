#!/usr/bin/env python3
"""Generate a deterministic synthetic multi-class LAS fixture for classifier spike.

Produces ≥5 ASPRS classes spread across elevation bands so the elevation-percentile
rule baseline has *some* signal:

    Class 2 — ground         (z: lowest 20th pct)
    Class 3 — low_vegetation (z: 20-45th pct, modest z offset above local ground)
    Class 5 — high_vegetation(z: 45-70th pct)
    Class 6 — building       (z: 70-90th pct, flat-topped clusters)
    Class 9 — water          (z: very low, separate basin area)

Elevation bands are realistically non-trivially separated so the rule-based
elevation-percentile classifier can partially discriminate them — but classes
overlap enough to yield an informative (non-trivial) accuracy number.

Deterministic: fixed seed 1701, no Date nondeterminism, no filesystem side-effects
beyond writing the fixture.

Run from repo root::

    .venv/bin/python tools/generate_synth_multiclass_las.py
"""

from __future__ import annotations

from pathlib import Path

import laspy
import numpy as np
from laspy.vlrs.known import WktCoordinateSystemVlr
from pyproj import CRS

OUT = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "survey_corpus"
    / "synth_multiclass.las"
)

# ASPRS class assignments
CLASS_GROUND = 2
CLASS_LOW_VEG = 3
CLASS_HIGH_VEG = 5
CLASS_BUILDING = 6
CLASS_WATER = 9

# Colorado Central EPSG:2232 — same CRS as synth_topo.las
BASE_X = 3_140_000.0
BASE_Y = 1_690_000.0
BASE_Z_GROUND = 5_280.0  # ~1609m in feet — realistic CO elevation


def main() -> None:
    rng = np.random.default_rng(1701)

    segments = []

    # ── 1. Ground: 200 pts, flat-ish area near BASE_Z ─────────────────────────
    n_ground = 200
    gx = rng.uniform(BASE_X, BASE_X + 200.0, n_ground)
    gy = rng.uniform(BASE_Y, BASE_Y + 200.0, n_ground)
    gz = rng.uniform(BASE_Z_GROUND, BASE_Z_GROUND + 1.5, n_ground)
    gc = np.full(n_ground, CLASS_GROUND, dtype=np.uint8)
    segments.append((gx, gy, gz, gc))

    # ── 2. Low vegetation: 150 pts, slight z lift above ground ───────────────
    n_lv = 150
    lx = rng.uniform(BASE_X, BASE_X + 200.0, n_lv)
    ly = rng.uniform(BASE_Y, BASE_Y + 200.0, n_lv)
    lz = rng.uniform(BASE_Z_GROUND + 1.0, BASE_Z_GROUND + 4.5, n_lv)
    lc = np.full(n_lv, CLASS_LOW_VEG, dtype=np.uint8)
    segments.append((lx, ly, lz, lc))

    # ── 3. High vegetation: 150 pts, tall canopy band ────────────────────────
    n_hv = 150
    hx = rng.uniform(BASE_X + 50.0, BASE_X + 150.0, n_hv)
    hy = rng.uniform(BASE_Y + 50.0, BASE_Y + 150.0, n_hv)
    hz = rng.uniform(BASE_Z_GROUND + 8.0, BASE_Z_GROUND + 20.0, n_hv)
    hc = np.full(n_hv, CLASS_HIGH_VEG, dtype=np.uint8)
    segments.append((hx, hy, hz, hc))

    # ── 4. Building: 200 pts, two flat-roofed clusters ────────────────────────
    n_b1 = 100
    b1x = rng.uniform(BASE_X + 20.0, BASE_X + 60.0, n_b1)
    b1y = rng.uniform(BASE_Y + 20.0, BASE_Y + 60.0, n_b1)
    # flat roof: z is nearly constant (small jitter)
    b1z = rng.uniform(BASE_Z_GROUND + 12.0, BASE_Z_GROUND + 13.5, n_b1)
    n_b2 = 100
    b2x = rng.uniform(BASE_X + 130.0, BASE_X + 180.0, n_b2)
    b2y = rng.uniform(BASE_Y + 130.0, BASE_Y + 180.0, n_b2)
    b2z = rng.uniform(BASE_Z_GROUND + 9.0, BASE_Z_GROUND + 10.5, n_b2)
    bx = np.concatenate([b1x, b2x])
    by = np.concatenate([b1y, b2y])
    bz = np.concatenate([b1z, b2z])
    bc = np.full(n_b1 + n_b2, CLASS_BUILDING, dtype=np.uint8)
    segments.append((bx, by, bz, bc))

    # ── 5. Water: 100 pts, lowest elevation (small pond depression) ───────────
    n_w = 100
    wx = rng.uniform(BASE_X + 170.0, BASE_X + 200.0, n_w)
    wy = rng.uniform(BASE_Y + 170.0, BASE_Y + 200.0, n_w)
    # water is LOWER than ground (below-grade depression / reservoir)
    wz = rng.uniform(BASE_Z_GROUND - 2.5, BASE_Z_GROUND - 0.5, n_w)
    wc = np.full(n_w, CLASS_WATER, dtype=np.uint8)
    segments.append((wx, wy, wz, wc))

    # ── Concatenate ───────────────────────────────────────────────────────────
    x = np.concatenate([s[0] for s in segments])
    y = np.concatenate([s[1] for s in segments])
    z = np.concatenate([s[2] for s in segments])
    classification = np.concatenate([s[3] for s in segments])

    # Shuffle so points aren't block-sorted by class (more realistic read order)
    perm = rng.permutation(len(x))
    x, y, z, classification = x[perm], y[perm], z[perm], classification[perm]

    n_total = len(x)

    header = laspy.LasHeader(point_format=6, version="1.4")
    header.offsets = [float(x.min()), float(y.min()), float(z.min())]
    header.scales = [0.001, 0.001, 0.001]

    las = laspy.LasData(header)
    las.x = x
    las.y = y
    las.z = z
    las.classification = classification
    las.vlrs.append(WktCoordinateSystemVlr(CRS.from_epsg(2232).to_wkt()))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    las.write(str(OUT))

    classes, counts = np.unique(classification, return_counts=True)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")
    print(f"  total points : {n_total}")
    print(f"  classes      : {classes.tolist()}")
    for cls, cnt in zip(classes, counts):
        print(f"    class {cls}: {cnt} pts ({100.0 * cnt / n_total:.1f}%)")


if __name__ == "__main__":
    main()
