#!/usr/bin/env python3
"""Regenerate the committed synth_topo.las fixture for sales demos and CI.

Deterministic: fixed RNG seed, Colorado Central (EPSG:2232) coords, WKT VLR.
Run from repo root::

    .venv/bin/python tools/generate_synth_topo_las.py
"""

from __future__ import annotations

from pathlib import Path

import laspy
import numpy as np
from laspy.vlrs.known import WktCoordinateSystemVlr
from pyproj import CRS

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "survey_corpus" / "synth_topo.las"


def main() -> None:
    rng = np.random.default_rng(42)
    n = 500
    x = rng.uniform(3_140_000.0, 3_140_200.0, n)
    y = rng.uniform(1_690_000.0, 1_690_200.0, n)
    z = rng.uniform(5_279.935, 5_308.38, n)

    header = laspy.LasHeader(point_format=6, version="1.4")
    header.offsets = [float(x.min()), float(y.min()), float(z.min())]
    header.scales = [0.001, 0.001, 0.001]

    las = laspy.LasData(header)
    las.x = x
    las.y = y
    las.z = z
    las.classification = rng.choice([2, 5, 6], n).astype(np.uint8)
    las.vlrs.append(WktCoordinateSystemVlr(CRS.from_epsg(2232).to_wkt()))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    las.write(str(OUT))
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
