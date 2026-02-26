import numpy as np

from laser_suite.io_csv import CanonicalBundle
from laser_suite.rpp import compute_rpp_rows
from laser_suite.schemas import AdjacencyPair, GeometryRecord, Observation, SetbackRecord, Station, WeightRule


def test_pair_covariance_rpp_formula() -> None:
    bundle = CanonicalBundle(
        stations=[
            Station("A", 0.0, 0.0, 0.0, "fixed"),
            Station("B", 10.0, 0.0, 0.0, "free"),
        ],
        observations=[Observation("O1", "A", "B", "distance", 10.0, None)],
        weights=[WeightRule("distance", 0.01, 0.0)],
        adjacency=[AdjacencyPair("A", "B")],
        boundaries=[GeometryRecord("B1", "POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        improvements=[GeometryRecord("I1", "POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        easements=[GeometryRecord("E1", "POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        setbacks=[SetbackRecord("S1", "B1", 1.0)],
    )

    cov = np.zeros((4, 4), dtype=float)
    cov[2, 2] = 0.0004
    cov[3, 3] = 0.0001

    rows = compute_rpp_rows(
        bundle=bundle,
        adjusted_xy={"A": (0.0, 0.0), "B": (10.0, 0.0)},
        covariance_xy_full=cov,
        k95=2.448,
        allowable_base_m=0.02,
        allowable_ppm=50.0,
    )
    assert len(rows) == 1
    assert rows[0].rpp_actual_m > 0
