from laser_suite.encroachment import analyze_encroachments
from laser_suite.io_csv import CanonicalBundle
from laser_suite.schemas import AdjacencyPair, GeometryRecord, Observation, SetbackRecord, Station, WeightRule


def test_encroachment_detects_boundary_crossing() -> None:
    bundle = CanonicalBundle(
        stations=[
            Station("A", 0.0, 0.0, 0.0, "fixed"),
            Station("B", 1.0, 0.0, 0.0, "fixed"),
        ],
        observations=[Observation("O1", "A", "B", "distance", 1.0, None)],
        weights=[WeightRule("distance", 0.01, 0.0)],
        adjacency=[AdjacencyPair("A", "B")],
        boundaries=[GeometryRecord("B1", "POLYGON((0 0,10 0,10 10,0 10,0 0))")],
        improvements=[GeometryRecord("I1", "POLYGON((9 1,12 1,12 4,9 4,9 1))")],
        easements=[GeometryRecord("E1", "POLYGON((2 2,3 2,3 3,2 3,2 2))")],
        setbacks=[SetbackRecord("S1", "B1", 1.0)],
    )
    out = analyze_encroachments(bundle, 0.001)
    assert out["row_count"] >= 1
    assert out["compliant"] is False
