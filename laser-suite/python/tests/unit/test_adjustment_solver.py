from pathlib import Path

from laser_suite.adjustment import run_adjustment
from laser_suite.config import DEFAULT_CONFIG
from laser_suite.io_csv import load_bundle


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_adjustment_converges_on_small_network(tmp_path: Path) -> None:
    _write(
        tmp_path / "stations.csv",
        "station_id,x,y,z,status\n"
        "A,0,0,0,fixed\n"
        "B,49,31,0,free\n"
        "C,100,0,0,fixed\n",
    )
    _write(
        tmp_path / "observations.csv",
        "obs_id,from_stn,to_stn,type,value\n"
        "O1,A,B,distance,58.30951895\n"
        "O2,C,B,distance,58.30951895\n",
    )
    _write(tmp_path / "weights.csv", "obs_type,std_dev,ppm\n" "distance,0.01,0\n")
    _write(tmp_path / "adjacency.csv", "station_i,station_j\n" "A,B\n")
    _write(tmp_path / "boundaries.csv", "boundary_id,wkt_geometry\n" "B1,\"POLYGON((0 0,100 0,100 100,0 100,0 0))\"\n")
    _write(tmp_path / "improvements.csv", "imp_id,wkt_geometry\n" "I1,\"POLYGON((10 10,20 10,20 20,10 20,10 10))\"\n")
    _write(tmp_path / "easements.csv", "easement_id,wkt_geometry\n" "E1,\"POLYGON((60 60,80 60,80 80,60 80,60 60))\"\n")
    _write(tmp_path / "setbacks.csv", "setback_id,boundary_id,distance_m\n" "S1,B1,2\n")

    bundle = load_bundle(tmp_path)
    result = run_adjustment(bundle, DEFAULT_CONFIG)

    bx, by = result.adjusted_xy["B"]
    assert abs(bx - 50.0) < 0.1
    assert abs(by - 30.0) < 0.1
    assert result.converged
