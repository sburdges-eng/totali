from pathlib import Path

from laser_suite.cli import main


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _bundle(path: Path) -> None:
    _write(path / "stations.csv", "station_id,x,y,z,status\nA,0,0,0,fixed\nB,49,31,0,free\nC,100,0,0,fixed\n")
    _write(path / "observations.csv", "obs_id,from_stn,to_stn,type,value\nO1,A,B,distance,58.30951895\nO2,C,B,distance,58.30951895\n")
    _write(path / "weights.csv", "obs_type,std_dev,ppm\ndistance,0.01,0\n")
    _write(path / "adjacency.csv", "station_i,station_j\nA,B\n")
    _write(path / "boundaries.csv", "boundary_id,wkt_geometry\nB1,\"POLYGON((0 0,100 0,100 100,0 100,0 0))\"\n")
    _write(path / "improvements.csv", "imp_id,wkt_geometry\nI1,\"POLYGON((10 10,20 10,20 20,10 20,10 10))\"\n")
    _write(path / "easements.csv", "easement_id,wkt_geometry\nE1,\"POLYGON((60 60,80 60,80 80,60 80,60 60))\"\n")
    _write(path / "setbacks.csv", "setback_id,boundary_id,distance_m\nS1,B1,2\n")


def test_cli_run(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    _bundle(bundle)

    config = tmp_path / "config.yaml"
    config.write_text("schemaVersion: '1.0.0'\n", encoding="utf-8")

    out_dir = tmp_path / "artifacts"
    code = main([
        "run",
        "--bundle-dir",
        str(bundle),
        "--config",
        str(config),
        "--out",
        str(out_dir),
        "--run-id",
        "test-run",
    ])
    assert code in {0, 2}
    assert (out_dir / "test-run/manifest/run_manifest.json").exists()
